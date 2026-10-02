package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"testbot/core"
)

type request struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Plugin string `json:"plugin,omitempty"`
	Target string `json:"target,omitempty"`
	Path   string `json:"path,omitempty"`
}

type host struct {
	rt            *core.Runtime
	out           *output
	mu            sync.Mutex
	state         string
	previous      map[string]pluginView
	stats         core.Stats
	lastState     string
	plugins       map[string]pluginConfig
	root          string
	configPath    string
	config        config
	configData    []byte
	mountRollback map[string]bool
}

func (h *host) current() snapshot {
	h.mu.Lock()
	state := h.state
	h.mu.Unlock()
	s := snapshot{Version: core.Version, State: state, Plugins: []pluginView{}, Stats: h.rt.Stats()}
	for _, item := range h.rt.Snapshot() {
		v := view(item)
		h.mu.Lock()
		if p, ok := h.plugins[item.ID]; ok {
			v.Provides = append(v.Provides, p.Provides...)
			v.Required = append(v.Required, p.Required...)
			v.Optional = append(v.Optional, p.Optional...)
		}
		h.mu.Unlock()
		s.Plugins = append(s.Plugins, v)
	}
	return s
}

func (h *host) publish() {
	s := h.current()
	h.mu.Lock()
	defer h.mu.Unlock()
	changed := h.stats != s.Stats || h.lastState != s.State
	h.stats, h.lastState = s.Stats, s.State
	for _, item := range s.Plugins {
		old, found := h.previous[item.ID]
		// 只记录状态变化，短任务的资源计数不刷日志
		if !found || old.State != item.State || old.Generation != item.Generation || old.Error != item.Error || !reflect.DeepEqual(old.Missing, item.Missing) {
			message := h.out.text(map[core.State]string{core.Starting: "Starting", core.Running: "Started", core.Stopping: "Stopping", core.Stopped: "Stopped", core.Waiting: "Waiting for services", core.Failed: "Failed", core.Quarantined: "Cleanup incomplete; quarantined"}[item.State])
			if len(item.Missing) > 0 {
				message += ": " + strings.Join(item.Missing, ", ")
			}
			if item.Error != "" {
				message += " - " + item.Error
			}
			level := "info"
			if item.State == core.Running || item.State == core.Stopped {
				level = "ok"
			}
			if item.State == core.Failed || item.State == core.Quarantined {
				level = "error"
			}
			_, configured := h.plugins[item.ID]
			if item.State != core.Registered && !(configured && item.State == core.Starting) {
				h.out.log(level, item.ID, message)
			}
			changed = true
		}
		h.previous[item.ID] = item
		if found && (old.Enabled != item.Enabled || !reflect.DeepEqual(old.Resources, item.Resources)) {
			changed = true
		}
	}
	if len(s.Plugins) != len(h.previous) {
		changed = true
		clear(h.previous)
	}
	if changed {
		h.out.send(map[string]any{"type": "state", "snapshot": s})
	}
}

func (h *host) execute(ctx context.Context, r request) error {
	h.mu.Lock()
	rollback := h.mountRollback[r.Plugin]
	h.mu.Unlock()
	if rollback && (r.Method == "start" || r.Method == "reload") {
		return fmt.Errorf("%s", h.out.text("mount rollback pending for %s; use /retryCleanup", r.Plugin))
	}
	switch r.Method {
	case "mount":
		return h.mount(ctx, r.Path)
	case "unmount":
		return h.unmount(ctx, r.Plugin)
	case "status":
		if r.Plugin != "" {
			if _, ok := h.rt.Status(r.Plugin); !ok {
				return core.ErrPluginNotFound
			}
		}
		return nil
	case "start":
		return h.rt.Start(ctx, r.Plugin)
	case "stop":
		return h.rt.Stop(ctx, r.Plugin)
	case "reload":
		return h.rt.Reload(ctx, r.Plugin)
	case "retryCleanup":
		if rollback {
			if _, found := h.rt.Status(r.Plugin); !found {
				h.forgetMount(r.Plugin)
				return nil
			}
		}
		if err := h.rt.RetryCleanup(ctx, r.Plugin); err != nil {
			return err
		}
		if rollback {
			if _, found := h.rt.Status(r.Plugin); found {
				if err := h.rt.Unload(ctx, r.Plugin); err != nil {
					return err
				}
			}
			h.forgetMount(r.Plugin)
		}
		return nil
	case "shutdown":
		h.mu.Lock()
		h.state = "closing"
		h.mu.Unlock()
		h.out.log("info", "core", h.out.text("closing runtime"))
		err := h.rt.Close(ctx)
		if err == nil {
			h.mu.Lock()
			h.state = "closed"
			h.mu.Unlock()
			h.out.log("ok", "core", h.out.text("closed - all managed resources released"))
		}
		return err
	default:
		return fmt.Errorf("%s", h.out.text("unknown method %q", r.Method))
	}
}

func serve(h *host, input io.Reader, plugins []pluginConfig) int {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	requests := make(chan request)
	readErr := make(chan error, 1)
	go func() {
		scan := bufio.NewScanner(input)
		scan.Buffer(make([]byte, 4096), 64<<10)
		for scan.Scan() {
			var r request
			d := json.NewDecoder(bytes.NewReader(scan.Bytes()))
			d.DisallowUnknownFields()
			if err := d.Decode(&r); err != nil || d.Decode(new(any)) != io.EOF || r.ID == "" || len(r.ID) > 128 {
				h.out.log("error", "me.init", h.out.text("invalid control request"))
				continue
			}
			requests <- r
		}
		readErr <- scan.Err()
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	active := "@init"
	initContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	done := make(chan error, 1)
	var pending request
	go func() {
		for _, p := range startupOrder(plugins) {
			if initContext.Err() != nil {
				break
			}
			if err := h.rt.Start(initContext, p.ID); err != nil {
				h.out.log("error", "me.init", p.ID+": "+err.Error())
			}
			h.publish()
		}
		done <- initContext.Err()
	}()
	finish := func(r request, err error) {
		h.publish()
		response := map[string]any{"type": "response", "id": r.ID, "snapshot": h.current()}
		if err != nil {
			response["error"] = err.Error()
		}
		h.out.send(response)
	}
	shutdown := func() int {
		if cancel != nil {
			cancel()
			<-done
		}
		ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := h.execute(ctx, request{Method: "shutdown"}); err != nil {
			h.out.log("error", "core", h.out.text("shutdown incomplete: %s", err.Error()))
			return 1
		}
		return 0
	}
	for {
		select {
		case <-signals:
			return shutdown()
		case err := <-readErr:
			if err != nil {
				h.out.log("error", "me.init", err.Error())
			}
			return shutdown()
		case <-ticker.C:
			h.publish()
		case r := <-requests:
			if r.Method == "cancel" {
				if active == r.Target && cancel != nil {
					cancel()
				}
				continue
			}
			if r.Method == "status" {
				finish(r, h.execute(context.Background(), r))
				continue
			}
			if active != "" {
				finish(r, fmt.Errorf("%s", h.out.text("another lifecycle operation is still running")))
				continue
			}
			pending, active = r, r.ID
			ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
			cancel = stop
			message := h.out.text(r.Method)
			if r.Plugin != "" {
				message += " " + r.Plugin
			}
			if r.Method == "mount" {
				message = h.out.text("Mounting %s", r.Path)
			}
			if r.Method == "unmount" {
				message = h.out.text("Unmounting %s", r.Plugin)
			}
			if r.Method != "shutdown" {
				h.out.log("info", "core", message)
			}
			go func() { done <- h.execute(ctx, r) }()
		case err := <-done:
			initializing := active == "@init"
			cancel()
			cancel = nil
			active = ""
			if initializing {
				h.mu.Lock()
				h.state = "running"
				h.mu.Unlock()
				if err != nil {
					h.out.log("warn", "me.init", h.out.text("initialization interrupted: %s", err.Error()))
				}
				level, message := "ok", h.out.text("runtime ready")
				for _, item := range h.rt.Snapshot() {
					if item.State == core.Failed || item.State == core.Quarantined {
						level, message = "warn", h.out.text("runtime ready with failed plugins")
						break
					}
				}
				h.out.log(level, "core", message)
				h.out.send(map[string]any{"type": "ready", "snapshot": h.current()})
				continue
			}
			finish(pending, err)
			if pending.Method == "shutdown" && err == nil {
				return 0
			}
		}
	}
}

func run() int {
	root := flag.String("root", "", "project directory")
	path := flag.String("config", "", "plugin configuration")
	flag.Parse()
	logs := &output{writer: os.Stdout}
	locale, localeErr := loadLocale(*root)
	if localeErr != nil {
		logs.log("error", "me.init", localeErr.Error())
		return 1
	}
	logs.locale = locale
	cfg, configData, err := readConfigFile(*path)
	if err != nil {
		logs.log("error", "me.init", err.Error())
		return 1
	}
	rt := core.New(core.Options{StartTimeout: time.Duration(cfg.StartTimeoutMS) * time.Millisecond, DisposeTimeout: time.Duration(cfg.DisposeTimeoutMS) * time.Millisecond})
	h := &host{rt: rt, out: logs, state: "initializing", previous: make(map[string]pluginView), plugins: make(map[string]pluginConfig),
		root: *root, configPath: *path, config: cfg, configData: configData}
	logs.log("info", "core", logs.text("created runtime %s", core.Version))
	for _, p := range cfg.Plugins {
		def, err := p.definition(*root, filepath.Dir(*path), logs)
		if err == nil {
			err = rt.Register(def)
		}
		if err != nil {
			logs.log("error", "me.init", p.ID+": "+err.Error())
			_ = rt.Close(context.Background())
			return 1
		}
		h.plugins[p.ID] = p
	}
	h.publish()
	return serve(h, os.Stdin, cfg.Plugins)
}

func main() {
	os.Exit(run())
}
