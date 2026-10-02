package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"testbot/core/bridge"
	"testbot/core/sdk"
)

type pluginConfig struct {
	ID        string            `json:"id"`
	Command   []string          `json:"command"`
	Dir       string            `json:"dir,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Provides  []string          `json:"provides,omitempty"`
	Required  []string          `json:"required,omitempty"`
	Optional  []string          `json:"optional,omitempty"`
	Events    map[string]string `json:"events,omitempty"`
	Autostart bool              `json:"autostart"`
}

type config struct {
	Plugins          []pluginConfig `json:"plugins"`
	StartTimeoutMS   int            `json:"startTimeoutMs,omitempty"`
	DisposeTimeoutMS int            `json:"disposeTimeoutMs,omitempty"`
}

func startupOrder(plugins []pluginConfig) []pluginConfig {
	pending := make([]pluginConfig, 0, len(plugins))
	providers := make(map[string]string)
	for _, p := range plugins {
		if !p.Autostart {
			continue
		}
		pending = append(pending, p)
		for _, key := range p.Provides {
			providers[key] = p.ID
		}
	}
	ordered := make([]pluginConfig, 0, len(pending))
	for len(pending) > 0 {
		waiting := make(map[string]bool, len(pending))
		for _, p := range pending {
			waiting[p.ID] = true
		}
		blocked := func(p pluginConfig, keys []string) bool {
			for _, key := range keys {
				if id := providers[key]; id != p.ID && waiting[id] {
					return true
				}
			}
			return false
		}
		selected := -1
		// optional只影响启动顺序，循环或缺失不能阻止启动
		for _, optional := range []bool{true, false} {
			for i, p := range pending {
				if !blocked(p, p.Required) && (!optional || !blocked(p, p.Optional)) {
					selected = i
					break
				}
			}
			if selected >= 0 {
				break
			}
		}
		if selected < 0 {
			selected = 0
		}
		ordered = append(ordered, pending[selected])
		pending = append(pending[:selected], pending[selected+1:]...)
	}
	return ordered
}

func readConfig(path string) (config, error) {
	result, _, err := readConfigFile(path)
	return result, err
}

func readConfigFile(path string) (config, []byte, error) {
	var result config
	f, err := os.Open(path)
	if err != nil {
		return result, nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return result, nil, err
	}
	if len(data) > 1<<20 {
		return result, nil, fmt.Errorf("config exceeds 1 MiB")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&result); err != nil {
		return result, nil, err
	}
	if d.Decode(new(any)) != io.EOF {
		return result, nil, fmt.Errorf("config must contain one JSON object")
	}
	if result.StartTimeoutMS < 0 || result.DisposeTimeoutMS < 0 || result.StartTimeoutMS > 60000 || result.DisposeTimeoutMS > 60000 {
		return result, nil, fmt.Errorf("timeouts must be between 0 and 60000 ms")
	}
	if len(result.Plugins) > 256 {
		return result, nil, fmt.Errorf("at most 256 plugins")
	}
	return result, data, nil
}

func (p pluginConfig) definition(root, configDir string, logs *output) (sdk.Definition, error) {
	resolve := strings.NewReplacer("${ROOT}", root, "${NODE}", os.Getenv("ME_NODE"), "${PYTHON}", os.Getenv("ME_PYTHON"))
	command := make([]string, len(p.Command))
	for i, value := range p.Command {
		command[i] = resolve.Replace(value)
	}
	dir := p.Dir
	if dir == "" {
		dir = root
	} else {
		dir = resolve.Replace(dir)
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(configDir, dir)
		}
	}
	env := []string{"PYTHONDONTWRITEBYTECODE=1", "PYTHONUNBUFFERED=1"}
	for name, value := range p.Env {
		if name == "" || strings.ContainsAny(name, "=\x00") {
			return sdk.Definition{}, fmt.Errorf("invalid environment key")
		}
		env = append(env, name+"="+resolve.Replace(value))
	}
	keys := func(names []string) []sdk.ServiceKey {
		result := make([]sdk.ServiceKey, len(names))
		for i, name := range names {
			result[i] = bridge.NewService(name).Key()
		}
		return result
	}
	var events []sdk.Event[json.RawMessage]
	for name, mode := range p.Events {
		var dispatch sdk.DispatchMode
		switch mode {
		case "emit":
			dispatch = sdk.EmitMode
		case "parallel":
			dispatch = sdk.ParallelMode
		default:
			return sdk.Definition{}, fmt.Errorf("event %s: expected emit or parallel", name)
		}
		events = append(events, sdk.NewEvent[json.RawMessage](name, dispatch))
	}
	bridgeConfig := bridge.Config{
		Manifest: sdk.Manifest{ID: p.ID, Provides: keys(p.Provides), Required: keys(p.Required), Optional: keys(p.Optional)},
		Command:  command, Dir: dir, Env: env, Events: events,
		StopTimeout: 2 * time.Second,
	}
	def, err := bridge.Definition(bridgeConfig)
	if err != nil {
		return def, err
	}
	def.Factory = func() (sdk.Plugin, error) {
		writer := &logWriter{out: logs, source: p.ID}
		current := bridgeConfig
		current.Stderr = writer
		foreign, err := bridge.Definition(current)
		if err != nil {
			return nil, err
		}
		plugin, err := foreign.Factory()
		if err != nil {
			return nil, err
		}
		return sdk.StartFunc(func(ctx sdk.Context) error {
			logs.log("info", p.ID, logs.text("Starting"))
			if err := ctx.Defer("plugin log tail", func(context.Context) error { writer.flush(); return nil }); err != nil {
				return err
			}
			return plugin.Start(ctx)
		}), nil
	}
	return def, nil
}
