package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"testbot/core"
	"testbot/core/sdk"
)

func TestStartupOrderUsesServicesWithoutEnablingDisabledProviders(t *testing.T) {
	tests := []struct {
		name    string
		plugins []pluginConfig
		want    []string
	}{
		{"providers first", []pluginConfig{
			{ID: "ui", Autostart: true, Required: []string{"chat"}, Optional: []string{"body"}},
			{ID: "chat", Autostart: true, Provides: []string{"chat"}, Required: []string{"model"}},
			{ID: "model", Autostart: true, Provides: []string{"model"}},
			{ID: "body", Autostart: true, Provides: []string{"body"}},
		}, []string{"model", "chat", "body", "ui"}},
		{"disabled and absent", []pluginConfig{
			{ID: "consumer", Autostart: true, Required: []string{"disabled", "absent"}},
			{ID: "disabled", Provides: []string{"disabled"}},
			{ID: "independent", Autostart: true},
		}, []string{"consumer", "independent"}},
		{"optional cycle", []pluginConfig{
			{ID: "a", Autostart: true, Provides: []string{"a"}, Optional: []string{"b"}},
			{ID: "b", Autostart: true, Provides: []string{"b"}, Optional: []string{"a"}},
		}, []string{"a", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := append([]pluginConfig(nil), tt.plugins...)
			var ids []string
			for _, p := range startupOrder(tt.plugins) {
				ids = append(ids, p.ID)
			}
			if !reflect.DeepEqual(ids, tt.want) || !reflect.DeepEqual(before, tt.plugins) {
				t.Fatalf("order=%v want=%v; configuration changed=%v", ids, tt.want, !reflect.DeepEqual(before, tt.plugins))
			}
		})
	}
}

func TestShutdownKeepsDirtyResourcesAvailableForRetry(t *testing.T) {
	rt := core.New()
	h := &host{rt: rt, out: &output{writer: io.Discard}, state: "running", previous: map[string]pluginView{}}
	first := true
	if err := rt.Register(sdk.Definition{Manifest: sdk.Manifest{ID: "fixture"}, Factory: func() (sdk.Plugin, error) {
		return sdk.StartFunc(func(ctx sdk.Context) error {
			return ctx.Defer("resource", func(context.Context) error {
				if first {
					first = false
					return errors.New("retry required")
				}
				return nil
			})
		}), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := h.execute(context.Background(), request{Method: "start", Plugin: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if err := h.execute(context.Background(), request{Method: "shutdown"}); err == nil {
		t.Fatal("dirty shutdown succeeded")
	}
	if h.current().State != "closing" {
		t.Fatal("lost closing state")
	}
	if h.current().Plugins[0].State != core.Quarantined {
		t.Fatal("lost quarantine")
	}
	if err := h.execute(context.Background(), request{Method: "retryCleanup", Plugin: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if err := h.execute(context.Background(), request{Method: "shutdown"}); err != nil {
		t.Fatal(err)
	}
	if h.current().State != "closed" || rt.Stats().Resources != 0 {
		t.Fatal("shutdown not clean")
	}
}

type frameSink struct{ frames chan map[string]any }

func TestProtectedModulesRejectRemovalAndStillReloadAndClose(t *testing.T) {
	h := mountHost(t)
	for _, id := range []string{"me.init", "me.cli"} {
		dir := mountFixture(t, h, id, "")
		if err := h.mount(context.Background(), dir); err != nil {
			t.Fatal(err)
		}
	}
	original, err := os.ReadFile(h.configPath)
	if err != nil {
		t.Fatal(err)
	}
	before := h.rt.Snapshot()
	for _, method := range []string{"stop", "unmount"} {
		for _, id := range []string{"core", "me.init", "me.cli"} {
			if err := h.execute(context.Background(), request{Method: method, Plugin: id}); err == nil || !strings.Contains(err.Error(), "critical module") {
				t.Fatal("critical module removal accepted", method, id, err)
			}
			if !reflect.DeepEqual(before, h.rt.Snapshot()) {
				t.Fatal("rejected removal changed runtime")
			}
		}
	}
	if err := h.unmount(context.Background(), "me.cli"); err == nil {
		t.Fatal("direct unmount bypassed protection")
	}
	if data, err := os.ReadFile(h.configPath); err != nil || !bytes.Equal(data, original) {
		t.Fatal("rejected removal changed config", err)
	}
	for _, id := range []string{"me.init", "me.cli"} {
		previous, _ := h.rt.Status(id)
		if err := h.execute(context.Background(), request{Method: "reload", Plugin: id}); err != nil {
			t.Fatal(err)
		}
		current, _ := h.rt.Status(id)
		if current.State != core.Running || current.Generation <= previous.Generation {
			t.Fatal("protected module could not reload", current)
		}
	}
	if err := h.execute(context.Background(), request{Method: "shutdown"}); err != nil {
		t.Fatal(err)
	}
	if h.rt.Stats().Resources != 0 || h.current().State != "closed" {
		t.Fatal("normal exit blocked", h.current())
	}
}

func (s *frameSink) Write(data []byte) (int, error) {
	var frame map[string]any
	if err := json.Unmarshal(data, &frame); err != nil {
		return 0, err
	}
	s.frames <- frame
	return len(data), nil
}

func TestServeAllowsStatusAndCancellationDuringStart(t *testing.T) {
	rt := core.New()
	entered := make(chan struct{})
	if err := rt.Register(sdk.Definition{Manifest: sdk.Manifest{ID: "slow"}, Factory: func() (sdk.Plugin, error) {
		return sdk.StartFunc(func(ctx sdk.Context) error { close(entered); <-ctx.Context().Done(); return ctx.Context().Err() }), nil
	}}); err != nil {
		t.Fatal(err)
	}
	sink := &frameSink{frames: make(chan map[string]any, 64)}
	h := &host{rt: rt, out: &output{writer: sink}, state: "initializing", previous: map[string]pluginView{}}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	done := make(chan int, 1)
	go func() { done <- serve(h, reader, nil) }()
	await := func(kind, id string) map[string]any {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case frame := <-sink.frames:
				if frame["type"] == kind && (id == "" || frame["id"] == id) {
					return frame
				}
			case <-deadline:
				t.Fatal("missing frame", kind, id)
			}
		}
	}
	send := func(r request) {
		t.Helper()
		if err := json.NewEncoder(writer).Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	await("ready", "")
	send(request{ID: "start", Method: "start", Plugin: "slow"})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("start did not run")
	}
	send(request{ID: "status", Method: "status"})
	frame := await("response", "status")
	if frame["error"] != nil {
		t.Fatal(frame)
	}
	send(request{ID: "cancel", Method: "cancel", Target: "start"})
	if await("response", "start")["error"] == nil {
		t.Fatal("cancelled Start succeeded")
	}
	send(request{ID: "close", Method: "shutdown"})
	if await("response", "close")["error"] != nil {
		t.Fatal("shutdown failed")
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatal(code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("host did not exit")
	}
}

func TestPluginLogLevelsAndUnterminatedTail(t *testing.T) {
	sink := &frameSink{frames: make(chan map[string]any, 8)}
	w := &logWriter{out: &output{writer: sink}, source: "fixture"}
	_, _ = w.Write([]byte("[WARN] split "))
	_, _ = w.Write([]byte("message\n[FA"))
	_, _ = w.Write([]byte("ILED] generation failed\nlast line"))
	w.flush()
	first, failed, last := <-sink.frames, <-sink.frames, <-sink.frames
	if first["level"] != "warn" || first["message"] != "split message" || first["source"] != "fixture" {
		t.Fatal(first)
	}
	if last["level"] != "info" || last["message"] != "last line" {
		t.Fatal(last)
	}
	if failed["level"] != "failed" || failed["message"] != "generation failed" || failed["source"] != "fixture" {
		t.Fatal(failed)
	}
}

func TestPluginLogClassificationPreservesSourceAndAttributes(t *testing.T) {
	for _, tt := range []struct {
		line, level, message string
	}{
		{"\x1b[31m[ERROR]\x1b[0m internal failure", "error", "internal failure"},
		{"  [warning]\twill retry\r\n", "warn", "will retry"},
		{"[OK] ready\n", "ok", "ready"},
		{"[FAILED] request rejected\n", "failed", "request rejected"},
		{"plain stderr\n", "info", "plain stderr"},
		{"[UNKNOWN] untouched\n", "info", "[UNKNOWN] untouched"},
		{`{"level":"ERROR","msg":"broken","source":"core","error":"detail"}` + "\n", "error", `broken - {"error":"detail","source":"core"}`},
		{`{"level":"failed","message":"rejected"}` + "\n", "failed", "rejected"},
		{`{"level":"debug","message":"connected"}` + "\n", "info", "connected"},
		{`{"level":"unknown","message":"original"}` + "\n", "info", `{"level":"unknown","message":"original"}`},
		{`{"level":"error","msg":123}` + "\n", "info", `{"level":"error","msg":123}`},
		{`{"level":"error",broken}` + "\n", "info", `{"level":"error",broken}`},
	} {
		t.Run(tt.line, func(t *testing.T) {
			sink := &frameSink{frames: make(chan map[string]any, 1)}
			writer := &logWriter{out: &output{writer: sink}, source: "fixture"}
			for _, b := range []byte(tt.line) {
				_, _ = writer.Write([]byte{b})
			}
			writer.flush()
			frame := <-sink.frames
			if frame["level"] != tt.level || frame["message"] != tt.message || frame["source"] != "fixture" {
				t.Fatal(frame)
			}
		})
	}
}

func TestPublishDoesNotRepeatUnchangedLifecycleLogs(t *testing.T) {
	var buffer bytes.Buffer
	rt := core.New()
	defer rt.Close(context.Background())
	h := &host{rt: rt, out: &output{writer: &buffer}, state: "running", previous: map[string]pluginView{}}
	if err := rt.Register(sdk.Definition{Manifest: sdk.Manifest{ID: "fixture"}, Factory: func() (sdk.Plugin, error) { return sdk.StartFunc(func(sdk.Context) error { return nil }), nil }}); err != nil {
		t.Fatal(err)
	}
	h.publish()
	size := buffer.Len()
	h.publish()
	if buffer.Len() != size {
		t.Fatal("unchanged snapshot produced duplicate logs")
	}
}
