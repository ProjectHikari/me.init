package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"testbot/core"
	"testbot/core/bridge"
	"testbot/core/sdk"
)

func mountHost(t *testing.T) *host {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plugins.json")
	if err = os.WriteFile(path, []byte("{\"plugins\":[],\"disposeTimeoutMs\":1000}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, data, err := readConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h := &host{rt: core.New(core.Options{DisposeTimeout: time.Second}), out: &output{writer: io.Discard},
		state: "running", previous: map[string]pluginView{}, plugins: map[string]pluginConfig{},
		root: root, configPath: path, config: cfg, configData: data}
	t.Cleanup(func() { _ = h.rt.Close(context.Background()) })
	return h
}

func mountFixture(t *testing.T, h *host, id, body string, required ...string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "plugin with spaces")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	sdk, _ := json.Marshal(filepath.Join(h.root, "core/bridge/sdk/typescript/sdk.mts"))
	code := "import { run } from " + string(sdk) + "\nawait run(async ctx => {\n" + body + "\n})\n"
	if err := os.WriteFile(filepath.Join(dir, "plugin.mts"), []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(pluginConfig{ID: id, Command: []string{"node", "plugin.mts"}, Required: required})
	if err := os.WriteFile(filepath.Join(dir, "plugin.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestMountPersistsAndRejectsDuplicates(t *testing.T) {
	h := mountHost(t)
	dir := mountFixture(t, h, "fixture", "")
	if err := h.execute(context.Background(), request{Method: "mount", Path: dir}); err != nil {
		t.Fatal(err)
	}
	if state, ok := h.rt.Status("fixture"); !ok || state.State != core.Running {
		t.Fatal(state)
	}
	cfg, data, err := readConfigFile(h.configPath)
	if err != nil || len(cfg.Plugins) != 1 || cfg.Plugins[0].Dir != dir || !cfg.Plugins[0].Autostart || cfg.DisposeTimeoutMS != 1000 {
		t.Fatal(cfg, err)
	}
	info, _ := os.Stat(h.configPath)
	if info.Mode().Perm() != 0600 {
		t.Fatal("config permissions changed")
	}
	if err = h.mount(context.Background(), filepath.Join(dir, "plugin.json")); err == nil {
		t.Fatal("duplicate mounted")
	}
	current, _ := os.ReadFile(h.configPath)
	if !bytes.Equal(data, current) {
		t.Fatal("duplicate changed config")
	}
	files, _ := filepath.Glob(filepath.Join(filepath.Dir(h.configPath), ".plugins-mount-*"))
	if len(files) != 0 {
		t.Fatal("temporary config leaked", files)
	}
}

func TestMountMissingDependencyWaitsAndStartsWhenProviderArrives(t *testing.T) {
	h := mountHost(t)
	consumer := mountFixture(t, h, "consumer", "await ctx.call('fixture.info', 'info', null)", "fixture.info")
	if err := h.mount(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	state, _ := h.rt.Status("consumer")
	if state.State != core.Waiting || state.Generation != 0 {
		t.Fatal(state)
	}
	provider := mountFixture(t, h, "provider", "await ctx.provide('fixture.info', async () => 'ready')")
	data, _ := json.Marshal(pluginConfig{ID: "provider", Command: []string{"node", "plugin.mts"}, Provides: []string{"fixture.info"}})
	if err := os.WriteFile(filepath.Join(provider, "plugin.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.mount(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	state, _ = h.rt.Status("consumer")
	if state.State != core.Running {
		t.Fatal(state)
	}
	if len(h.config.Plugins) != 2 {
		t.Fatal(h.config)
	}
}

func TestMountFailureRollsBackOrRetainsDirtyCleanup(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean", true: "dirty"}[dirty], func(t *testing.T) {
			h := mountHost(t)
			original := append([]byte(nil), h.configData...)
			body := "throw new Error('start failed')"
			if dirty {
				body = "let first = true\nawait ctx.defer('external resource', async () => { if (first) { first = false; throw new Error('retry cleanup') } })\n" + body
			}
			dir := mountFixture(t, h, "fixture", body)
			err := h.mount(context.Background(), dir)
			if err == nil || !strings.Contains(err.Error(), "start failed") {
				t.Fatal(err)
			}
			data, _ := os.ReadFile(h.configPath)
			if !bytes.Equal(data, original) {
				t.Fatal("failed mount persisted")
			}
			if dirty {
				state, found := h.rt.Status("fixture")
				if !found || state.State != core.Quarantined || !h.mountRollback["fixture"] {
					t.Fatal(state, err)
				}
				if err = h.execute(context.Background(), request{Method: "reload", Plugin: "fixture"}); err == nil {
					t.Fatal("rollback restarted")
				}
				if err = h.execute(context.Background(), request{Method: "retryCleanup", Plugin: "fixture"}); err != nil {
					t.Fatal(err)
				}
			}
			if _, found := h.rt.Status("fixture"); found || h.rt.Stats().Resources != 0 || len(h.plugins) != 0 || len(h.mountRollback) != 0 {
				t.Fatal("mount rollback left dirty state", h.current())
			}
		})
	}
}

func TestMountDoesNotOverwriteChangedConfig(t *testing.T) {
	h := mountHost(t)
	original := append([]byte(nil), h.configData...)
	dir := mountFixture(t, h, "fixture", "")
	external := []byte("{\"plugins\":[],\"disposeTimeoutMs\":1234}")
	if err := os.WriteFile(h.configPath, external, 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.mount(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "configuration changed") {
		t.Fatal(err)
	}
	if len(h.rt.Snapshot()) != 0 || !bytes.Equal(original, h.configData) {
		t.Fatal("changed config mounted")
	}
	// 插件初始化期间外部修改配置，提交前也要拒绝覆盖
	if err := os.WriteFile(h.configPath, original, 0600); err != nil {
		t.Fatal(err)
	}
	file, _ := json.Marshal(h.configPath)
	content, _ := json.Marshal(string(external))
	sdk, _ := json.Marshal(filepath.Join(h.root, "core/bridge/sdk/typescript/sdk.mts"))
	code := "import { writeFile } from 'node:fs/promises'\nimport { run } from " + string(sdk) + "\nawait run(async () => { await writeFile(" + string(file) + ", " + string(content) + ") })"
	if err := os.WriteFile(filepath.Join(dir, "plugin.mts"), []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.mount(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "configuration changed") {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(h.configPath)
	if !bytes.Equal(data, external) || len(h.rt.Snapshot()) != 0 || h.rt.Stats().Resources != 0 {
		t.Fatal("commit conflict not rolled back", h.current())
	}
}

func TestMountCancellationKeepsStatusReadableAndReclaimsResources(t *testing.T) {
	h := mountHost(t)
	marker := filepath.Join(t.TempDir(), "entered")
	file, _ := json.Marshal(marker)
	dir := mountFixture(t, h, "slow", "await import('node:fs/promises').then(fs => fs.writeFile("+string(file)+", 'ready'))\nawait new Promise(resolve => ctx.signal.addEventListener('abort', resolve, { once: true }))\nthrow new Error('cancelled')")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.mount(ctx, dir) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("plugin did not start")
		}
		_ = h.current()
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled mount succeeded")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("cancelled mount stuck")
	}
	if len(h.rt.Snapshot()) != 0 || h.rt.Stats().Resources != 0 || len(h.config.Plugins) != 0 {
		t.Fatal("cancelled mount leaked", h.current())
	}
}

func TestMountRejectsInvalidManifest(t *testing.T) {
	for _, manifest := range []string{"null", "{}", `{"id":"fixture","command":["node"],"unknown":true}`, `{"id":"fixture","command":["node"],"events":{"bad":"queue"}}`, `{} {}`, strings.Repeat(" ", 65537)} {
		h := mountHost(t)
		file := filepath.Join(t.TempDir(), "plugin.json")
		if err := os.WriteFile(file, []byte(manifest), 0600); err != nil {
			t.Fatal(err)
		}
		if err := h.mount(context.Background(), file); err == nil {
			t.Fatal("invalid manifest mounted", manifest[:min(len(manifest), 80)])
		}
		if len(h.rt.Snapshot()) != 0 || len(h.config.Plugins) != 0 {
			t.Fatal("invalid manifest changed runtime")
		}
	}
}

func TestMountServiceConflictKeepsExistingProvider(t *testing.T) {
	h := mountHost(t)
	for _, id := range []string{"first", "second"} {
		dir := mountFixture(t, h, id, "await ctx.provide('shared.info', async () => 'ready')")
		data, _ := json.Marshal(pluginConfig{ID: id, Command: []string{"node", "plugin.mts"}, Provides: []string{"shared.info"}})
		if err := os.WriteFile(filepath.Join(dir, "plugin.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		err := h.mount(context.Background(), dir)
		if id == "first" && err != nil {
			t.Fatal(err)
		}
		if id == "second" && !errors.Is(err, sdk.ErrConflict) {
			t.Fatal("provider conflict accepted", err)
		}
	}
	if len(h.config.Plugins) != 1 || len(h.rt.Snapshot()) != 1 {
		t.Fatal("conflict changed registry", h.current())
	}
	state, _ := h.rt.Status("first")
	if state.State != core.Running {
		t.Fatal(state)
	}
}

func TestMountRollbackWaitsForDependentsAndRetryCompletesUnload(t *testing.T) {
	h := mountHost(t)
	first := true
	if err := h.rt.Register(sdk.Definition{Manifest: sdk.Manifest{ID: "consumer", Required: []sdk.ServiceKey{bridge.NewService("fixture.info").Key()}}, Factory: func() (sdk.Plugin, error) {
		return sdk.StartFunc(func(ctx sdk.Context) error {
			return ctx.Defer("consumer resource", func(context.Context) error {
				if first {
					first = false
					return errors.New("dependent cleanup failed once")
				}
				return nil
			})
		}), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := h.rt.Start(context.Background(), "consumer"); err != nil {
		t.Fatal(err)
	}
	file, _ := json.Marshal(h.configPath)
	dir := mountFixture(t, h, "provider", "await ctx.provide('fixture.info', async () => 'ready')\nawait import('node:fs/promises').then(fs => fs.writeFile("+string(file)+", '{\"plugins\":[]}'))")
	data, _ := json.Marshal(pluginConfig{ID: "provider", Command: []string{"node", "plugin.mts"}, Provides: []string{"fixture.info"}})
	if err := os.WriteFile(filepath.Join(dir, "plugin.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	err := h.mount(context.Background(), dir)
	if err == nil || !strings.Contains(err.Error(), "configuration changed") || !h.mountRollback["provider"] {
		t.Fatal(err)
	}
	provider, _ := h.rt.Status("provider")
	consumer, _ := h.rt.Status("consumer")
	if provider.State != core.Quarantined || consumer.State != core.Quarantined {
		t.Fatal(provider, consumer)
	}
	if err = h.execute(context.Background(), request{Method: "retryCleanup", Plugin: "provider"}); err != nil {
		t.Fatal(err)
	}
	if _, found := h.rt.Status("provider"); found || len(h.config.Plugins) != 0 || len(h.mountRollback) != 0 {
		t.Fatal("failed mount retained", h.current())
	}
	consumer, _ = h.rt.Status("consumer")
	if consumer.State != core.Waiting || !consumer.Enabled || h.rt.Stats().Resources != 0 {
		t.Fatal("dependent not restored to waiting", consumer)
	}
}
