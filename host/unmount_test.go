package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"testbot/core"
)

func TestUnmountProviderKeepsDependentWaitingAndAllowsRemount(t *testing.T) {
	h := mountHost(t)
	consumer := mountFixture(t, h, "consumer", "await ctx.call('fixture.info', 'info', null)", "fixture.info")
	if err := h.mount(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	provider := mountFixture(t, h, "provider", "await ctx.provide('fixture.info', async () => 'ready')")
	manifest, _ := json.Marshal(pluginConfig{ID: "provider", Command: []string{"node", "plugin.mts"}, Provides: []string{"fixture.info"}})
	if err := os.WriteFile(filepath.Join(provider, "plugin.json"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.mount(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	previous, _ := h.rt.Status("consumer")
	marker := filepath.Join(provider, "user-data")
	if err := os.WriteFile(marker, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.execute(context.Background(), request{Method: "unmount", Plugin: "provider"}); err != nil {
		t.Fatal(err)
	}
	if _, found := h.rt.Status("provider"); found {
		t.Fatal("provider definition retained")
	}
	current, _ := h.rt.Status("consumer")
	if current.State != core.Waiting || !current.Enabled || h.rt.Stats().Resources != 0 || h.rt.Stats().Dirty != 0 {
		t.Fatal(current, h.rt.Stats())
	}
	cfg, data, err := readConfigFile(h.configPath)
	if err != nil || len(cfg.Plugins) != 1 || cfg.Plugins[0].ID != "consumer" || cfg.DisposeTimeoutMS != 1000 || !bytes.Equal(data, h.configData) {
		t.Fatal(cfg, err)
	}
	if _, found := h.plugins["provider"]; found {
		t.Fatal("host definition retained")
	}
	if info, err := os.Stat(h.configPath); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "keep" {
		t.Fatal("plugin data removed", err)
	}
	if err := h.unmount(context.Background(), "provider"); !errors.Is(err, core.ErrPluginNotFound) {
		t.Fatal(err)
	}
	if err := h.mount(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	current, _ = h.rt.Status("consumer")
	if current.State != core.Running || current.Generation <= previous.Generation {
		t.Fatal("dependent did not restart", current)
	}
}

func TestUnmountWaitingStoppedAndFailedPlugins(t *testing.T) {
	for _, state := range []string{"waiting", "stopped", "failed"} {
		t.Run(state, func(t *testing.T) {
			h := mountHost(t)
			var required []string
			if state == "waiting" {
				required = []string{"missing.info"}
			}
			dir := mountFixture(t, h, "fixture", "", required...)
			if err := h.mount(context.Background(), dir); err != nil {
				t.Fatal(err)
			}
			if state == "stopped" {
				if err := h.rt.Stop(context.Background(), "fixture"); err != nil {
					t.Fatal(err)
				}
			}
			if state == "failed" {
				sdk, _ := json.Marshal(filepath.Join(h.root, "core/bridge/sdk/typescript/sdk.mts"))
				code := "import { run } from " + string(sdk) + "\nawait run(async () => { throw new Error('failed restart') })"
				if err := os.WriteFile(filepath.Join(dir, "plugin.mts"), []byte(code), 0600); err != nil {
					t.Fatal(err)
				}
				if err := h.rt.Reload(context.Background(), "fixture"); err == nil {
					t.Fatal("reload succeeded")
				}
			}
			if err := h.unmount(context.Background(), "fixture"); err != nil {
				t.Fatal(err)
			}
			if len(h.rt.Snapshot()) != 0 || h.rt.Stats().Resources != 0 || len(h.config.Plugins) != 0 {
				t.Fatal(h.current(), h.config)
			}
		})
	}
}

func TestUnmountDirtyCleanupRetainsConfigUntilRetry(t *testing.T) {
	h := mountHost(t)
	dir := mountFixture(t, h, "fixture", "let first = true\nawait ctx.defer('external resource', async () => { if (first) { first = false; throw new Error('cleanup failed once') } })")
	if err := h.mount(context.Background(), dir); err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), h.configData...)
	if err := h.unmount(context.Background(), "fixture"); !errors.Is(err, core.ErrQuarantined) {
		t.Fatal(err)
	}
	current, found := h.rt.Status("fixture")
	data, _ := os.ReadFile(h.configPath)
	if !found || current.State != core.Quarantined || !bytes.Equal(data, original) || len(h.config.Plugins) != 1 {
		t.Fatal(current, h.config)
	}
	if err := h.rt.Start(context.Background(), "fixture"); !errors.Is(err, core.ErrQuarantined) {
		t.Fatal("dirty plugin restarted", err)
	}
	if err := h.execute(context.Background(), request{Method: "retryCleanup", Plugin: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if current, found = h.rt.Status("fixture"); !found || current.State != core.Stopped || len(h.config.Plugins) != 1 {
		t.Fatal("retry removed configured plugin", current)
	}
	if err := h.unmount(context.Background(), "fixture"); err != nil {
		t.Fatal(err)
	}
	if len(h.rt.Snapshot()) != 0 || h.rt.Stats().Dirty != 0 || len(h.config.Plugins) != 0 {
		t.Fatal(h.current(), h.config)
	}
	files, _ := filepath.Glob(filepath.Join(filepath.Dir(h.configPath), ".plugins-unmount-*"))
	if len(files) != 0 {
		t.Fatal("temporary configs retained", files)
	}
}

func TestUnmountRejectsExternalConfigEditsBeforeAndDuringCleanup(t *testing.T) {
	for _, during := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "during"}[during], func(t *testing.T) {
			h := mountHost(t)
			file, _ := json.Marshal(h.configPath)
			external := []byte("{\"plugins\":[],\"disposeTimeoutMs\":1234}")
			content, _ := json.Marshal(string(external))
			body := ""
			if during {
				body = "await ctx.defer('edit config', async () => { const fs = await import('node:fs/promises'); await fs.writeFile(" + string(file) + ", " + string(content) + ") })"
			}
			dir := mountFixture(t, h, "fixture", body)
			if err := h.mount(context.Background(), dir); err != nil {
				t.Fatal(err)
			}
			if !during {
				if err := os.WriteFile(h.configPath, external, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := h.unmount(context.Background(), "fixture"); err == nil || !strings.Contains(err.Error(), "configuration changed") {
				t.Fatal(err)
			}
			data, _ := os.ReadFile(h.configPath)
			current, found := h.rt.Status("fixture")
			if !found || !bytes.Equal(data, external) || len(h.config.Plugins) != 1 {
				t.Fatal(current, h.config)
			}
			if !during && current.State != core.Running || during && current.State != core.Stopped {
				t.Fatal(current)
			}
		})
	}
}

func TestUnmountSaveFailureRestoresDefinitionWithoutRestart(t *testing.T) {
	h := mountHost(t)
	directory, _ := json.Marshal(filepath.Dir(h.configPath))
	body := "await ctx.defer('remove pending config', async () => { const fs = await import('node:fs/promises'); for (const file of await fs.readdir(" + string(directory) + ")) if (file.startsWith('.plugins-unmount-')) await fs.unlink(" + string(directory) + " + '/' + file) })"
	dir := mountFixture(t, h, "fixture", body)
	if err := h.mount(context.Background(), dir); err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), h.configData...)
	if err := h.unmount(context.Background(), "fixture"); err == nil || !strings.Contains(err.Error(), "was not saved") {
		t.Fatal(err)
	}
	current, found := h.rt.Status("fixture")
	data, _ := os.ReadFile(h.configPath)
	if !found || current.Enabled || h.rt.Stats().Resources != 0 || !bytes.Equal(data, original) || len(h.plugins) != 1 {
		t.Fatal("save failure lost definition or restarted plugin", current, h.config)
	}
	if err := h.unmount(context.Background(), "fixture"); err != nil {
		t.Fatal(err)
	}
	if len(h.rt.Snapshot()) != 0 || len(h.config.Plugins) != 0 {
		t.Fatal(h.current(), h.config)
	}
}

func TestUnmountCancellationBeforeStartKeepsPluginRunning(t *testing.T) {
	h := mountHost(t)
	dir := mountFixture(t, h, "fixture", "")
	if err := h.mount(context.Background(), dir); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.unmount(ctx, "fixture"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	current, _ := h.rt.Status("fixture")
	if current.State != core.Running || len(h.config.Plugins) != 1 {
		t.Fatal(current, h.config)
	}
}

func TestUnmountProviderWaitsForDirtyDependent(t *testing.T) {
	h := mountHost(t)
	consumer := mountFixture(t, h, "consumer", "let first = true\nawait ctx.defer('dependent resource', async () => { if (first) { first = false; throw new Error('dependent cleanup failed') } })", "fixture.info")
	if err := h.mount(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	provider := mountFixture(t, h, "provider", "await ctx.provide('fixture.info', async () => 'ready')")
	manifest, _ := json.Marshal(pluginConfig{ID: "provider", Command: []string{"node", "plugin.mts"}, Provides: []string{"fixture.info"}})
	if err := os.WriteFile(filepath.Join(provider, "plugin.json"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.mount(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), h.configData...)
	if err := h.unmount(context.Background(), "provider"); !errors.Is(err, core.ErrQuarantined) {
		t.Fatal(err)
	}
	for _, id := range []string{"provider", "consumer"} {
		current, found := h.rt.Status(id)
		if !found || current.State != core.Quarantined || len(current.Resources) == 0 {
			t.Fatal("dependent released its provider too soon", current)
		}
	}
	if data, _ := os.ReadFile(h.configPath); !bytes.Equal(data, original) {
		t.Fatal("dirty subtree removed from config")
	}
	if err := h.execute(context.Background(), request{Method: "retryCleanup", Plugin: "provider"}); err != nil {
		t.Fatal(err)
	}
	if err := h.unmount(context.Background(), "provider"); err != nil {
		t.Fatal(err)
	}
	current, _ := h.rt.Status("consumer")
	if current.State != core.Waiting || !current.Enabled || h.rt.Stats().Resources != 0 || len(h.config.Plugins) != 1 {
		t.Fatal(current, h.config)
	}
}
