package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func readManifest(root, path string) (pluginConfig, error) {
	var p pluginConfig
	if strings.TrimSpace(path) == "" {
		return p, fmt.Errorf("plugin path is required")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return p, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return p, err
	}
	if info.IsDir() {
		path = filepath.Join(path, "plugin.json")
	}
	f, err := os.Open(path)
	if err != nil {
		return p, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil {
		return p, err
	}
	if len(data) > 64<<10 {
		return p, fmt.Errorf("plugin manifest exceeds 64 KiB")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&p); err != nil {
		return p, err
	}
	if d.Decode(new(any)) != io.EOF {
		return p, fmt.Errorf("manifest must contain one JSON object")
	}
	if p.Dir == "" {
		p.Dir = filepath.Dir(path)
	} else if !filepath.IsAbs(p.Dir) && !strings.Contains(p.Dir, "${ROOT}") {
		p.Dir = filepath.Join(filepath.Dir(path), p.Dir)
	}
	// mount显式启用插件，保存后的启动行为保持一致
	p.Autostart = true
	return p, nil
}

func (h *host) unchangedConfig() error {
	data, err := os.ReadFile(h.configPath)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, h.configData) {
		return fmt.Errorf("%s", h.out.text("plugin configuration changed; restart CLI before mounting"))
	}
	return nil
}

func (h *host) forgetMount(id string) {
	h.mu.Lock()
	delete(h.plugins, id)
	delete(h.mountRollback, id)
	h.mu.Unlock()
}

func (h *host) rollbackMount(id string, cause error) error {
	// 取消挂载后仍须给旧作用域独立的清理时间
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.rt.Unload(ctx, id); err != nil {
		h.mu.Lock()
		if h.mountRollback == nil {
			h.mountRollback = make(map[string]bool)
		}
		h.mountRollback[id] = true
		h.mu.Unlock()
		return errors.Join(cause, fmt.Errorf("%s: %w", h.out.text("mount rollback incomplete for %s; use /retryCleanup", id), err))
	}
	h.forgetMount(id)
	return cause
}

func (h *host) mount(ctx context.Context, path string) error {
	h.mu.Lock()
	state := h.state
	h.mu.Unlock()
	if state != "running" {
		return fmt.Errorf("%s", h.out.text("runtime is not running"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := readManifest(h.root, path)
	if err != nil {
		return err
	}
	h.mu.Lock()
	rollback := h.mountRollback[p.ID]
	h.mu.Unlock()
	if rollback {
		return fmt.Errorf("%s", h.out.text("mount rollback pending for %s; use /retryCleanup", p.ID))
	}
	def, err := p.definition(h.root, filepath.Dir(h.configPath), h.out)
	if err != nil {
		return err
	}
	if len(h.config.Plugins) >= 256 {
		return fmt.Errorf("at most 256 plugins")
	}
	for _, current := range h.config.Plugins {
		if current.ID == p.ID {
			return fmt.Errorf("%s", h.out.text("plugin %s is already configured", p.ID))
		}
	}
	if err = h.unchangedConfig(); err != nil {
		return err
	}
	next := h.config
	next.Plugins = append(append([]pluginConfig(nil), next.Plugins...), p)
	data, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > 1<<20 {
		return fmt.Errorf("config exceeds 1 MiB")
	}
	info, err := os.Stat(h.configPath)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(h.configPath), ".plugins-mount-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	err = temporary.Chmod(info.Mode().Perm())
	if err == nil {
		_, err = temporary.Write(data)
	}
	if err == nil {
		err = temporary.Sync()
	}
	err = errors.Join(err, temporary.Close())
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = h.rt.Register(def); err != nil {
		return err
	}
	h.mu.Lock()
	if h.plugins == nil {
		h.plugins = make(map[string]pluginConfig)
	}
	h.plugins[p.ID] = p
	h.mu.Unlock()
	if err = h.rt.Start(ctx, p.ID); err != nil {
		return h.rollbackMount(p.ID, err)
	}
	if err = ctx.Err(); err == nil {
		err = h.unchangedConfig()
	}
	if err == nil {
		err = os.Rename(temporary.Name(), h.configPath)
	}
	if err != nil {
		return h.rollbackMount(p.ID, err)
	}
	h.config, h.configData = next, data
	h.out.log("ok", "me.init", h.out.text("Mounted %s", p.ID))
	return nil
}
