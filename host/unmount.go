package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"testbot/core"
)

func (h *host) unmount(ctx context.Context, id string) error {
	if err := h.checkRemoval(id); err != nil {
		return err
	}
	h.mu.Lock()
	state := h.state
	h.mu.Unlock()
	if state != "running" {
		return fmt.Errorf("%s", h.out.text("runtime is not running"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	index := -1
	for i, p := range h.config.Plugins {
		if p.ID == id {
			index = i
			break
		}
	}
	if index < 0 {
		return core.ErrPluginNotFound
	}
	if err := h.unchangedConfig(); err != nil {
		return err
	}
	p := h.config.Plugins[index]
	def, err := p.definition(h.root, filepath.Dir(h.configPath), h.out)
	if err != nil {
		return err
	}
	next := h.config
	next.Plugins = make([]pluginConfig, 0, len(next.Plugins)-1)
	next.Plugins = append(next.Plugins, h.config.Plugins[:index]...)
	next.Plugins = append(next.Plugins, h.config.Plugins[index+1:]...)
	temporary, data, err := h.prepareConfig(next, ".plugins-unmount-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary)
	if err = h.rt.Stop(ctx, id); err != nil {
		current, found := h.rt.Status(id)
		// Failed可能只保留旧故障，资源已回收时仍可卸载
		if ctx.Err() != nil || !found || current.State != core.Failed || len(current.Resources) != 0 {
			return err
		}
	}
	if err = ctx.Err(); err == nil {
		err = h.unchangedConfig()
	}
	if err != nil {
		return err
	}
	restore := func(cause error) error {
		if _, found := h.rt.Status(id); found {
			return cause
		}
		// 配置未提交时恢复未启用的定义，不重启旧实例
		return errors.Join(fmt.Errorf("%s: %w", h.out.text("unmount configuration was not saved; %s remains configured; use /start to recover", id), cause), h.rt.Register(def))
	}
	if err = h.rt.Unload(ctx, id); err != nil {
		return restore(err)
	}
	if err = ctx.Err(); err == nil {
		err = h.unchangedConfig()
	}
	if err == nil {
		err = os.Rename(temporary, h.configPath)
	}
	if err != nil {
		return restore(err)
	}
	h.config, h.configData = next, data
	h.forgetMount(id)
	h.out.log("ok", "me.init", h.out.text("Unmounted %s", id))
	return nil
}

func (h *host) checkRemoval(id string) error {
	switch id {
	case "core", "me.init", "me.cli":
		return fmt.Errorf("%s", h.out.text("critical module %s cannot be stopped or unmounted; use /quit to close Core", id))
	}
	return nil
}
