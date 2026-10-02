package main

import (
	"encoding/json"
	"io"
	"strings"
	"sync"
	"time"

	"testbot/core"
)

type resourceView struct {
	Label string `json:"label"`
	Kind  string `json:"kind"`
	Dirty bool   `json:"dirty"`
	Error string `json:"error,omitempty"`
}

type pluginView struct {
	ID         string         `json:"id"`
	State      core.State     `json:"state"`
	Enabled    bool           `json:"enabled"`
	Generation uint64         `json:"generation"`
	Missing    []string       `json:"missing"`
	Error      string         `json:"error,omitempty"`
	Resources  []resourceView `json:"resources"`
	Provides   []string       `json:"provides"`
	Required   []string       `json:"required"`
	Optional   []string       `json:"optional"`
}

type snapshot struct {
	Version string       `json:"version"`
	State   string       `json:"state"`
	Plugins []pluginView `json:"plugins"`
	Stats   core.Stats   `json:"stats"`
}

func view(s core.Status) pluginView {
	v := pluginView{ID: s.ID, State: s.State, Enabled: s.Enabled, Generation: s.Generation, Missing: []string{}, Resources: []resourceView{}, Provides: []string{}, Required: []string{}, Optional: []string{}}
	if s.Error != nil {
		v.Error = s.Error.Error()
	}
	for _, key := range s.Missing {
		v.Missing = append(v.Missing, key.Name)
	}
	for _, r := range s.Resources {
		item := resourceView{Label: r.Label, Kind: r.Kind, Dirty: r.Dirty}
		if r.Error != nil {
			item.Error = r.Error.Error()
		}
		v.Resources = append(v.Resources, item)
	}
	return v
}

type output struct {
	locale map[string]string
	mu     sync.Mutex
	writer io.Writer
	err    error
}

func (o *output) send(value any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return
	}
	o.err = json.NewEncoder(o.writer).Encode(value)
}

func (o *output) log(level, source, message string) {
	o.send(map[string]any{"type": "log", "time": time.Now().UTC().Format(time.RFC3339Nano), "level": level, "source": source, "message": message})
}

type logWriter struct {
	mu      sync.Mutex
	out     *output
	source  string
	partial string
}

func (w *logWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.partial != "" {
		w.line(w.partial)
		w.partial = ""
	}
}

func (w *logWriter) line(text string) {
	for _, level := range []string{"info", "ok", "warn", "error"} {
		prefix := "[" + strings.ToUpper(level) + "]"
		if strings.HasPrefix(text, prefix) {
			w.out.log(level, w.source, strings.TrimLeft(text[len(prefix):], " "))
			return
		}
	}
	w.out.log("info", w.source, text)
}

func (w *logWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.partial += string(data)
	for {
		i := strings.IndexByte(w.partial, '\n')
		if i < 0 {
			if len(w.partial) >= 8192 {
				w.line(w.partial)
				w.partial = ""
			}
			break
		}
		w.line(strings.TrimSuffix(w.partial[:i], "\r"))
		w.partial = w.partial[i+1:]
	}
	return len(data), nil
}
