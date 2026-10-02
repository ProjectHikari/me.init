package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func loadLocale(root string) (map[string]string, error) {
	var catalogs map[string]map[string]string
	data, err := os.ReadFile(filepath.Join(root, "me.init", "locale.json"))
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, &catalogs); err != nil {
		return nil, err
	}
	return catalogs[os.Getenv("ME_LANGUAGE")], nil
}

func (o *output) text(template string, params ...any) string {
	if translated := o.locale[template]; translated != "" {
		template = translated
	}
	if len(params) == 0 {
		return template
	}
	return fmt.Sprintf(template, params...)
}
