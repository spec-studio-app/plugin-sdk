// plugin-conformance runs the protocol tests against a Spec Studio backend plugin.
//
//	plugin-conformance [flags] -- <plugin command> [args]   # stdio
//	plugin-conformance [flags] --url http://127.0.0.1:7070/rpc
//
// It prints each test as pass, fail or skip, and exits with code 1 when a
// test fails, and 2 on a usage error.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/spec-studio-app/plugin-sdk/conformance"
)

// version is set at release time with -ldflags "-X main.version=...".
var version = "0.0.0-dev"

type settingsFlag map[string]string

func (s settingsFlag) String() string { return "" }
func (s settingsFlag) Set(v string) error {
	name, value, ok := strings.Cut(v, "=")
	if !ok || name == "" {
		return fmt.Errorf("a setting is name=value, not %q", v)
	}
	s[name] = value
	return nil
}

func main() {
	fs := flag.NewFlagSet("plugin-conformance", flag.ContinueOnError)
	url := fs.String("url", "", "test the plugin that serves HTTP at this URL (for example http://127.0.0.1:7070/rpc)")
	token := fs.String("token", os.Getenv("PLUGIN_TOKEN"), "the HTTP bearer token (default $PLUGIN_TOKEN)")
	schemas := fs.String("schemas", "", "check each message against the JSON Schemas in this folder")
	run := fs.String("run", "", "run only the tests whose names match this regular expression")
	verbose := fs.Bool("v", false, "show the plugin's stderr")
	showVersion := fs.Bool("version", false, "print the version and exit")
	settings := settingsFlag{}
	fs.Var(settings, "setting", "a setting for the handshake, name=value (repeat the flag for more)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage:\n  plugin-conformance [flags] -- <plugin command> [args]\n  plugin-conformance [flags] --url <url>\n\nFlags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if *showVersion {
		fmt.Println(version)
		return
	}
	target := conformance.Target{
		Command:  fs.Args(),
		URL:      *url,
		Token:    *token,
		Settings: settings,
		Schemas:  *schemas,
		Run:      *run,
	}
	if *verbose {
		target.PluginStderr = os.Stderr
	}
	if (len(target.Command) == 0) == (target.URL == "") {
		fmt.Fprintln(os.Stderr, "Give a plugin command after --, or --url, but not both.")
		fs.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	report, err := conformance.Run(ctx, target, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if report.Failed() {
		os.Exit(1)
	}
}
