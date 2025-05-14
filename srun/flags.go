package srun

import (
	"flag"
	"strings"
)

type arrayFlag []string

func (a arrayFlag) String() string {
	return strings.Join(a, "_")
}

func (a *arrayFlag) Set(value string) error {
	*a = append(*a, value)
	return nil
}

// Flags stores the flag that pre-defined by srun.
type Flags struct {
	fs *flag.FlagSet
	// version define that the program will print version instead of run the program.
	version bool
	// test define that the program is in the test mode and not to run the program entirely.
	// Sometimes when testing our build for an environment we want to test them out to really sure that the program and
	// the configuration can run properly before it really starts. It improves the feedback loop to the engineers because
	// we know the problem sooner.
	test bool
	// featureFlags stores all the feature enabled by the flags.
	featureFlags map[string]bool
	// config stores the name/path of the configuration for the program.
	config string
	// hookFn is a hook function that allows the client to parse an additional flags to their program. It uses a custom
	// FlagSet to prevent the client to call Parse from the original FlagSet.
	hookFn func(*FlagSet)
}

func newFlags(hookFn func(*FlagSet)) *Flags {
	fs := flag.NewFlagSet("srun", flag.ExitOnError)
	return &Flags{
		fs:           fs,
		featureFlags: make(map[string]bool),
		hookFn:       hookFn,
	}
}

func (f *Flags) Parse(args ...string) error {
	flagArgs := args

	var featureFlags arrayFlag
	f.fs.Var(&featureFlags, "feature.enable", "--feature.enable=feature_name")
	f.fs.StringVar(&f.config, "config", "", "--config=config_name")
	f.fs.BoolVar(&f.version, "version", false, "--version")
	f.fs.BoolVar(&f.test, "test", false, "--test")
	if f.hookFn != nil {
		f.hookFn(&FlagSet{f.fs})
	}
	if err := f.fs.Parse(flagArgs); err != nil {
		return err
	}

	for _, feature := range featureFlags {
		f.featureFlags[feature] = true
	}
	return nil
}

func (f *Flags) IsFeaturective(name string) bool {
	return f.featureFlags[name]
}

func (f *Flags) Config() string {
	return f.config
}

func (f *Flags) Test() bool {
	return f.test
}

type FlagSet struct {
	*flag.FlagSet
}

// Parse overrides the flagset Parse to disallow the flagset to be parsed on the client side.
func (fs *FlagSet) Parse(arguments []string) error {
	return nil
}
