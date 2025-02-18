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
	// testConfig define that the program is in the test configuration mode and not to run the program entirely.
	// Sometimes when testing our build for an environment we want to test them out to really sure that the program and
	// the configuration can run properly before it really starts. It improves the feedback loop to the engineers because
	// we know the problem sooner.
	testConfig bool
	// featureFlags stores all the feature enabled by the flags.
	featureFlags map[string]bool
	// config stores the name/path of the configuration for the program.
	config string
}

func newFlags() *Flags {
	fs := flag.NewFlagSet("srun", flag.ExitOnError)
	return &Flags{
		fs:           fs,
		featureFlags: make(map[string]bool),
	}
}

func (f *Flags) Parse(args ...string) error {
	flagArgs := args

	var featureFlags arrayFlag
	f.fs.Var(&featureFlags, "feature.enable", "--feature.enable=feature_name")
	f.fs.StringVar(&f.config, "config", "", "--config=config_name")
	f.fs.BoolVar(&f.version, "version", false, "--version")
	f.fs.BoolVar(&f.testConfig, "test-config", false, "--test-config")
	f.fs.Parse(flagArgs)

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

func (f *Flags) TestConfig() bool {
	return f.testConfig
}
