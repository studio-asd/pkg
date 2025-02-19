package srun

import "testing"

func TestParseFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		args             []string
		expectConfig     string
		expectFeatures   map[string]bool
		expectVersion    bool
		expectTestConfig bool
	}{
		{
			name:         "config",
			args:         []string{"--config=some.yaml"},
			expectConfig: "some.yaml",
		},
		{
			name:          "version",
			args:          []string{"--version"},
			expectVersion: true,
		},
		{
			name:             "test config",
			args:             []string{"--test-config"},
			expectTestConfig: true,
		},
		{
			name: "feature_flags",
			args: []string{
				"--feature.enable=feature_1",
				"--feature.enable=feature_2",
				"--feature.enable=feature_3",
			},
			expectFeatures: map[string]bool{
				"feature_1": true,
				"feature_2": true,
				"feature_3": true,
			},
		},
		{
			name: "all flags",
			args: []string{
				"--config=some.yaml",
				"--feature.enable=feature_1",
				"--feature.enable=feature_2",
				"--feature.enable=feature_3",
				"--version",
				"--test-config",
			},
			expectConfig: "some.yaml",
			expectFeatures: map[string]bool{
				"feature_1": true,
				"feature_2": true,
				"feature_3": true,
			},
			expectVersion:    true,
			expectTestConfig: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFlags()
			if err := f.Parse(test.args...); err != nil {
				t.Fatal(err)
			}
			if test.expectConfig != f.config {
				t.Fatalf("expecting config %s but got %s", test.expectConfig, f.config)
			}
			if test.expectTestConfig != f.testConfig {
				t.Fatalf("expecting test config %v but got %v", test.expectTestConfig, f.testConfig)
			}
			if len(test.expectFeatures) != len(f.featureFlags) {
				t.Fatalf("feature_flag: expecting length of %d but got %d", len(test.expectFeatures), len(f.featureFlags))
			}
			for k, v := range test.expectFeatures {
				ok, exist := f.featureFlags[k]
				if !exist {
					t.Fatalf("flag %s does not exists", k)
				}
				if ok != v {
					t.Fatalf("expecting value of %v for key %s but got %v", v, k, ok)
				}
				if !f.IsFeaturective(k) {
					t.Fatalf("feature %s is not active", k)
				}
			}
		})
	}
}
