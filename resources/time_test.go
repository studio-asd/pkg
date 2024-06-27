package resources

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gopkg.in/yaml.v3"
)

var testData = `time1: 1s
time2: 10m0s
time3: -1s
time4: -1ns
`

func TestUnmarshal(t *testing.T) {
	t.Parallel()

	var config struct {
		Time1       Duration `yaml:"time1"`
		expectTime1 Duration
		Time2       Duration `yaml:"time2"`
		expectTime2 Duration
		Time3       Duration `yaml:"time3"`
		expectTime3 Duration
		Time4       Duration `yaml:"time4"`
		expectTime4 Duration
	}
	config.expectTime1 = Duration(time.Second)
	config.expectTime2 = Duration(time.Minute * 10)
	config.expectTime3 = Duration(time.Second * -1)
	config.expectTime4 = Duration(-1)

	if err := yaml.Unmarshal([]byte(testData), &config); err != nil {
		t.Fatal(err)
	}

	if config.Time1 != config.expectTime1 {
		t.Fatalf("expecting %d but got %d", config.expectTime1, config.Time1)
	}
	if config.Time2 != config.expectTime2 {
		t.Fatalf("expecting %d but got %d", config.expectTime2, config.Time2)
	}
	if config.Time3 != config.expectTime3 {
		t.Fatalf("expecting %d but got %d", config.expectTime3, config.Time3)
	}
	if config.Time4 != config.expectTime4 {
		t.Fatalf("expecting %d but got %d", config.expectTime4, config.Time4)
	}
}

func TestMarshal(t *testing.T) {
	t.Parallel()

	var config struct {
		Time1 Duration `yaml:"time1"`
		Time2 Duration `yaml:"time2"`
		Time3 Duration `yaml:"time3"`
		Time4 Duration `yaml:"time4"`
	}
	config.Time1 = Duration(time.Second)
	config.Time2 = Duration(time.Minute * 10)
	config.Time3 = Duration(time.Second * -1)
	config.Time4 = Duration(-1)

	outMarshal, err := yaml.Marshal(&config)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(testData, string(outMarshal)); diff != "" {
		t.Fatalf("(-want/+got)\n%s", diff)
	}
}
