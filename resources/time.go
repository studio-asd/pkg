package resources

import "time"

// Duration type supports MarshalYAML and UnmarshalYAML.
type Duration time.Duration

// MarshalYAML for marshaling duration to yaml.
func (d Duration) MarshalYAML() (any, error) {
	dur := time.Duration(d)
	return dur.String(), nil
}

// UnmarshalYAML for unmarshaling duration to yaml.
func (d *Duration) UnmarshalYAML(unmarshal func(any) error) error {
	var durationStr string
	if err := unmarshal(&durationStr); err != nil {
		return err
	}

	// The -1 is a special treatment because we sometimes want to use value less than 0
	// to be a special cases. This allow the time configuration to be specified without
	// a time unit such as 's', 'm', etc.
	if durationStr == "-1" {
		*d = Duration(time.Duration(-1))
		return nil
	}

	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		return err
	}

	*d = Duration(dur)
	return nil
}
