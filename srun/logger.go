package srun

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

const (
	LogFormatText = "text"
	LogFormatJSON = "json"
)

// LoggerConfig configures log/slog logger and put the configuration result as the default logger to slog.
type LoggerConfig struct {
	Format     string // Either a 'text' or 'json'. We use 'json' by default.
	RemoveTime bool   // Removes the time from logger.
	AddSource  bool   // Adds source code location when logging.
	// Output overrides and control the output of the program log. By default, all logs will be sent to os.Stderr.
	Output io.Writer
}

func setDefaultSlog(config LoggerConfig) {
	var handler slog.Handler
	var replacerFunc func([]string, slog.Attr) slog.Attr

	// Set the default format of logging to text.
	if config.Format == "" {
		config.Format = LogFormatText
	}
	// Remove time from the slog logger by checking the attributes when logging.
	if config.RemoveTime {
		replacerFunc = func(groups []string, attr slog.Attr) slog.Attr {
			// Remove time.
			if attr.Key == slog.TimeKey && len(groups) == 0 {
				return slog.Attr{}
			}
			return attr
		}
	}

	// By default, send all the logs to os.Stderr, but overrides the configuration with user parameters.
	var output io.Writer = os.Stderr
	if config.Output != nil {
		output = config.Output
	}

	switch strings.ToLower(config.Format) {
	case LogFormatJSON:
		handler = slog.NewJSONHandler(
			output, &slog.HandlerOptions{
				AddSource:   config.AddSource,
				ReplaceAttr: replacerFunc,
			},
		)
	case LogFormatText:
		handler = slog.NewTextHandler(
			output,
			&slog.HandlerOptions{
				AddSource:   config.AddSource,
				ReplaceAttr: replacerFunc,
			},
		)
	}
	slog.SetDefault(slog.New(handler))
}
