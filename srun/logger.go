package srun

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

const (
	LogFormatText = "text"
	LogFormatJSON = "json"

	loggerAppNameKey    = "app_name"
	loggerAppVersionKey = "app_version"
	loggerGoVersionKey  = "go_version"
)

const defaultLogLevel = slog.LevelInfo

// LoggerConfig configures log/slog logger and put the configuration result as the default logger to slog.
type LoggerConfig struct {
	Format     string // Either a 'text' or 'json'. We use 'json' by default.
	RemoveTime bool   // Removes the time from logger.
	AddSource  bool   // Adds source code location when logging.
	Level      slog.Level
	// Output overrides and control the output of the program log. By default, all logs will be sent to os.Stderr.
	Output io.Writer

	// Below information is injected inside the library.
	appName    string
	appVersion string
	goVersion  string
}

func (l *LoggerConfig) Validate() error {
	if l.Format == "" {
		l.Format = LogFormatText
	}
	// If the log format from GOPKG_LOG_FORMAT is not empty, then we need to respect the env variable.
	logFormat := os.Getenv("GOPKG_LOG_FORMAT")
	if logFormat != "" {
		l.Format = logFormat
	}

	if l.Output == nil {
		l.Output = os.Stderr
	}
	return nil
}

func setDefaultSlog(config LoggerConfig) {
	var handler slog.Handler
	var replacerFunc func([]string, slog.Attr) slog.Attr

	// Ensure format is not empty, if format empty then we change the format back to text.
	switch config.Format {
	case LogFormatJSON, LogFormatText:
	default:
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
	// If testing then we should force the removal of several key attributes.
	if testing.Testing() {
		replacerFunc = func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 {
				switch attr.Key {
				case slog.TimeKey, loggerAppVersionKey, loggerAppNameKey, loggerGoVersionKey:
					return slog.Attr{}
				}
			}
			return attr
		}
	}

	// By default, send all the logs to os.Stderr, but overrides the configuration with user parameters.
	var output io.Writer = os.Stderr
	if config.Output != nil {
		output = config.Output
	}

	logLevel := config.Level
	switch logLevel {
	case slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError:
	default:
		logLevel = defaultLogLevel
	}

	switch strings.ToLower(config.Format) {
	case LogFormatJSON:
		handler = slog.NewJSONHandler(
			output, &slog.HandlerOptions{
				AddSource:   config.AddSource,
				ReplaceAttr: replacerFunc,
				Level:       logLevel,
			},
		)
	case LogFormatText:
		handler = slog.NewTextHandler(
			output,
			&slog.HandlerOptions{
				AddSource:   config.AddSource,
				ReplaceAttr: replacerFunc,
				Level:       logLevel,
			},
		)
	}
	logger := slog.New(handler).With(
		slog.String(loggerAppNameKey, config.appName),
		slog.String(loggerAppVersionKey, config.appVersion),
		slog.String(loggerGoVersionKey, config.goVersion),
	)
	slog.SetDefault(logger)
}
