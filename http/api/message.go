package api

import (
	"context"

	"github.com/albertwidi/pkg/instrumentation"
)

var _ Message = (*I18nMessage)(nil)

// PreferredLanguage is the list of the language that can be preferred by the user.
//
// The value of preferred language in context.Context for http.Request is now controlled
// by x-preferred-language HTTP header.
const (
	PreferredLanguageEN string = "en"
	PreferredLanguageID string = "id"
)

// I18nMessage implements ErrorMessage interface that supports internationalization
// of error message.
type I18nMessage struct {
	ID string
	EN string
}

// String returns the default message from I18Message based on the request context. If the preferred
// language is not known, then English will be used as default language.
func (i18n I18nMessage) String(ctx context.Context) string {
	bg := instrumentation.BaggageFromContext(ctx)
	switch bg.PreferredLanguage {
	case PreferredLanguageEN:
		return i18n._EN()
	case PreferredLanguageID:
		return i18n._ID()
	}
	return i18n._EN()
}

func (i18n I18nMessage) _ID() string {
	return i18n.ID
}

func (i18n I18nMessage) _EN() string {
	return i18n.EN
}
