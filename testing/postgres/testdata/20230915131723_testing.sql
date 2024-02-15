-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS testing (
	testing_id VARCHAR PRIMARY KEY,
	created_at TIMESTAMPTZ NOT NULL
)
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS testing;
-- +goose StatementEnd
