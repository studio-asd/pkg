package pgtest

import (
	"context"
	"sync"

	"github.com/studio-asd/pkg/postgres"
)

type TestHelper[V any] struct {
	conn *postgres.Postgres
	pgt  *PGTest
	// forks is the list of test helper that forked from the origin. The list of forked helper is needed
	// so we can close all the resources when the main helper is closed.
	forks []*TestHelper[V]
	// isFork flag whether the test helper is forked or not. We will apply some conditions on
	// whether the helper is a fork or not.
	isFork bool
	// ensure close is exclusive to one caller only.
	closeMu sync.Mutex
	closed  bool
}

func NewTestHelper[V any](ctx context.Context, config postgres.ConnectConfig) (*TestHelper[V], error) {
	pg, err := postgres.Connect(ctx, config)
	if err != nil {
		return nil, err
	}
	return &TestHelper[V]{
		conn: pg,
	}, nil
}
