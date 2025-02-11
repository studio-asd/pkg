package git

import "testing"

func TestRepositoryRoot(t *testing.T) {
	g, err := New(".")
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.RepositoryRoot()
	if err != nil {
		t.Fatal(err)
	}
	if out == "" {
		t.Fatal("repository root is empty")
	}
}
