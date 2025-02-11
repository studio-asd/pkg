package git

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

type Git struct {
	// dir is a valid git direcotry with .git directory inside of it.
	dir string
}

func New(dir string) (*Git, error) {
	_, err := exec.LookPath("git")
	if err != nil {
		return nil, err
	}
	// Ensure the directory is really a directory.
	stat, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	// As reading the hidden directory requires more trick, for git we can just invoke a simple git stats
	// to understand whether the .git is inside the directory or not.
	cmd := exec.Command("git", "status")
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return &Git{dir: dir}, nil
}

// RepositoryRoot returns the path of root repository using git command.
func (g *Git) RepositoryRoot() (string, error) {
	cmd := exec.CommandContext(context.Background(), "git", "rev-parse", "--show-toplevel")
	cmd.Dir = g.dir

	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(string(out), "\n", ""), nil
}
