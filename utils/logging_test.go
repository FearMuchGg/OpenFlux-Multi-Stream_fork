package utils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// LogToFile writes next to the executable, which for a test binary is a
// temporary directory — so this does not litter the source tree.
func TestLogToFileCreatesAndReceives(t *testing.T) {
	path, err := LogToFile()
	if err != nil {
		t.Fatalf("LogToFile: %v", err)
	}
	// Leave the global loggers pointing somewhere sane for anything after this.
	defer SetOutput(os.Stderr)
	defer os.Remove(path)

	if !filepath.IsAbs(path) {
		t.Fatalf("expected an absolute path, got %s", path)
	}
	if filepath.Ext(path) != ".log" {
		t.Fatalf("expected a .log file, got %s", path)
	}
	if !strings.HasPrefix(filepath.Base(path), "openflux-") {
		t.Fatalf("unexpected file name %s", filepath.Base(path))
	}

	// The debug logger must land in the file, not just the standard one.
	SetDebug(true)
	Debugf("hello from the debug logger")
	SetDebug(false)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !strings.Contains(string(data), "hello from the debug logger") {
		t.Fatalf("debug line missing from the file, got %q", data)
	}
}

// Two runs must not share a file, or the log of the run that failed is lost to
// the restart that follows it.
func TestLogToFileDoesNotClobberPreviousRun(t *testing.T) {
	first, err := LogToFile()
	if err != nil {
		t.Fatalf("LogToFile: %v", err)
	}
	SetOutput(os.Stderr)
	defer os.Remove(first)

	if err := os.WriteFile(first, []byte("run one\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// The name has one-second resolution, so step past it to make the check
	// meaningful.
	time.Sleep(1100 * time.Millisecond)

	second, err := LogToFile()
	if err != nil {
		t.Fatalf("LogToFile: %v", err)
	}
	defer SetOutput(os.Stderr)
	defer os.Remove(second)

	if first == second {
		t.Fatalf("a second run reused the same file: %s", first)
	}

	data, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("first log disappeared: %v", err)
	}
	if string(data) != "run one\n" {
		t.Fatalf("first log was modified: %q", data)
	}
}
