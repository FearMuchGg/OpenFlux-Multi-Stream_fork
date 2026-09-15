package utils

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

var (
	debugLog *log.Logger
	verbose  bool
	output   io.Writer = os.Stderr
)

// SetOutput redirects all debug and standard log output to w.
// Used by the mobile bridge to pipe logs into the app UI.
func SetOutput(w io.Writer) {
	output = w
	log.SetOutput(w)
	if debugLog != nil {
		debugLog.SetOutput(w)
	}
}

func EnableDebug() {
	verbose = true
	debugLog = log.New(output, "", log.LstdFlags|log.Lmicroseconds)
	log.SetOutput(output)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}

func Debugf(format string, args ...interface{}) {
	if verbose {
		debugLog.Output(2, fmt.Sprintf(format, args...))
	}
}

// SetDebug toggles verbose logging at runtime (off = Debugf becomes a no-op).
func SetDebug(on bool) {
	if on {
		EnableDebug()
		return
	}
	verbose = false
}

// LogToFile starts writing every log line to a timestamped file as well as to
// stderr, and returns the path it opened.
//
// A file per run rather than one fixed name: on a server the interesting log is
// the one from the run that went wrong, and a fixed name would have been
// overwritten by the restart that followed.
//
// The file goes next to the executable, which is what "the same directory"
// normally means for a server binary and, unlike the working directory, does
// not move under systemd or a nohup started from elsewhere. If that directory
// is not writable the working directory is used instead.
func LogToFile() (string, error) {
	name := "openflux-" + time.Now().Format("20060102-150405") + ".log"

	path := name
	if dir, err := executableDir(); err == nil {
		path = filepath.Join(dir, name)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil && path != name {
		// Not writable next to the binary (read-only install directory, for
		// instance) — fall back to the working directory before giving up.
		path = name
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	}
	if err != nil {
		return "", err
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}

	// Tee rather than redirect: running in the foreground should still show
	// output, and the file is what survives.
	SetOutput(io.MultiWriter(os.Stderr, f))
	return abs, nil
}

func executableDir() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	// Resolve symlinks so a binary symlinked into /usr/local/bin logs next to
	// its real location rather than next to the link.
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return filepath.Dir(exe), nil
}

func IsVerbose() bool {
	return verbose
}

// SafeGo runs fn in a new goroutine, recovering from any panic so a crash in
// one worker cannot take down the whole process (critical when this code runs
// embedded as a library inside a mobile app).
func SafeGo(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				Debugf("[PANIC] recovered in %s: %v", name, r)
			}
		}()
		fn()
	}()
}
