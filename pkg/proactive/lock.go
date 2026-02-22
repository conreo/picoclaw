package proactive

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type FileLock struct {
	path string
	file *os.File
}

func NewFileLock(path string) *FileLock {
	lockDir := filepath.Dir(path)
	os.MkdirAll(lockDir, 0o755)
	return &FileLock{
		path: path + ".lock",
	}
}

func (l *FileLock) Lock() error {
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create lock file: %w", err)
	}

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	l.file = file
	return nil
}

func (l *FileLock) Unlock() error {
	if l.file == nil {
		return nil
	}

	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	l.file.Close()
	l.file = nil

	os.Remove(l.path)
	return nil
}

func (l *FileLock) TryLock(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := l.Lock()
		if err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("lock acquisition timed out after %v", timeout)
}

type LockedFile struct {
	lock *FileLock
	file *os.File
}

func AcquireLockedFile(path string, timeout time.Duration) (*LockedFile, error) {
	lock := NewFileLock(path)

	var err error
	if timeout > 0 {
		err = lock.TryLock(timeout)
	} else {
		err = lock.Lock()
	}

	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		lock.Unlock()
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return &LockedFile{
		lock: lock,
		file: file,
	}, nil
}

func (lf *LockedFile) File() *os.File {
	return lf.file
}

func (lf *LockedFile) Release() error {
	var errs []error

	if lf.file != nil {
		if err := lf.file.Close(); err != nil {
			errs = append(errs, err)
		}
		lf.file = nil
	}

	if lf.lock != nil {
		if err := lf.lock.Unlock(); err != nil {
			errs = append(errs, err)
		}
		lf.lock = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during release: %v", errs)
	}
	return nil
}

func WithLock(path string, timeout time.Duration, fn func() error) error {
	lock := NewFileLock(path)

	var err error
	if timeout > 0 {
		err = lock.TryLock(timeout)
	} else {
		err = lock.Lock()
	}

	if err != nil {
		return err
	}
	defer lock.Unlock()

	return fn()
}
