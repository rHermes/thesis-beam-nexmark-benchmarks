package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

func IntPtr(a int) *int {
	return &a
}

// https://stackoverflow.com/a/22467409/712014
func FileExists(name string) (bool, error) {
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func Bprintf(format string, a ...interface{}) []byte {
	return []byte(fmt.Sprintf(format, a...))
}
