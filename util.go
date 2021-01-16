package main

import "os"

func IntPtr(a int) *int {
	return &a
}

// https://stackoverflow.com/a/22467409/712014
func FileExists(name string) (bool, error) {
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err != nil, err
}
