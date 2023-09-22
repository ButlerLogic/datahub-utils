package util

import (
	"errors"
	"os"
)

// InsertArgAt inserts an argument into os.Args at the specified position
func InsertArgAt(index int, value string) error {
	if index < 0 || index > len(os.Args) {
		return errors.New("index out of bounds")
	}

	begin := os.Args[:index]
	end := os.Args[index:]
	os.Args = append(append(append([]string{}, begin...), value), end...)

	return nil
}

// RemoveArgAt removes the os.Arg at the specified index
func RemoveArgAt(index int) error {
	if index < 0 || index > len(os.Args) {
		return errors.New("index out of bounds")
	}

	args := os.Args
	list := args[:index]
	list = append(list, args[index:]...)
	os.Args = list

	return nil
}

// InSlice determines whether a value is in the slice.
// Uses generics to specify type.
func InSlice[V comparable](item V, slice []V) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func Map[V comparable](slice []V, fn func(el V) V) []V {
	for i, value := range slice {
		slice[i] = fn(value)
	}

	return slice
}
