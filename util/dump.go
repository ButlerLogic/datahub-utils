package util

import (
	"encoding/json"
	"fmt"
	"os"
)

func Dump(obj interface{}) {
	j, _ := json.MarshalIndent(obj, "", "  ")
	fmt.Println(string(j))
}

func DumpFile(name string, obj interface{}) {
	file, err := os.Create(name)
	if err != nil {
		fmt.Println("Error creating file", err)
		return
	}
	defer file.Close()

	j, _ := json.MarshalIndent(obj, "", "  ")
	_, err = file.Write(j)
}

func DumpLog(name string, obj interface{}) {
	file, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	j, _ := json.MarshalIndent(obj, "", "  ")

	_, err = file.Write(j)
	if err != nil {
		fmt.Println("Error writing file:", err)
	}

	err = file.Close()
	if err != nil {
		fmt.Println("Error closing file:", err)
	}
}
