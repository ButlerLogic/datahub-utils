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
