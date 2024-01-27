package main

import (
	"log"
	"os"

	"github.com/koskimas/norsu/internal/cmd"
)

func main() {
	wd, err := os.Getwd()
	if err != nil {
		log.Fatal("failed to determine working directory")
	}

	err = cmd.Run(cmd.Settings{
		WorkingDir: wd,
	})

	if err != nil {
		log.Fatal(err.Error())
	}
}
