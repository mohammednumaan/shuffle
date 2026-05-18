package mapper

import (
	"fmt"
	"os"

	"github.com/mohammednumaan/shuffle/internal/config"
)

func Run(cfg *config.Config) {
	entries, err := os.ReadDir(cfg.InputDir)
	if err != nil {
		fmt.Printf("failed to read input directory: %v\n", err)
		return
	}

	files := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		files = append(files, entry.Name())
	}

	fmt.Printf("Mapper started with files: %v\n", files)

	// Simulate processing the files
	for _, file := range files {
		fmt.Printf("Processing file: %s\n", file)
		// Here you would add your actual mapping logic
	}
}
