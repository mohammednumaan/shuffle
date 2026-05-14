package mapper

import (
	"fmt"
	"github.com/mohammednumaan/shuffle/internal/config"
)

func Run(cfg *config.Config) {
	fmt.Printf("Running mapper with config: %+v\n", cfg)
}
