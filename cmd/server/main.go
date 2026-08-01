package main

import (
	"fmt"
	"log"

	"eventhub/internal/config"
)

func main() {
	appConfig, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	fmt.Printf("EventHub server starting on port %s\n", appConfig.HTTPPort)
}
