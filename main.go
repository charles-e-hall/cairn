package main

import (
	"cairn/cairn"
	"fmt"
)

func main() {
	client, err := cairn.NewClient()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Host %s", client.Host)
}
