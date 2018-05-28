build:
	dep ensure
	env GOOS=linux go build -ldflags="-s -w" -o bin/inventory-item-write inventory-item-write/main.go