build:
	dep ensure
	protoc --go_out=. inventory-item-write/proto/item_events.proto           
	env GOOS=linux go build -ldflags="-s -w" -o bin/inventory-item-write inventory-item-write/*.go