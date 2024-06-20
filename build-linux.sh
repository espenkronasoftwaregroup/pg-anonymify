mkdir -p release/linux-amd64
env GOOS=linux GOARCH=amd64 go build -o release/linux-amd64 -ldflags "-s -w"