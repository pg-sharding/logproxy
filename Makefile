build:
	go build -pgo=auto -o logproxy ./cmd/logproxy 
unittest:
	make build
	go test github.com/pg-sharding/logproxy/test -v -race 