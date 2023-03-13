go.fmt:
	go mod tidy
	go fmt ./...

go.vet:
	go vet ./...

go.test:
	go test -v ./test

go.bench:
	go test -v ./test -run "^$$" -bench "Benchmark" -benchtime 5x -benchmem
