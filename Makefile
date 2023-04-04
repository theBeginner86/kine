go.fmt:
	go mod tidy
	go fmt ./...

go.vet:
	go vet ./...

go.test:
	go test -v ./test

go.test.dqlite:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -tags=dqlite -v ./test

go.bench:
	go test -v ./test -run "^$$" -bench "Benchmark" -benchmem

go.bench.dqlite:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -tags=dqlite -v ./test -run "^$$" -bench "Benchmark" -benchmem
