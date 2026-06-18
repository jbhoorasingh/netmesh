BINARY := netmesh
PKG := ./cmd/netmesh
PORT ?= 5999

.PHONY: build test race vet fmt run-controller run-agent clean

build:
	go build -o $(BINARY) $(PKG)

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

## run-controller: start an open controller on $(PORT)
run-controller: build
	./$(BINARY) -master=self -port=$(PORT)

## run-agent MASTER=10.0.0.5 : start an agent joining MASTER
run-agent: build
	./$(BINARY) -master=$(MASTER) -port=$(PORT)

clean:
	rm -f $(BINARY)
