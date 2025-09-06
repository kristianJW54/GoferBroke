# =============================
# Project
# =============================
BINARY_NAME := goferbroke
MAIN_PACKAGE := ./cmd/server

# =============================
# Git version info
# =============================
VERSION      := $(shell git describe --tags --always)
COMMIT       := $(shell git rev-parse --short HEAD)

# Set correct package for ldflags
VERSION_PKG=github.com/kristianJW54/GoferBroke/internal/version
LDFLAGS=-X $(VERSION_PKG).Version=$(VERSION) -X $(VERSION_PKG).Commit=$(COMMIT)

# Default build
build:
	go build -o $(BINARY_NAME) $(MAIN_PACKAGE)

# Build and run
run: build
	./$(BINARY_NAME)

# Run tests
test:
	go test ./...

# Build with version + commit injected
release:
	go build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME) $(MAIN_PACKAGE)

# Clean artifacts
clean:
	rm -f $(BINARY_NAME)

#==============================
# Cross-Compilation Targets
#==============================

# Example: make build-linux
build-linux:
	GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME)-linux $(MAIN_PACKAGE)

build-mac:
	GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME)-mac $(MAIN_PACKAGE)

build-windows:
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME).exe $(MAIN_PACKAGE)

# Build all
build-all: build-linux build-mac build-windows