BIN_NAME=csv-data-collector

build:
	@echo "Building project"
	go build -ldflags='-s -w' -o ${BIN_NAME} cmd/*.go 
	@echo "Build completed!"
