.PHONY: all

all:
	make -C module

clean:
	make -C module clean

sample: sample/sample.go
	go build -o bin/sample sample/sample.go
