OS:=$(shell uname -s | awk '{ print tolower($$1) }')
ARCH=amd64
TARGET:=kafka-timescaledb-adapter

.PHONY: all clean

SOURCES:=$(shell find . -name "*.go")

all: $(TARGET)

$(TARGET): $(SOURCES)
	GOOS=$(OS) GOARCH=${ARCH} go build -a --ldflags '-w -s' -o $@

clean:
	go clean
	rm -f *~ $(TARGET)
