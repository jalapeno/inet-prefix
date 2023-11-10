REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all ebgp-ext container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: ebgp-ext

ebgp-ext:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-ebgp-ext

ebgp-ext-container: ebgp-ext
	docker build -t $(REGISTRY_NAME)/ebgp-ext:$(IMAGE_VERSION) -f ./build/Dockerfile.ebgp-ext .

push: ebgp-ext-container
	docker push $(REGISTRY_NAME)/ebgp-ext:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
