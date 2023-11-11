REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all inet-prefix container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: inet-prefix

inet-prefix:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-inet-prefix

inet-prefix-container: inet-prefix
	docker build -t $(REGISTRY_NAME)/inet-prefix:$(IMAGE_VERSION) -f ./build/Dockerfile.inet-prefix .

push: inet-prefix-container
	docker push $(REGISTRY_NAME)/inet-prefix:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
