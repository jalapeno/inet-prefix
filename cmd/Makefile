compile-ebgp-ext:
	CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -a -ldflags '-extldflags "-static"' -o ../bin/ebgp-ext ./ebgp-ext.go
