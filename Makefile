K6_VERSION ?= v2.2.0
K6_OUT ?= ./k6
LIBIROH = $(CURDIR)/ffi/libiroh

# IROH_REPLACE points the pure-Go backend at a local go-iroh checkout:
#   make build IROH_REPLACE=../go-iroh
IROH_REPLACE ?=

help:
	@awk 'BEGIN{FS=":.*## "} /^[a-z][a-z0-9-]*:.*## /{printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# The rm is not tidiness. xk6 writes the new binary over the old one in
# place, and on macOS that invalidates the signature of a file that may
# still be mapped: the result builds cleanly, reports success, and is
# killed on exec with "Killed: 9". Removing it first costs nothing.
build: ## k6 with the pure-Go backend
	@rm -f $(K6_OUT)
	xk6 build $(K6_VERSION) -o $(K6_OUT) \
		--with github.com/tmc/xk6-iroh=. \
		$(if $(IROH_REPLACE),--replace github.com/tmc/go-iroh=$(IROH_REPLACE))

# build-ffi links a k6 carrying both backends, so a script can pick the
# implementation at runtime with impl: 'ffi'.
#
# CGO_LDFLAGS puts the locally built archive ahead of the one iroh-go
# vendors, which is built from upstream's lockfile and pins an older
# iroh. The verify step then fails the build if the wrong one got linked,
# because the binary and not the lockfile decides which iroh is present:
# a lockfile records an intention, and this is the only place the
# intention is checked against the artifact.
build-ffi: ## k6 with both backends, so a script can pick impl at runtime
	cd $(LIBIROH) && cargo build --release --locked --lib
	@rm -f $(K6_OUT)
	CGO_ENABLED=1 CGO_LDFLAGS="-L$(LIBIROH)/target/release" \
		xk6 build $(K6_VERSION) --cgo=1 -o $(K6_OUT) \
		--with github.com/tmc/xk6-iroh=. \
		--with github.com/tmc/xk6-iroh/ffi=./ffi \
		$(if $(IROH_REPLACE),--replace github.com/tmc/go-iroh=$(IROH_REPLACE))
	@want=$$(awk '/^name = "iroh"$$/{getline; gsub(/version = |"/,""); print; exit}' $(LIBIROH)/Cargo.lock); \
	got=$$(strings $(K6_OUT) | grep -oE "iroh-1\.[0-9]+\.[0-9]+" | sort -u | tr '\n' ' '); \
	case "$$got" in \
		"iroh-$$want ") echo "k6: linked iroh $$want";; \
		*) echo "k6: want iroh-$$want, linked: $$got"; exit 1;; \
	esac

test: ## test both modules
	go test -race ./...
	go build -C ffi ./...

vet:
	go vet ./...
	gofmt -l . | grep -v '^ffi/libiroh/' && exit 1 || true

.PHONY: help build build-ffi test vet
