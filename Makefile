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

# build-ffi-vendored is the ffi backend with no Rust toolchain at all, on
# linux only. iroh-go vendors a libiroh.a for linux/amd64 and
# linux/arm64, and cgo finds it without help; there is no darwin archive,
# which is why build-ffi below exists.
#
# The tradeoff is the iroh version. The vendored archive is built from
# iroh-go's own lockfile and is currently iroh 1.0.0, where ffi/libiroh
# pins 1.0.3. For trying the backend out that difference does not matter.
# For anything you intend to quote, it does, so this target prints what
# it linked rather than leaving you to assume.
build-ffi-vendored: ## linux only: ffi backend against iroh-go's vendored libiroh, no Rust
	@case "$$(go env GOOS)" in linux) ;; *) \
		echo "build-ffi-vendored is linux-only: iroh-go vendors no archive for $$(go env GOOS)"; \
		echo "  use make build-ffi, which builds one from ffi/libiroh"; \
		exit 1;; esac
	@rm -f $(K6_OUT)
	CGO_ENABLED=1 xk6 build $(K6_VERSION) --cgo=1 -o $(K6_OUT) \
		--with github.com/tmc/xk6-iroh=. \
		--with github.com/tmc/xk6-iroh/ffi=./ffi \
		$(if $(IROH_REPLACE),--replace github.com/tmc/go-iroh=$(IROH_REPLACE))
	@echo "linked iroh: $$(strings $(K6_OUT) | grep -oE 'iroh-1\.[0-9]+\.[0-9]+' | sort -u | tr '\n' ' ')"

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

# ffi depends on the parent by version, not by a replace, because a
# replace in a published module is ignored by whoever depends on it --
# the module would resolve to a version that does not exist. The cost is
# that ffi builds against the released parent even here, so a change to
# the parent is invisible to it until that change is tagged.
#
# make workspace is the way to see it anyway, and it is deliberately a
# thing you ask for: go.work is gitignored, so the substitution cannot
# travel with the repository or quietly outlive the work it was for.
workspace: ## go.work substituting this tree for the released parent (development only)
	go work init . ./ffi
	@echo "workspace created; ffi now builds against this tree. rm go.work to undo."

test: ## test both modules
	go test -race ./...
	go build -C ffi ./...

# A missing tool and a tool that found something must not look alike:
# `cmd && tool || echo skipped` reports "skipped" for both, and swallows
# the failure. Test for the tool first, then let it decide the exit
# status.
#
# Two xk6 lint checks stay disabled, for different reasons. Do not read
# them as a pair.
#
# vulnerability: deduplication. govulncheck below covers it and reports
# what it found rather than only that it found something.
#
# security: it reports a missing gosec as a failed check, which is the
# exact bug described above. The reason is that we cannot make xk6
# distinguish missing from failing, not that the check does not apply.
#
# readme, license, git and versions are NOT disabled here. They were, in
# the repository this was split out of, because they assume the extension
# directory is its own repository root -- which it now is.
lint: ## static checks; a real finding fails the target
	go vet ./...
	@gofmt -l . | grep -v '^ffi/libiroh/' && { echo "gofmt: files above need formatting"; exit 1; } || true
	@if command -v xk6 >/dev/null; then \
		xk6 lint --disable vulnerability,security .; \
	else echo "xk6 not installed; skipped"; fi
	@if command -v govulncheck >/dev/null; then \
		govulncheck ./...; \
	else echo "govulncheck not installed; skipped"; fi

.PHONY: help build build-ffi build-ffi-vendored workspace test lint
