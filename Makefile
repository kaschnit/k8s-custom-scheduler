.DEFAULT_GOAL := help

## Shell config
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

## Tool Binaries
GO ?= go
KUBECTL ?= $(GO) tool k8s.io/kubernetes/cmd/kubectl
KIND ?= $(GO) tool sigs.k8s.io/kind
KUSTOMIZE ?= $(GO) tool sigs.k8s.io/kustomize/kustomize/v5
DEEPCOPY_GEN ?= $(GO) tool k8s.io/code-generator/cmd/deepcopy-gen
GOLANGCI_LINT ?= $(GO) tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint
KO ?= $(GO) tool github.com/google/ko
KUBEBUILDER ?= $(GO) tool sigs.k8s.io/kubebuilder/v4

CMD := $(CURDIR)/cmd
CRD_BASES := $(CURDIR)/config/crd/bases

## Location for build artifacts
BUILD_DIR := $(CURDIR)/build
$(BUILD_DIR):
	mkdir -p "$(BUILD_DIR)"

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: clean
clean: ## Clean up files.
	find . -name .DS_Store -type f -delete
	find . -name zz_generated.*.go -type f -delete
	rm -rf $(CRD_BASES)
	rm -rf $(BUILD_DIR)

##@ Development

.PHONY: generate
generate: generate-deepcopy ## Generate code.

generate-deepcopy: ## Generate k8s DeepCopy code.
	$(DEEPCOPY_GEN) --output-file zz_generated.deepcopy.go ./...

.PHONY: go-tidy
go-tidy: generate ## Tidy go.mod and go.sum.
	$(GO) mod tidy

.PHONY: go-tidy-check
go-tidy-check: generate ## Check if go.mod and go.sum are tidy.
	$(GO) mod tidy --diff

.PHONY: go-mod-download
go-mod-download: generate ## Download dependencies from go.mod and go.sum.
	$(GO) mod download

.PHONY: install-deps
install-deps: go-mod-download ## Install dependencies.

.PHONY: lint
lint: generate ## Run linters.
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: generate ## Run linters and perform fixes.
	$(GOLANGCI_LINT) run --fix

.PHONY: test
test: TESTFLAGS := -v -race
test: TESTTARGET := ./...
test: generate ## Run unit tests.
	$(GO) test $(TESTFLAGS) $(TESTTARGET)

##@ Build

.PHONY: build
build: generate $(BUILD_DIR) ## Build manager binary.
	$(GO) build -o $(BUILD_DIR)/manager $(CMD)

.PHONY: run
run: generate ## Run a controller from your host.
	$(GO) run $(CMD)

.PHONY: image
image: PUSH := false
image: generate $(BUILD_DIR) ## Build an image and optionally push it.
	$(KO) build $(CMD) --push=$(PUSH) --platform=linux/$(shell $(GO) env GOARCH) --tarball=$(BUILD_DIR)/image.tar
