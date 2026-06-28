.DEFAULT_GOAL := help

## Shell config
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

## Tool Binaries
GO ?= go
KUBECTL ?= $(GO) tool k8s.io/kubernetes/cmd/kubectl
KIND ?= $(GO) tool sigs.k8s.io/kind
GOLANGCI_LINT ?= $(GO) tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint
KO ?= $(GO) tool github.com/google/ko
KUBEBUILDER ?= $(GO) tool sigs.k8s.io/kubebuilder/v4
CONTROLLER_GEN ?= $(GO) tool sigs.k8s.io/controller-tools/cmd/controller-gen
CLIENT_GEN ?= $(GO) tool k8s.io/code-generator/cmd/client-gen
LISTER_GEN ?= $(GO) tool k8s.io/code-generator/cmd/lister-gen
INFORMER_GEN ?= $(GO) tool k8s.io/code-generator/cmd/informer-gen
HELM ?= $(GO) tool helm.sh/helm/v4/cmd/helm

MODULE := $(shell $(GO) list -m)

# KIND
KIND_CLUSTER_NAME = "kind-scheduler-test"

CMD := $(CURDIR)/cmd/scheduler

## Location for build artifacts
BUILD_DIR := $(CURDIR)/build

# Binary
LOCALBIN_DIR := $(BUILD_DIR)/bin

# Image
IMG_DIR := $(BUILD_DIR)/image
IMG_TAR_FILE := $(IMG_DIR)/scheduler.tar

# Helm
CHART_DIR := $(CURDIR)/charts/kaschnit-scheduler

$(BUILD_DIR):
	mkdir -p "$(BUILD_DIR)"

$(LOCALBIN_DIR):
	mkdir -p "$(LOCALBIN_DIR)"

$(IMG_DIR):
	mkdir -p "$(IMG_DIR)"

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: clean
clean: ## Clean up files.
	find . -name .DS_Store -type f -delete
	rm -rf $(BUILD_DIR)

##@ Development

.PHONY: generate
generate: generate-code generate-manifests ## Generate files.

.PHONY: generate-manifests
generate-manifests: ## Generate Kubernetes manifests.
	$(CONTROLLER_GEN) paths=./apis/... crd:crdVersions=v1 output:crd:artifacts:config=$(CHART_DIR)/templates

.PHONY: generate-code
generate-code: controller-gen-objects generate-k8s-clients ## Generate code.

.PHONY: controller-gen-objects
controller-gen-objects:
	$(CONTROLLER_GEN) object paths=./apis/...

.PHONY: generate-k8s-clients
generate-k8s-clients: k8s-client-gen k8s-lister-gen k8s-informer-gen

.PHONY: k8s-client-gen
k8s-client-gen: controller-gen-objects
	$(CLIENT_GEN) \
		--clientset-name "scheduling" \
		--input-base $(MODULE)/apis \
		--input scheduling/v1 \
		--output-dir ./client/clientset \
		--output-pkg $(MODULE)/client/clientset

.PHONY: k8s-lister-gen
k8s-lister-gen: controller-gen-objects
	$(LISTER_GEN) \
		--output-dir ./client/listers \
		--output-pkg $(MODULE)/client/listers \
		./apis/scheduling/v1

.PHONY: k8s-informer-gen
k8s-informer-gen: controller-gen-objects k8s-client-gen k8s-lister-gen
	$(INFORMER_GEN) \
        --versioned-clientset-package $(MODULE)/client/clientset/scheduling \
        --listers-package $(MODULE)/client/listers \
        --output-dir ./client/informers \
        --output-pkg $(MODULE)/client/informers \
        ./apis/scheduling/v1

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
build: generate $(LOCALBIN_DIR) ## Build manager binary.
	$(GO) build -o $(LOCALBIN_DIR)/scheduler $(CMD)

.PHONY: run
run: generate ## Run the scheduler.
	$(GO) run $(CMD)

.PHONY: image
image: PUSH := false
image: generate $(IMG_DIR) ## Build an image and optionally push it.
	KO_DOCKER_REPO=kaschnit-scheduler \
		$(KO) build \
			--push=$(PUSH) \
			--platform=linux/$(shell $(GO) env GOARCH) \
			--tarball=$(IMG_TAR_FILE) \
			--bare \
			--tags=development \
			$(CMD)

.PHONY: kind-delete
kind-delete: ## Delete the KIND testing cluster.
	$(KIND) delete cluster --name "$(KIND_CLUSTER_NAME)"

.PHONY: kind-create
kind-create: kind-delete ## Create a KIND cluster for testing.
	$(KIND) create cluster --name "$(KIND_CLUSTER_NAME)"

.PHONY: kind-deploy
kind-deploy: generate image kind-create ## Deploy the scheduler to a KIND cluster for testing.
	$(KIND) load image-archive $(IMG_TAR_FILE) --name "$(KIND_CLUSTER_NAME)"
	$(HELM) install kaschnit-scheduler $(CHART_DIR) \
		--values test/kind/values.yaml \
		--namespace kaschnit-scheduler \
		--create-namespace
	$(KUBECTL) apply -f test/kind/base
