#!/usr/bin/env bash

### TODO:
### Convert to Makefile so we can use `$(GO) tool kind`` and `$(GO) tool kubectl` and `$(GO) tool helm for consistency`.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

NAME="scheduler-demo"

# Recreate cluster
kind delete cluster --name "${NAME}"
kind create cluster --config "${SCRIPT_DIR}/cluster.yaml" --name "${NAME}"

# Install KWOK
helm repo add kwok https://kwok.sigs.k8s.io/charts
helm repo update
helm install kwok kwok/kwok --namespace kube-system

# Create namespace and PC
kubectl apply -f "${SCRIPT_DIR}/namespace.yaml"
kubectl apply -f "${SCRIPT_DIR}/pc.yaml"

# Load custom scheduler image
kind load image-archive "${SCRIPT_DIR}/../../build/scheduler-image.tar" --name "${NAME}"

# Deploy custom scheduler
kubectl apply -k "${SCRIPT_DIR}/scheduler"
