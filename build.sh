#!/bin/bash
kind delete cluster
kind create cluster
docker build -t ghcr.io/kubeflow/trainer/trainer-controller-manager -f ./cmd/trainer-controller-manager/Dockerfile .
make generate
make manifests
kind load docker-image ghcr.io/kubeflow/trainer/trainer-controller-manager
kubectl apply --server-side -k ./manifests/overlays/manager
sleep 10
kubectl apply -f examples/hpc/flux/