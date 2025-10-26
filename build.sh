#!/bin/bash
docker build -t ghcr.io/kubeflow/trainer/trainer-controller-manager -f ./cmd/trainer-controller-manager/Dockerfile .
make generate
make manifests
kubectl delete -k ./manifests/overlays/manager
kind load docker-image ghcr.io/kubeflow/trainer/trainer-controller-manager
kubectl apply --server-side -k ./manifests/overlays/manager
