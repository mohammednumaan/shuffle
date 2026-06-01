#!/usr/bin/env bash

set -euo pipefail

NAMESPACE="shuffle"
IMAGE="shuffle:dev"

echo "===== Starting Minikube ====="
minikube start

echo "===== Using Minikube Docker ====="
eval "$(minikube docker-env)"

echo "===== Building image ====="
docker build -t ${IMAGE} .

echo "===== Creating shared directories in Minikube ====="
minikube ssh "sudo mkdir -p /tmp/shuffle-input /tmp/shuffle-output"

echo "===== Copying input files ====="
for f in sample_input/*; do
  minikube cp "$f" /tmp/shuffle-input/
done
echo "  -> $(ls sample_input/ | wc -l) files copied to /tmp/shuffle-input"

echo "===== Creating namespace ====="
kubectl apply -f infra/namespaces.yaml

echo "===== Deploying master ====="
kubectl apply -f infra/master.yaml

echo "===== Waiting for master ====="
kubectl rollout status deployment/master -n ${NAMESPACE} --timeout=120s

echo "===== Deploying workers ====="
kubectl apply -f infra/worker.yaml

echo "===== Waiting for workers ====="
kubectl rollout status deployment/worker -n ${NAMESPACE} --timeout=120s

echo
echo "===== Pods ====="
kubectl get pods -n ${NAMESPACE} -o wide

echo
echo "===== Services ====="
kubectl get svc -n ${NAMESPACE}

echo
echo "===== Verifying shared input ====="
MASTER_POD=$(kubectl get pod -n ${NAMESPACE} -l role=master -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n ${NAMESPACE} "$MASTER_POD" -- ls -lah /data/input

echo
echo "===== Master Logs ====="
kubectl logs -n ${NAMESPACE} deployment/master --tail=50

echo
echo "Setup complete."
