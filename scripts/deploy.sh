#!/usr/bin/env bash

set -euo pipefail

NAMESPACE="shuffle"
IMAGE="shuffle:dev"
RUN_FAULT_SIMULATION=true
FAULT_COUNT=6
FAULT_INTERVAL=8

usage() {
  cat <<'EOF'
Usage: ./deploy.sh [options]

Options:
  --no-fault-sim          Skip fault simulation after deployment
  --fault-count <num>     Number of worker pod failures to inject (default: 6)
  --fault-interval <sec>  Seconds between injected failures (default: 8)
  --fault-timeout <sec>   Reducer output wait timeout (default: 180)
  -h, --help              Show this help message
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --no-fault-sim)
        RUN_FAULT_SIMULATION=false
        shift
        ;;
      --fault-count)
        FAULT_COUNT="$2"
        shift 2
        ;;
      --fault-interval)
        FAULT_INTERVAL="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

echo "===== Starting Minikube ====="
minikube start

echo "===== Using Minikube Docker ====="
eval "$(minikube docker-env)"

echo "===== Building image ====="
docker build -t ${IMAGE} ..

echo "===== Creating shared directories in Minikube ====="
minikube ssh "sudo mkdir -p /tmp/shuffle-input /tmp/shuffle-output"

echo "===== Copying input files ====="
for f in ../sample_input/*; do
  minikube cp "$f" /tmp/shuffle-input/
done
echo "  -> $(ls ../sample_input/ | wc -l) files copied to /tmp/shuffle-input"

echo "===== Creating namespace ====="
kubectl apply -f ../infra/namespaces.yaml

echo "===== Deploying master ====="
kubectl apply -f ../infra/master.yaml

echo "===== Restarting master to pick latest image ====="
kubectl rollout restart deployment/master -n ${NAMESPACE}

echo "===== Waiting for master ====="
kubectl rollout status deployment/master -n ${NAMESPACE} --timeout=120s

echo "===== Deploying workers ====="
kubectl apply -f ../infra/worker.yaml

echo "===== Restarting workers to pick latest image ====="
kubectl rollout restart deployment/worker -n ${NAMESPACE}

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

if [[ "$RUN_FAULT_SIMULATION" == true ]]; then
  echo
  echo "===== Running fault simulation ====="
  ./fault_simulation.sh \
    --namespace "${NAMESPACE}" \
    --worker-deploy worker \
    --master-deploy master \
    --fault-count "${FAULT_COUNT}" \
    --interval "${FAULT_INTERVAL}"
else
  echo
  echo "===== Fault simulation skipped ====="
fi

echo
echo "Setup complete."
