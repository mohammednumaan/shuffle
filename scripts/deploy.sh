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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

pre_checks() {
  local fail=false

  if ! command -v docker &>/dev/null; then
    echo "MISSING: docker is not installed"
    fail=true
  fi

  if ! command -v minikube &>/dev/null; then
    echo "MISSING: minikube is not installed"
    fail=true
  fi

  if ! command -v kubectl &>/dev/null; then
    echo "MISSING: kubectl is not installed"
    fail=true
  fi

  if [ ! -d "$PROJECT_ROOT/sample_input" ] || [ -z "$(ls -A "$PROJECT_ROOT/sample_input" 2>/dev/null)" ]; then
    echo "MISSING: sample_input/ is empty or missing — run scripts/download_gutenberg.py first"
    fail=true
  fi

  if [ ! -f "$PROJECT_ROOT/infra/namespaces.yaml" ] || \
     [ ! -f "$PROJECT_ROOT/infra/master.yaml" ] || \
     [ ! -f "$PROJECT_ROOT/infra/worker.yaml" ]; then
    echo "MISSING: infra manifests (namespaces.yaml, master.yaml, worker.yaml) not found in infra/"
    fail=true
  fi

  if [ ! -f "$PROJECT_ROOT/Dockerfile" ]; then
    echo "MISSING: Dockerfile not found at project root"
    fail=true
  fi

  if [ ! -f "$PROJECT_ROOT/go.mod" ]; then
    echo "MISSING: go.mod not found — is this the right project root?"
    fail=true
  fi

  if [ "$RUN_FAULT_SIMULATION" = true ] && [ ! -f "$SCRIPT_DIR/fault_simulation.sh" ]; then
    echo "MISSING: fault_simulation.sh not found in scripts/ (required when --no-fault-sim is not set)"
    fail=true
  fi

  if $fail; then
    echo ""
    echo "Pre-checks failed. Fix the issues above and re-run."
    exit 1
  fi
}

echo "===== Running pre-checks ====="
pre_checks
echo "  -> all checks passed"

echo "===== Starting Minikube ====="
minikube start

echo "===== Using Minikube Docker ====="
eval "$(minikube docker-env)"

echo "===== Building image from $PROJECT_ROOT ====="
docker build -t ${IMAGE} "$PROJECT_ROOT"

echo "===== Creating shared directories in Minikube ====="
minikube ssh "sudo mkdir -p /tmp/shuffle-input /tmp/shuffle-output"

echo "===== Copying input files ====="
for f in "$PROJECT_ROOT/sample_input"/*; do
  minikube cp "$f" /tmp/shuffle-input/
done
echo "  -> $(ls "$PROJECT_ROOT/sample_input/" | wc -l) files copied to /tmp/shuffle-input"

echo "===== Creating namespace ====="
kubectl apply -f "$PROJECT_ROOT/infra/namespaces.yaml"

echo "===== Deploying master ====="
kubectl apply -f "$PROJECT_ROOT/infra/master.yaml"

echo "===== Restarting master to pick latest image ====="
kubectl rollout restart deployment/master -n ${NAMESPACE}

echo "===== Waiting for master ====="
kubectl rollout status deployment/master -n ${NAMESPACE} --timeout=120s

echo "===== Deploying workers ====="
kubectl apply -f "$PROJECT_ROOT/infra/worker.yaml"

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
  "$SCRIPT_DIR/fault_simulation.sh" \
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
