
set -euo pipefail

NAMESPACE="shuffle"
WORKER_DEPLOYMENT="worker"
MASTER_DEPLOYMENT="master"
FAULT_COUNT=6
FAULT_INTERVAL=8
POST_WAIT_TIMEOUT=180

usage() {
  cat <<'EOF'
Usage: ./scripts/fault_simulation.sh [options]

Simulates worker failures by deleting random worker pods while a MapReduce job is running,
then verifies that reducers still produce output.

Options:
  -n, --namespace <name>          Kubernetes namespace (default: shuffle)
  -w, --worker-deploy <name>      Worker deployment name (default: worker)
  -m, --master-deploy <name>      Master deployment name (default: master)
  -c, --fault-count <num>         Number of pod deletions (default: 6)
  -i, --interval <seconds>        Seconds between faults (default: 8)
  -t, --timeout <seconds>         Post-fault completion wait timeout (default: 180)
  -h, --help                      Show this help message

Examples:
  ./scripts/fault_simulation.sh
  ./scripts/fault_simulation.sh --fault-count 10 --interval 5
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -n|--namespace)
        NAMESPACE="$2"
        shift 2
        ;;
      -w|--worker-deploy)
        WORKER_DEPLOYMENT="$2"
        shift 2
        ;;
      -m|--master-deploy)
        MASTER_DEPLOYMENT="$2"
        shift 2
        ;;
      -c|--fault-count)
        FAULT_COUNT="$2"
        shift 2
        ;;
      -i|--interval)
        FAULT_INTERVAL="$2"
        shift 2
        ;;
      -t|--timeout)
        POST_WAIT_TIMEOUT="$2"
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

jsonpath_worker_pod_names='{range .items[*]}{.metadata.name}{"\n"}{end}'

wait_for_deployments_ready() {
  echo "[1/5] Waiting for deployments to be ready"
  kubectl rollout status deployment/"$MASTER_DEPLOYMENT" -n "$NAMESPACE" --timeout=120s >/dev/null
  kubectl rollout status deployment/"$WORKER_DEPLOYMENT" -n "$NAMESPACE" --timeout=120s >/dev/null
}

expected_reducers() {
  local args arg

  args=$(kubectl get deployment "$MASTER_DEPLOYMENT" -n "$NAMESPACE" -o jsonpath='{range .spec.template.spec.containers[0].args[*]}{.}{"\n"}{end}')
  while IFS= read -r arg; do
    if [[ "$arg" == -num-machines=* ]]; then
      echo "${arg#*=}"
      return
    fi
  done <<< "$args"

  echo "4"
}

inject_faults() {
  echo "[2/5] Injecting $FAULT_COUNT worker pod failures"
  for ((i = 1; i <= FAULT_COUNT; i++)); do
    mapfile -t pods < <(kubectl get pods -n "$NAMESPACE" -l app=shuffle,role=worker -o jsonpath="$jsonpath_worker_pod_names")

    if [[ ${#pods[@]} -eq 0 ]]; then
      echo "No worker pods found; cannot continue fault injection" >&2
      exit 1
    fi

    victim_index=$((RANDOM % ${#pods[@]}))
    victim="${pods[$victim_index]}"

    echo "  - fault $i/$FAULT_COUNT: deleting worker pod $victim"
    kubectl delete pod "$victim" -n "$NAMESPACE" --wait=false >/dev/null

    if [[ "$i" -lt "$FAULT_COUNT" ]]; then
      sleep "$FAULT_INTERVAL"
    fi
  done
}

wait_for_worker_recovery() {
  echo "[3/5] Waiting for worker deployment recovery"
  kubectl rollout status deployment/"$WORKER_DEPLOYMENT" -n "$NAMESPACE" --timeout="${POST_WAIT_TIMEOUT}s" >/dev/null
}

wait_for_output_files() {
  local reducers master_pod start now elapsed

  reducers=$(expected_reducers)
  master_pod=$(kubectl get pod -n "$NAMESPACE" -l app=shuffle,role=master -o jsonpath='{.items[0].metadata.name}')

  echo "[4/5] Waiting for reducer outputs (expected: $reducers files)"
  start=$(date +%s)

  while true; do
    count=$(kubectl exec -n "$NAMESPACE" "$master_pod" -- sh -c 'set -e; ls /data/output/reducer-* 2>/dev/null | wc -l' | tr -d '[:space:]')
    if [[ "$count" =~ ^[0-9]+$ ]] && (( count >= reducers )); then
      echo "Reducer output check passed: found $count reducer files"
      break
    fi

    now=$(date +%s)
    elapsed=$((now - start))
    if (( elapsed > POST_WAIT_TIMEOUT )); then
      echo "Timed out waiting for reducer outputs (found: $count, expected: $reducers)" >&2
      return 1
    fi
    sleep 3
  done
}

print_diagnostics() {
  echo "[5/5] Diagnostics"
  echo "Recent master logs:"
  kubectl logs -n "$NAMESPACE" deployment/"$MASTER_DEPLOYMENT" --tail=40

  echo
  echo "Current worker pods:"
  kubectl get pods -n "$NAMESPACE" -l app=shuffle,role=worker -o wide
}

main() {
  parse_args "$@"
  require_cmd kubectl

  wait_for_deployments_ready
  inject_faults
  wait_for_worker_recovery
  wait_for_output_files
  print_diagnostics

  echo
  echo "Fault simulation complete: job continued through worker failures."
}

main "$@"
