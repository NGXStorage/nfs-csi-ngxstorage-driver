#!/usr/bin/env bash
source "$CSI_TEST_ROOT/lib.sh"
set -euo pipefail

oc apply -f benchmark-kbench-namespace.yaml
oc apply -f benchmark-kbench-serviceaccount.yaml
oc -n benchmark-kbench-nfs delete pod benchmark-kbench-nfs --ignore-not-found --wait=false
oc apply -f benchmark-kbench-sc.yaml -f benchmark-kbench-pvc.yaml -f benchmark-kbench-pod.yaml
oc wait --for=jsonpath='{.status.phase}'=Bound pvc/benchmark-kbench-nfs-pvc -n benchmark-kbench-nfs --timeout=10m
oc wait --for=jsonpath='{.status.phase}'=Succeeded pod/benchmark-kbench-nfs -n benchmark-kbench-nfs --timeout="${KBENCH_TIMEOUT:-1200}s"
oc logs pod/benchmark-kbench-nfs -n benchmark-kbench-nfs
oc describe pod/benchmark-kbench-nfs -n benchmark-kbench-nfs | tail -40
