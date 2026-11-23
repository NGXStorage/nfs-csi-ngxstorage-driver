#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRATCH_ROOT="$HOME/.cache/ngxstorage"
case "$SCRATCH_ROOT" in /tmp|/tmp/*|/var/tmp|/var/tmp/*) echo "refusing temporary cache under /tmp or /var/tmp" >&2; exit 2 ;; esac
mkdir -p "$SCRATCH_ROOT"
VER="${1:?usage: ALLOW_HOST_RESET=1 STORAGE_API_KEY=... STORAGE_IPS=... POOL_NAME=... STORAGE_DATA_IP=... DRIVER_MANIFEST_DIR=... $0 <version>}"
LOG="$SCRATCH_ROOT/k8s-versions.log"
if [[ "${ALLOW_HOST_RESET:-}" != 1 ]]; then
  echo "refusing: this test resets kubeadm and deletes host Kubernetes state; use only an isolated disposable VM with ALLOW_HOST_RESET=1" >&2
  exit 2
fi
if [[ -z "${STORAGE_API_KEY:-}" ]]; then
  echo "STORAGE_API_KEY must be supplied in the environment" >&2
  exit 2
fi
if [[ -z "${STORAGE_IPS:-}" || -z "${POOL_NAME:-}" || -z "${STORAGE_DATA_IP:-}" ]]; then
  echo "STORAGE_IPS, POOL_NAME and STORAGE_DATA_IP must be supplied in the environment (no lab defaults are baked into this script)" >&2
  exit 2
fi
DRIVER_MANIFEST_DIR="${DRIVER_MANIFEST_DIR:?DRIVER_MANIFEST_DIR must point to reviewed driver manifests}"
[[ -d "$DRIVER_MANIFEST_DIR" ]] || { echo "driver manifest directory not found: $DRIVER_MANIFEST_DIR" >&2; exit 2; }
echo "===== TEST K8S v$VER =====" >> $LOG
date >> $LOG

# 1. reset
sudo kubeadm reset -f >/dev/null 2>&1
sudo rm -rf /etc/cni/net.d /var/lib/kubelet/* /var/lib/etcd/*
sudo systemctl restart containerd

# 2. install version
sudo apt-mark unhold kubeadm kubectl kubelet 2>/dev/null
sudo apt-get install -y --allow-downgrades --allow-change-held-packages \
  kubeadm=$VER.* kubectl=$VER.* kubelet=$VER.* >> $LOG 2>&1
sudo systemctl restart containerd kubelet
echo "installed: $(sudo kubeadm version --short)" >> $LOG

# 3. init
sudo kubeadm init --pod-network-cidr=10.244.0.0/16 >> $LOG 2>&1
mkdir -p $HOME/.kube
sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config
# k8s <=1.24 uses node-role.kubernetes.io/master; newer uses control-plane.
# Remove both so test pods can schedule on the single control-plane node.
kubectl taint node master node-role.kubernetes.io/control-plane:NoSchedule- 2>/dev/null || true
kubectl taint node master node-role.kubernetes.io/master:NoSchedule- 2>/dev/null || true
kubectl get node master -o jsonpath='{.spec.taints}' 2>/dev/null || true
echo "" 

# 4. CNI
kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml >> $LOG 2>&1
sleep 20
kubectl wait --for=condition=Ready node master --timeout=180s >> $LOG 2>&1
kubectl get nodes >> $LOG 2>&1

# 5. deploy driver
kubectl delete ns nfs-csi-ngxstorage --ignore-not-found >/dev/null 2>&1
sleep 5
kubectl apply -f "$DRIVER_MANIFEST_DIR/nfs-csi-ngxstorage-rbac.yaml" >> "$LOG" 2>&1
kubectl apply -f "$DRIVER_MANIFEST_DIR/nfs-csi-ngxstorage-class.yaml" >> "$LOG" 2>&1
kubectl apply -f "$DRIVER_MANIFEST_DIR/nfs-csi-ngxstorage-controller.yaml" >> "$LOG" 2>&1
kubectl apply -f "$DRIVER_MANIFEST_DIR/nfs-csi-ngxstorage-node.yaml" >> "$LOG" 2>&1
umask 077
SECRET_ENV_FILE="$(mktemp "$SCRATCH_ROOT/nfs-csi-config.XXXXXX")"
chmod 600 "$SECRET_ENV_FILE"
printf 'storage-ips=%s\npool-name=%s\napi-key=%s\ndata-ip=%s\n' \
  "$STORAGE_IPS" "$POOL_NAME" "$STORAGE_API_KEY" "$STORAGE_DATA_IP" > "$SECRET_ENV_FILE"
trap 'rm -f "$SECRET_ENV_FILE"' EXIT
kubectl create secret generic nfs-csi-ngxstorage-config -n nfs-csi-ngxstorage \
  --from-env-file="$SECRET_ENV_FILE" >/dev/null 2>&1
rm -f "$SECRET_ENV_FILE"
trap - EXIT

# 6. test PVC bind
sleep 25
cat > "$SCRATCH_ROOT/tpvc.yaml" << EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: test-sc}
provisioner: nfs.csi.ngxstorage.com
parameters: {blocksize: "128k", compression: "on", thin_provision: "on", permissions: "111111111"}
volumeBindingMode: Immediate
reclaimPolicy: Delete
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: test-pvc}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}, storageClassName: test-sc}
EOF
kubectl apply -f "$SCRATCH_ROOT/tpvc.yaml" >/dev/null 2>&1
sleep 5
BIND=$(kubectl wait --for=jsonpath={.status.phase}=Bound pvc/test-pvc --timeout=90s 2>&1 | tail -1)
echo "BIND v$VER: $BIND" >> $LOG
if echo "$BIND" | grep -q "condition met"; then
  # 7. dd write test
  cat > "$SCRATCH_ROOT/ddtest.yaml" << EOF
apiVersion: v1
kind: Pod
metadata: {name: ddtest}
spec:
  containers:
  - name: dd
    image: busybox
    command: ["/bin/sh","-c","dd if=/dev/urandom of=/data/test.bin bs=1M count=100 2>&1; sync; echo DD_DONE; ls -l /data/test.bin"]
    volumeMounts: [{name: data, mountPath: /data}]
  volumes:
  - name: data
    persistentVolumeClaim: {claimName: test-pvc}
  restartPolicy: Never
EOF
  kubectl apply -f "$SCRATCH_ROOT/ddtest.yaml" >/dev/null 2>&1
  # wait for pod to reach Running or Succeeded (dd is fast)
  for i in $(seq 1 30); do
    PHASE=$(kubectl get pod ddtest -o jsonpath='{.status.phase}' 2>/dev/null)
    if [ "$PHASE" = "Running" ] || [ "$PHASE" = "Succeeded" ]; then break; fi
    sleep 3
  done
  # wait for completion then capture logs
  sleep 20
  DDLOG=$(kubectl logs ddtest 2>&1 | tail -8)
  echo "DD v$VER: $DDLOG" >> $LOG
  echo "DD v$VER: $DDLOG"
  # capacity check via storage API
  VOLHANDLE=$(kubectl get pv -o jsonpath='{.items[0].spec.csi.volumeHandle}' 2>/dev/null)
  CAP="$($SCRIPT_DIR/capacity-check.sh "$VOLHANDLE")"
  echo "CAP v$VER: $CAP" >> $LOG
  echo "CAP v$VER: $CAP"
  kubectl delete pod ddtest --force --grace-period=0 >/dev/null 2>&1
fi
echo "===== END v$VER =====" >> $LOG
