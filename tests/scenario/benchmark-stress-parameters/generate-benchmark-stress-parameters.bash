#!/bin/bash
# Generate a parameter-matrix stress test for the NFS CSI driver.
#
# Matrix: blocksize(4k|128k) x compression(on|off) x deduplication(on|off)
#         x thin(on|off) = 16 StorageClasses (permissions fixed at 111111111)
# Layout: 16 SCs x 10 PVCs = 160 PVCs = 16 pods x 2 containers x 5 mounts
#
# Each PVC maps to SC index (n % 16), so every StorageClass gets exactly
# 10 PVCs and each container mounts 5 different variants.
#
# The backend pool exposes ~3.4 TiB. 160 shares x 20 GiB = 3.125 TiB keeps
# the full matrix (including thick reservations and fill data) under the
# pool ceiling. Override with PVC_SIZE (e.g. PVC_SIZE=20Gi ./generate).

set -euo pipefail

readonly CSIDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly OUT_DIR="${OUT_DIR:-$CSIDIR/examples/benchmark-stress-parameters}"
readonly NS="default"
readonly PODS=16
readonly CONTAINERS=2
readonly MOUNTS=5
readonly TOTAL=$((PODS * CONTAINERS * MOUNTS)) # 160
readonly SCS=16
readonly SIZE="${PVC_SIZE:-20Gi}"
readonly IMAGE="debian:13"

blocksizes=(4k 128k)
compressions=(on off)
deduplications=(on off)
readonly PERMISSIONS="111111111"
thins=(on off)

sc_name() {
	local b=$1 c=$2 d=$3 p=$4 t=$5
	echo "nfs-csi-ngxstorage-b${b}-c${c}-d${d}-p${p}-t${t}"
}

mkdir -p "$OUT_DIR"

# --- StorageClasses ----------------------------------------------------------
: > "$OUT_DIR/01-storageclasses.yaml"
for b in "${blocksizes[@]}"; do
	for c in "${compressions[@]}"; do
		for d in "${deduplications[@]}"; do
			for t in "${thins[@]}"; do
				cat >> "$OUT_DIR/01-storageclasses.yaml" <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: $(sc_name "$b" "$c" "$d" "$PERMISSIONS" "$t")
provisioner: nfs.csi.ngxstorage.com
parameters:
  blocksize: "$b"
  compression: "$c"
  deduplication: "$d"
  permissions: "$PERMISSIONS"
  thin_provision: "$t"
volumeBindingMode: Immediate
---
EOF
			done
		done
	done
done

# --- PVCs --------------------------------------------------------------------
: > "$OUT_DIR/02-pvcs.yaml"
pvc_index=0
for i in $(seq 0 $((PODS - 1))); do
	for j in $(seq 0 $((CONTAINERS - 1))); do
		for k in $(seq 0 $((MOUNTS - 1))); do
			name="stress-p${i}-c${j}-m${k}"
			idx=$((pvc_index % SCS))
			sc=$(sc_name "${blocksizes[$(((idx >> 0) & 1))]}" \
				"${compressions[$(((idx >> 1) & 1))]}" \
				"${deduplications[$(((idx >> 2) & 1))]}" \
				"$PERMISSIONS" \
				"${thins[$(((idx >> 3) & 1))]}")
			cat >> "$OUT_DIR/02-pvcs.yaml" <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $name
  namespace: $NS
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: $SIZE
  storageClassName: $sc
---
EOF
			pvc_index=$((pvc_index + 1))
		done
	done
done

# --- Pods --------------------------------------------------------------------
: > "$OUT_DIR/03-pods.yaml"
for i in $(seq 0 $((PODS - 1))); do
	pod="$OUT_DIR/03-pods.yaml"
	{
		echo "apiVersion: v1"
		echo "kind: Pod"
		echo "metadata:"
		echo "  name: stress-p${i}"
		echo "  namespace: $NS"
		echo "spec:"
		echo "  restartPolicy: OnFailure"
		echo "  containers:"
	} >> "$pod"
	for j in $(seq 0 $((CONTAINERS - 1))); do
		cat >> "$pod" <<EOF
  - name: app-${j}
    image: $IMAGE
    command:
      - /bin/sh
      - -c
      - |
        set -e
        for d in /data0 /data1 /data2 /data3 /data4; do
          echo "ok-$(hostname)-${i}-${j}" > "\$d/write-test.txt"
        done
        sleep 86400
    volumeMounts:
EOF
		for k in $(seq 0 $((MOUNTS - 1))); do
			cat >> "$pod" <<EOF
      - name: v${j}-${k}
        mountPath: /data${k}
EOF
		done
	done
	{
		echo "  volumes:"
	} >> "$pod"
	for j in $(seq 0 $((CONTAINERS - 1))); do
		for k in $(seq 0 $((MOUNTS - 1))); do
			cat >> "$pod" <<EOF
  - name: v${j}-${k}
    persistentVolumeClaim:
      claimName: stress-p${i}-c${j}-m${k}
EOF
		done
	done
	echo "---" >> "$pod"
done

echo "generated:"
echo "  storageclasses: $(grep -c '^kind: StorageClass' "$OUT_DIR/01-storageclasses.yaml")"
echo "  pvcs:           $(grep -c '^kind: PersistentVolumeClaim' "$OUT_DIR/02-pvcs.yaml")"
echo "  pods:           $(grep -c '^kind: Pod' "$OUT_DIR/03-pods.yaml")"
