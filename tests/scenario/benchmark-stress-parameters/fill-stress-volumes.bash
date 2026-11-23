#!/bin/bash
# Fill every benchmark-stress-parameters volume with data using varying dd block sizes.
#
# The matrix has 16 pods x 2 containers x 5 mounts = 160 volumes of
# PVC_SIZE (default 20Gi, see generate-benchmark-stress-parameters.bash). Each volume is
# filled with FILL_GIB GiB of zeros, rotating the dd block size through
# 4k / 64k / 128k / 1M so every block size is exercised against the
# backend. Uses conv=fsync so the write hits the NFS server before the
# script reports success.
#
# Total physical usage after a run: thick shares reserve PVC_SIZE up front
# (80 x 20Gi) plus thin shares consume FILL_GIB each (80 x 15Gi) ~ 2.8 TiB,
# which stays under the ~3.4 TiB pool ceiling.

set -euo pipefail

readonly NS="${NS:-default}"
readonly PODS=16
readonly CONTAINERS=2
readonly MOUNTS=5
readonly FILL_GIB="${FILL_GIB:-1}"
readonly BS_LIST=(4k 64k 128k 1M)

block_bytes() {
	case "$1" in
	4k) echo 4096 ;;
	64k) echo 65536 ;;
	128k) echo 131072 ;;
	1M) echo 1048576 ;;
	*) echo "unknown block size: $1" >&2 && exit 1 ;;
	esac
}

total_volumes=$((PODS * CONTAINERS * MOUNTS))
count_per_volume=$((FILL_GIB * 1024 * 1024 * 1024))

fill_pod() {
	local i=$1
	for j in $(seq 0 $((CONTAINERS - 1))); do
		for k in $(seq 0 $((MOUNTS - 1))); do
			# fill_pod runs in a background subshell. Derive a globally stable
			# index instead of incrementing a subshell-local counter so progress
			# and block-size rotation cover all 160 distinct mounts accurately.
			index=$((i * CONTAINERS * MOUNTS + j * MOUNTS + k))
			bs="${BS_LIST[$((index % ${#BS_LIST[@]}))]}"
			count=$((count_per_volume / $(block_bytes "$bs")))
			echo "[$((index + 1))/${total_volumes}] pod=stress-p${i} container=app-${j} mount=/data${k} bs=${bs} count=${count}"
			oc exec -n "$NS" "stress-p${i}" -c "app-${j}" -- \
				dd if=/dev/zero of="/data${k}/fill.bin" bs="$bs" count="$count" conv=fsync status=none
		done
	done
}

for i in $(seq 0 $((PODS - 1))); do
	fill_pod "$i" &
done
wait

echo "done: ${total_volumes} volumes filled with ${FILL_GIB}GiB each"
