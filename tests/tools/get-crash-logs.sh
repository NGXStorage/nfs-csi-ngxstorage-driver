#!/usr/bin/env bash

# get-crash-logs.sh collects NFS CSI driver log archives from every cluster
# node into a local destination directory. Use after a container crash or pod
# deletion, when oc logs --previous is no longer sufficient.
#
# Usage: ./tests/tools/get-crash-logs.sh [destination-dir]

set -u

DEST="${1:-./nfs-csi-log-bundle-$(date +%F)}"

if ! command -v oc >/dev/null 2>&1; then
	echo "error: oc not found in PATH" >&2
	echo "usage: ./tests/tools/get-crash-logs.sh [destination-dir]" >&2
	exit 1
fi

nodes=$(oc get nodes -o name 2>/dev/null)
if [ -z "$nodes" ]; then
	echo "error: no nodes returned by oc get nodes" >&2
	exit 1
fi

for node in $nodes; do
	name=${node#*/}
	name=${name%$'\r'}
	mkdir -p "$DEST/$name"

	files=$(timeout 300 oc debug "node/$name" -- chroot /host sh -c 'find /var/log/ngxstorage/nfs -maxdepth 1 -type f \( -name "*.log" -o -name "*.log.gz" \) -printf "%f\n" 2>/dev/null || true' 2>/dev/null)
	rc=$?
	if [ "$rc" -ne 0 ]; then
		echo "$name: unreachable (oc debug failed)"
		continue
	fi
	if [ -z "$files" ]; then
		echo "$name: no logs found"
		continue
	fi

	while read -r file; do
		timeout 300 oc debug "node/$name" -- chroot /host cat "/var/log/ngxstorage/nfs/$file" >"$DEST/$name/$file" 2>/dev/null
	done <<<"$files"
	echo "$name: $(wc -l <<<"$files") files copied"
done

echo "bundle written to $DEST"
