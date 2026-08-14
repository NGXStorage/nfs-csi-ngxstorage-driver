#!/usr/bin/env bash
# publish-helm-repo.sh
#
# Package the Helm chart and generate the repository index.
# Run this in CI on every release tag push.
#
# Usage:
#   GITHUB_PAGES_URL=https://ngxstorage.github.io/nfs-csi-ngxstorage-driver \
#     bash deploy/helm/scripts/publish-helm-repo.sh
#
# Output: deploy/helm/repo/index.yaml + deploy/helm/repo/*.tgz
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART_DIR="${REPO_ROOT}/deploy/helm/chart/nfs-csi-ngxstorage"
HELM_REPO_DIR="${REPO_ROOT}/deploy/helm/repo"
PAGES_URL="${GITHUB_PAGES_URL:-https://ngxstorage.github.io/nfs-csi-ngxstorage-driver}"

mkdir -p "${HELM_REPO_DIR}"

echo "==> Packaging chart from ${CHART_DIR}"
helm package "${CHART_DIR}" --destination "${HELM_REPO_DIR}"

echo "==> Building Helm repository index at ${HELM_REPO_DIR}"
helm repo index "${HELM_REPO_DIR}" --url "${PAGES_URL}"

echo ""
echo "Helm repository ready:"
ls -la "${HELM_REPO_DIR}"
echo ""
echo "Test with:"
echo "  helm repo add nfs-csi-ngxstorage ${PAGES_URL}"
echo "  helm repo update"
echo "  helm search repo nfs-csi-ngxstorage"
