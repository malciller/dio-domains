#!/usr/bin/env bash
set -euo pipefail

################################################################################
# CONFIG — adjust if you rename things
################################################################################
LOCAL_SRC_DIR="/Users/malciller/dev/dio-domains/dio"
REMOTE_USER="malciller"
REMOTE_HOST="ann100db12"
REMOTE_PORT="2222"

REMOTE_BASE="/home/malciller"
REMOTE_SRC_DIR="$REMOTE_BASE/dio-src"
DOCKER_IMAGE="dio"
CONTAINER_NAME="dio_engine"
PORT_MAPPING="5001:5001"


echo "Starting deployment …"

###############################################################################
# 1. Sync sources to remote host
###############################################################################
echo "Rsyncing source tree to $REMOTE_HOST …"
rsync -az --delete -e "ssh -p $REMOTE_PORT" \
  "$LOCAL_SRC_DIR"/ \
  "${REMOTE_USER}@${REMOTE_HOST}:$REMOTE_SRC_DIR/"

###############################################################################
# 2. SSH and buildx / run on the server
###############################################################################
ssh -p "$REMOTE_PORT" "${REMOTE_USER}@${REMOTE_HOST}" bash <<EOF
set -euo pipefail
echo "Building Docker image on server …"
cd "$REMOTE_SRC_DIR"
docker buildx build --platform linux/amd64 -t $DOCKER_IMAGE --load .

echo "Stopping old container (if any) …"
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm   $CONTAINER_NAME 2>/dev/null || true

echo "Starting new container with production optimizations …"
docker run -it -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  --network host \
  -p "127.0.0.1:${PORT_MAPPING}" \
  --log-opt max-size=10m --log-opt max-file=3 \
  --security-opt no-new-privileges:true \
  -e TERM=xterm-256color \
  "$DOCKER_IMAGE" dio --dashboard

echo "Verifying container is running …"
if docker ps | grep -q "$CONTAINER_NAME"; then
  echo "Container is running successfully. Cleaning up source files …"
  cd /
  rm -rf "$REMOTE_SRC_DIR"
  echo "Source files removed from server."
else
  echo "ERROR: Container failed to start. Source files preserved for debugging."
  exit 1
fi

echo "Deployment finished successfully on \$(hostname -s)"
EOF
