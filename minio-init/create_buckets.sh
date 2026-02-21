#!/bin/sh
# ================================================================
# MinIO Bucket Initialiser
# Runs once via the minio-init service after MinIO is healthy.
# ================================================================

set -e

MC_ALIAS="local"
MINIO_URL="http://minio:9000"

echo "⏳  Waiting for MinIO to be ready..."
until mc alias set "$MC_ALIAS" "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
  echo "   MinIO not ready — retrying in 3 s..."
  sleep 3
done

echo "✅  MinIO connection established."

# ── Create buckets if they don't already exist ─────────────────
for BUCKET in "$MINIO_RAW_BUCKET" "$MINIO_PROCESSED_BUCKET"; do
  if mc ls "$MC_ALIAS/$BUCKET" > /dev/null 2>&1; then
    echo "   Bucket '$BUCKET' already exists — skipping."
  else
    mc mb "$MC_ALIAS/$BUCKET"
    echo "   ✅  Created bucket: $BUCKET"
  fi
done

# ── Apply anonymous read policy on raw-data (optional) ────────
mc anonymous set download "$MC_ALIAS/$MINIO_RAW_BUCKET"

echo "🎉  MinIO initialisation complete."
