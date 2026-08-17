#!/usr/bin/env bash
# Stands up a real, vanilla, single-master/single-worker Apache Celeborn 0.7.0 cluster from the
# official binary distribution (no private fork, no patched build), runs this module's tests
# against it, and tears the cluster down on exit. Reproduces the same claim the module's own
# tests make: every CelebornExchangeStore behavior tested here (handleKind, serializeHandle/
# deserializeHandle, isFresh, checkIdentityIsolation, and reattach's documented
# UnsupportedOperationException) is proven against a real cluster, not a mock.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
VENDOR="$REPO_ROOT/.vendor"
CELEBORN_VERSION="0.7.0"
DIST_NAME="apache-celeborn-${CELEBORN_VERSION}-bin"
DIST_TGZ="$VENDOR/${DIST_NAME}.tgz"
DIST_DIR="$VENDOR/${DIST_NAME}"
WORK="$VENDOR/work"

mkdir -p "$VENDOR"

if [ ! -f "$DIST_TGZ" ]; then
  echo "== downloading Apache Celeborn ${CELEBORN_VERSION} (official release, ~430MB) =="
  curl -L --retry 5 --retry-delay 5 -o "$DIST_TGZ" \
    "https://downloads.apache.org/celeborn/celeborn-${CELEBORN_VERSION}/${DIST_NAME}.tgz"
fi

if [ ! -d "$DIST_DIR" ]; then
  echo "== extracting =="
  tar xzf "$DIST_TGZ" -C "$VENDOR"
fi

rm -rf "$WORK"
mkdir -p "$WORK/worker-data" "$WORK/conf" "$WORK/logs" "$WORK/pids"
sed "s#__WORKER_DATA_DIR__#$WORK/worker-data#" "$HERE/conf/celeborn-defaults.conf" > "$WORK/conf/celeborn-defaults.conf"
cp "$DIST_DIR/conf/log4j2.xml.template" "$WORK/conf/log4j2.xml"

export JAVA_HOME="${JAVA_HOME:-$HOME/.sdkman/candidates/java/17.0.11-tem}"
export PATH="$JAVA_HOME/bin:$PATH"
export CELEBORN_CONF_DIR="$WORK/conf"
export CELEBORN_HOME="$DIST_DIR"
export CELEBORN_LOG_DIR="$WORK/logs"
export CELEBORN_PID_DIR="$WORK/pids"

cleanup() {
  echo "== stopping Celeborn master/worker =="
  (cd "$CELEBORN_HOME" && ./sbin/stop-worker.sh || true)
  (cd "$CELEBORN_HOME" && ./sbin/stop-master.sh || true)
}
trap cleanup EXIT

echo "== starting Celeborn master =="
(cd "$CELEBORN_HOME" && ./sbin/start-master.sh)
sleep 5

echo "== starting Celeborn worker =="
(cd "$CELEBORN_HOME" && ./sbin/start-worker.sh)
sleep 5

echo "== confirming the master's admin REST API is reachable =="
for i in $(seq 1 15); do
  if curl -sf --max-time 3 http://localhost:9098/api/v1/applications >/dev/null 2>&1; then
    echo "master REST API is up"
    break
  fi
  if [ "$i" -eq 15 ]; then
    echo "FAILED: master REST API never came up -- see $WORK/logs" >&2
    exit 1
  fi
  sleep 2
done

echo "== running spark-resume-celeborn's tests =="
cd "$REPO_ROOT"
mvn -o -pl spark-resume-api,spark-resume-core,spark-resume-celeborn -am clean test
