#!/usr/bin/env bash
# Runs this module's cross-process, cross-backend composition proof: ProcessA (a real Spark
# shuffle query, a real Celeborn shuffle registration, a real anchor written to Redis) as ONE
# `mvn test` invocation/JVM, then ProcessB (a fresh SparkSession + fresh JVM, reading that same
# anchor back, running it through the real AdmissionEngine, then SafeReattach.attempt against the
# real Celeborn cluster) as a SEPARATE `mvn test` invocation/JVM -- two real OS processes, not two
# specs sharing one JVM (see ProcessASpec/ProcessBSpec's doc comments for why that split matters).
#
# Needs a real Redis and a real Celeborn cluster already reachable -- this script does NOT stand
# either up itself (unlike spark-resume-celeborn/run-celeborn-tests.sh): both are already-proven,
# already-documented pieces of infrastructure (see spark-resume-redis/README.md and
# spark-resume-celeborn/run-celeborn-tests.sh), and this module's whole point is proving
# composition ACROSS already-real backends, not standing up a third variant of the same clusters.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"

export REDIS_HOST="${REDIS_HOST:-localhost}"
export REDIS_PORT="${REDIS_PORT:-6379}"
export CELEBORN_MASTER_REST="${CELEBORN_MASTER_REST:-http://localhost:9098}"
export CELEBORN_MASTER_ENDPOINT="${CELEBORN_MASTER_ENDPOINT:-localhost:9097}"
export JAVA_HOME="${JAVA_HOME:-$HOME/.sdkman/candidates/java/17.0.11-tem}"
export PATH="$JAVA_HOME/bin:$PATH"

echo "== checking real Redis is reachable at ${REDIS_HOST}:${REDIS_PORT} =="
if ! (exec 3<>"/dev/tcp/${REDIS_HOST}/${REDIS_PORT}") 2>/dev/null; then
  echo "FAILED: no Redis reachable at ${REDIS_HOST}:${REDIS_PORT} -- see spark-resume-redis/README.md" >&2
  exit 1
fi
exec 3>&- 3<&- || true

echo "== checking real Celeborn master REST API is reachable at ${CELEBORN_MASTER_REST} =="
if ! curl -sf --max-time 3 "${CELEBORN_MASTER_REST}/api/v1/applications" >/dev/null 2>&1; then
  echo "FAILED: no Celeborn master reachable at ${CELEBORN_MASTER_REST} -- see spark-resume-celeborn/run-celeborn-tests.sh" >&2
  exit 1
fi

# One value shared by BOTH separately-launched processes -- a fresh one per run so repeated runs
# never collide on the same Redis key / Celeborn appUniqueId.
export INTEGRATION_QUERY_ID="integration-$(date +%s)-$$"
echo "== INTEGRATION_QUERY_ID=${INTEGRATION_QUERY_ID} =="

echo "== building this module and its dependencies =="
cd "$REPO_ROOT"
mvn -o -pl spark-resume-api,spark-resume-core,spark-resume-spark-3.5,spark-resume-redis,spark-resume-celeborn,spark-resume-integration -am -DskipTests -Dmaven.test.skip=true install

echo "== ProcessA (producing side) -- its own JVM =="
mvn -o -pl spark-resume-integration test -Dspark.resume.integration.skipTests=false -Dsuites=org.apache.spark.resume.integration.ProcessASpec

echo "== ProcessB (resuming side) -- a SEPARATE, later-launched JVM =="
mvn -o -pl spark-resume-integration test -Dspark.resume.integration.skipTests=false -Dsuites=org.apache.spark.resume.integration.ProcessBSpec

echo "== SUCCESS: full pipeline composed end-to-end across two real processes, a real Redis, and a real Celeborn cluster =="
