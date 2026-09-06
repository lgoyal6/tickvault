# Source this from anywhere inside the repository before running anything here.
#
# Spark ships its own Python worker launcher and will happily start a worker on
# whichever python3 is first on PATH, which is often a different minor version
# from the venv the driver is running. Pointing both at the venv is not
# optional; the mismatch surfaces as a worker crash halfway through a batch.
export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null)}"
TICKVAULT_ROOT="$(git rev-parse --show-toplevel)"
export TICKVAULT_VENV="${TICKVAULT_VENV:-$TICKVAULT_ROOT/.agent-work/venv}"
export PYSPARK_PYTHON="$TICKVAULT_VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PY="$PYSPARK_PYTHON"
