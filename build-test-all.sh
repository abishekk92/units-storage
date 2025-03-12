#!/bin/bash
set -e

echo "Building and testing all crates..."

# Parse command line arguments
TEST_ONLY=false
BUILD_ONLY=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --test-only)
      TEST_ONLY=true
      shift
      ;;
    --build-only)
      BUILD_ONLY=true
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--test-only] [--build-only]"
      exit 1
      ;;
  esac
done

# Helper to run cargo commands with features
run_cargo_command() {
  local crate_path=$1
  local crate_name=$(basename "$crate_path")
  local command=$2
  local features=$3
  
  echo "===== $command $crate_name ====="
  if [ -n "$features" ]; then
    echo "Running: cargo $command --features $features in $crate_path"
    (cd "$crate_path" && fish -c "cargo $command --features $features")
  else
    echo "Running: cargo $command in $crate_path"
    (cd "$crate_path" && fish -c "cargo $command")
  fi
  
  echo ""
}

# Build each crate
if [ "$TEST_ONLY" = false ]; then
  echo "=== Building all crates ==="
  run_cargo_command "./units-core" "build" ""
  run_cargo_command "./units-proofs" "build" ""
  run_cargo_command "./units-storage-impl" "build" "all"
  run_cargo_command "./units-runtime" "build" ""
  run_cargo_command "./units" "build" ""
fi

# Test each crate
if [ "$BUILD_ONLY" = false ]; then
  echo "=== Testing all crates ==="
  run_cargo_command "./units-core" "test" ""
  run_cargo_command "./units-proofs" "test" ""
  run_cargo_command "./units-storage-impl" "test" "all"
  run_cargo_command "./units-runtime" "test" ""
  run_cargo_command "./units" "test" ""
fi

echo "All operations complete!"