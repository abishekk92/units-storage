#!/bin/bash
set -e

echo "Checking all crates..."

# Helper to run cargo check with features
check_crate() {
  local crate_path=$1
  local crate_name=$(basename "$crate_path")
  local features=$2
  
  echo "===== Checking $crate_name ====="
  if [ -n "$features" ]; then
    echo "Running: cargo check --features $features in $crate_path"
    (cd "$crate_path" && fish -c "cargo check --features $features")
  else
    echo "Running: cargo check in $crate_path"
    (cd "$crate_path" && fish -c "cargo check")
  fi
  
  echo ""
}

# Check each crate
check_crate "./units-core"
check_crate "./units-proofs"
check_crate "./units-storage-impl" "all" 
check_crate "./units-runtime"
check_crate "./units"

echo "All checks complete!"
echo "If there are errors, fix them and run this script again."