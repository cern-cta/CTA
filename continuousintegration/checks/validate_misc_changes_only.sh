#!/bin/bash
set -e

die() {
  echo "$@" 1>&2
  exit 1
}

usage() {
  echo "Checks whether all modified files in comparison to the given branch are in the provided whitelist file."
  echo ""
  echo "Usage: $0 [options] -f <namespace>"
  echo ""
  echo "Options:"
  echo "  -h, --help:                   Show help output."
  echo "  -w|--whitelist <file>:        File containing the whitelist entries."
  echo "  -b|--branch <branch>:         Branch to determine changes against."
  exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -h | --help) usage ;;
    -w|--whitelist)
      WHITELIST_FILE="$2"
      shift ;;
    -b|--branch)
      COMPARE_BRANCH="$2"
      shift ;;
    *)
      echo "Unsupported argument: $1"
      usage
      ;;
  esac
  shift
done

if [ -z "${WHITELIST_FILE}" ]; then
  echo "Missing mandatory argument: -w | --whitelist"
  usage
fi

test -f "${WHITELIST_FILE}" || die "Whitelist file ${WHITELIST_FILE} does not exist"

if [ -z "${COMPARE_BRANCH}" ]; then
  echo "Missing mandatory argument: -b | --branch"
  usage
fi

# Read whitelist patterns into an array
mapfile -t WHITELIST < "$WHITELIST_FILE"

# Get the list of changed files
git fetch "origin/$COMPARE_BRANCH"
CHANGED_FILES=$(git diff --name-only "origin/$COMPARE_BRANCH")

# Check each changed file against the whitelist
NON_WHITELISTED=()

for file in $CHANGED_FILES; do
  MATCHED=false
  for pattern in "${WHITELIST[@]}"; do
    if [[ "$file" == $pattern ]]; then
      MATCHED=true
      break
    fi
  done
  if ! $MATCHED; then
    NON_WHITELISTED+=("$file")
  fi
done

# If any files are not whitelisted, fail
if [ ${#NON_WHITELISTED[@]} -ne 0 ]; then
  echo "The following files are not in the whitelist:"
  for f in "${NON_WHITELISTED[@]}"; do
    echo "  $f"
  done
  exit 1
else
  echo "All changed files are whitelisted."
fi
