#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

usage() {
  cat <<EOF
Usage:
  ci/mount_candidate_demo/demo.sh [options]

Options:
      --namespace <namespace>  Kubernetes namespace. Defaults to CTA_DEMO_NAMESPACE or dev.
      --keep-state             Do not restore tapes/drives or delete demo files on exit.
  -h, --help                   Show this help.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

step() {
  echo
  echo "================================================================================"
  echo "$*"
  echo "================================================================================"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

namespace="${CTA_DEMO_NAMESPACE:-dev}"
keep_state=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --namespace)
      [[ -n "${2:-}" && "$2" != -* ]] || die "--namespace requires an argument"
      namespace="$2"
      shift
      ;;
    --keep-state)
      keep_state=true
      ;;
    *)
      die "Unsupported argument: $1"
      ;;
  esac
  shift
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CTA_DEMO_NAMESPACE="$namespace"
export PATH="${script_dir}/wrappers:$PATH"

require_command kubectl
require_command jq
require_command cta-admin
require_command eos
require_command xrdcp
require_command xrdfs
require_command xrdfs-retrieve

run_id="$(date +%Y%m%d_%H%M%S)_$$"
reason="mount candidate demo"
selected_library=""
selected_drive=""
selected_candidate_key=""
selected_candidate_vid=""
eos_instance="ctaeos"
demo_dir=""
all_library_tapes=()
selected_tapes=()
demo_files=()

client_exec() {
  kubectl -n "$namespace" exec cta-client-0 -c client -- "$@"
}

wait_for_drive_status() {
  local drive="$1"
  local desired_status="$2"
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if cta-admin --json dr ls "$drive" \
      | jq -e --arg drive "$drive" --arg status "$desired_status" \
        '.[] | select(.driveName == $drive and .driveStatus == $status)' >/dev/null; then
      return 0
    fi
    sleep 1
  done
  cta-admin --json dr ls "$drive" | jq .
  die "Drive $drive did not reach status $desired_status"
}

set_drive_state() {
  local drive="$1"
  local state="$2"
  if [[ "$state" == "UP" ]]; then
    cta-admin dr up "$drive" --reason "$reason"
  else
    cta-admin dr down "$drive" --reason "$reason"
  fi
  wait_for_drive_status "$drive" "$state"
}

set_tape_state() {
  local vid="$1"
  local state="$2"
  cta-admin tape ch --vid "$vid" --state "$state" --reason "$reason"
}

wait_for_tape_state() {
  local vid="$1"
  local desired_state="$2"
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if cta-admin --json tape ls --vid "$vid" \
      | jq -e --arg vid "$vid" --arg state "$desired_state" \
        '.[] | select(.vid == $vid and .state == $state)' >/dev/null; then
      return 0
    fi
    sleep 1
  done
  cta-admin --json tape ls --vid "$vid" | jq .
  die "Tape $vid did not reach state $desired_state"
}

set_all_library_tapes() {
  local state="$1"
  local vid
  for vid in "${all_library_tapes[@]}"; do
    set_tape_state "$vid" "$state"
  done
  for vid in "${all_library_tapes[@]}"; do
    wait_for_tape_state "$vid" "$state"
  done
}

print_mount_candidates() {
  cta-admin mountcandidate ls || true
  echo
  cta-admin --json mountcandidate ls | jq .
}

wait_for_file_tape_only() {
  local path="$1"
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if eos "root://${eos_instance}" ls -y "$path" 2>/dev/null | grep -q 'd0::t1'; then
      return 0
    fi
    sleep 2
  done
  eos "root://${eos_instance}" file info "$path" || true
  die "File $path did not become tape-only"
}

wait_for_mount_candidate_filter() {
  local description="$1"
  shift
  local candidates
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    candidates=$(cta-admin --json mountcandidate ls)
    if jq -e "$@" <<< "$candidates" >/dev/null; then
      printf "%s\n" "$candidates"
      return 0
    fi
    sleep 2
  done
  printf "%s\n" "${candidates:-[]}" | jq .
  die "Timed out waiting for mount candidate condition: $description"
}

archive_new_file() {
  local index="$1"
  local wait="$2"
  local source="/tmp/mount_candidate_demo_${run_id}_${index}"
  local path="${demo_dir}/file_${index}.dat"

  client_exec dd if=/dev/urandom of="$source" bs=1M count=1 >&2
  xrdcp "$source" "root://${eos_instance}/${path}" >&2
  client_exec rm -f "$source" >&2

  if [[ "$wait" == true ]]; then
    wait_for_file_tape_only "$path" >&2
  fi

  printf "%s\n" "$path"
}

submit_retrieve() {
  local path="$1"
  xrdfs-retrieve "$eos_instance" prepare -s "$path"
}

cleanup() {
  local status=$?
  if [[ "$keep_state" == true ]]; then
    echo "Keeping demo state because --keep-state was provided."
    exit "$status"
  fi

  echo
  echo "Restoring demo state..."

  if [[ -n "$selected_candidate_key" ]]; then
    cta-admin mountcandidate ch --candidatekey "$selected_candidate_key" --score 0 >/dev/null 2>&1 || true
  fi

  local vid
  for vid in "${all_library_tapes[@]:-}"; do
    cta-admin tape ch --vid "$vid" --state ACTIVE --reason "$reason" >/dev/null 2>&1 || true
  done

  cta-admin dr up '.*' --reason "$reason" >/dev/null 2>&1 || true

  if [[ -n "$demo_dir" ]]; then
    eos "root://${eos_instance}" rm -rF --no-confirmation "$demo_dir" >/dev/null 2>&1 || true
  fi

  exit "$status"
}
trap cleanup EXIT

# Step 1: Check all tapes in one logical library and the drives associated with it.
step "Step 1: discover a logical library with at least 2 drives and 3 tapes"
drives_json=$(cta-admin --json dr ls)
tapes_json=$(cta-admin --json ta ls --all)

selected_library=$(
  jq -n \
    --argjson drives "$drives_json" \
    --argjson tapes "$tapes_json" \
    -r '
      [$drives[].logicalLibrary] | unique[] as $ll
      | select(([$drives[] | select(.logicalLibrary == $ll)] | length) >= 2)
      | select(([$tapes[] | select(.logicalLibrary == $ll)] | length) >= 3)
      | $ll
    ' | head -n 1
)

[[ -n "$selected_library" ]] || die "No logical library with at least 2 drives and 3 tapes found"

mapfile -t drives_in_library < <(
  jq -n -r --arg ll "$selected_library" --argjson drives "$drives_json" \
    '$drives | [.[] | select(.logicalLibrary == $ll)] | sort_by(.driveName) | .[].driveName'
)
mapfile -t all_library_tapes < <(
  jq -n -r --arg ll "$selected_library" --argjson tapes "$tapes_json" \
    '$tapes | [.[] | select(.logicalLibrary == $ll)] | sort_by(.vid) | .[].vid'
)
mapfile -t selected_tapes < <(
  printf "%s\n" "${all_library_tapes[@]}" | head -n 3
)

selected_drive="${drives_in_library[0]}"
echo "Logical library: $selected_library"
echo "Selected drive: $selected_drive"
printf "Drives in library:\n"
printf "  %s\n" "${drives_in_library[@]}"
printf "Selected tapes:\n"
printf "  %s\n" "${selected_tapes[@]}"

# Step 2: Put every drive down, then keep only the selected drive up.
step "Step 2: put all other drives down and keep $selected_drive up"
cta-admin dr down '.*' --reason "$reason"
for drive in "${drives_in_library[@]}"; do
  wait_for_drive_status "$drive" DOWN
done
set_drive_state "$selected_drive" UP

# Step 3: Archive one file per selected tape, allowing only that tape to be active.
step "Step 3: archive one file to each selected tape"
eos_instance=$(eos --json version 2>/dev/null | jq -r '.version[0].EOS_INSTANCE // empty' || true)
[[ -n "$eos_instance" ]] || eos_instance="ctaeos"
demo_dir="/eos/${eos_instance}/cta/mount_candidate_demo_${run_id}"
eos "root://${eos_instance}" mkdir -p "$demo_dir"

for index in "${!selected_tapes[@]}"; do
  vid="${selected_tapes[$index]}"
  echo "Archiving demo file $((index + 1)) with only tape $vid active"
  set_all_library_tapes DISABLED
  set_tape_state "$vid" ACTIVE
  wait_for_tape_state "$vid" ACTIVE
  demo_file=$(archive_new_file "$((index + 1))" true)
  demo_files+=("$demo_file")
  echo "Archived $demo_file to the active tape set containing $vid"
done

# Step 4: Disable all tapes and put all drives down so candidates can queue.
step "Step 4: disable all tapes and put all drives down"
set_all_library_tapes DISABLED
cta-admin dr down '.*' --reason "$reason"
for drive in "${drives_in_library[@]}"; do
  wait_for_drive_status "$drive" DOWN
done

# Step 5: Submit one archive request while the system is blocked.
step "Step 5: submit one blocked archive request"
blocked_archive_file=$(archive_new_file "blocked_archive" false)
echo "Submitted archive request for $blocked_archive_file"
wait_for_mount_candidate_filter \
  "archive candidate" \
  --arg ll "$selected_library" \
  '[.[] | select((.mountType | ascii_upcase | contains("ARCHIVE")) and .logicalLibrary == $ll and (.filesQueued | tonumber) >= 1)] | length > 0'

# Step 6: Show the mount candidate list with the archive candidate present.
step "Step 6: show mountcandidate ls after the archive request"
print_mount_candidates

# Step 7: Submit retrieve requests for all files archived on the selected tapes.
step "Step 7: submit retrieve requests for all archived files"
for file in "${demo_files[@]}"; do
  echo "Submitting retrieve for $file"
  submit_retrieve "$file"
done
wait_for_mount_candidate_filter \
  "retrieve candidates" \
  --arg ll "$selected_library" --argjson count "${#demo_files[@]}" \
  '[.[] | select((.mountType | ascii_upcase | contains("RETRIEVE")) and .logicalLibrary == $ll and (.filesQueued | tonumber) >= 1)] | length >= $count'

# Step 8: Show the mount candidate list with archive and retrieve candidates.
step "Step 8: show mountcandidate ls after retrieve submission"
print_mount_candidates

# Step 9: Manually increase the score of one retrieve mount candidate.
step "Step 9: increase one retrieve candidate score"
candidates_json=$(cta-admin --json mountcandidate ls)
selected_candidate_key=$(jq -r --arg ll "$selected_library" '
  [.[] | select((.mountType | ascii_upcase | contains("RETRIEVE")) and .logicalLibrary == $ll and (.vid // "") != "")]
  | sort_by(.candidateScore) | .[0].candidateKey // empty
' <<< "$candidates_json")
selected_candidate_vid=$(jq -r --arg key "$selected_candidate_key" '.[] | select(.candidateKey == $key) | .vid' <<< "$candidates_json")
candidate_score=$(jq -r --arg key "$selected_candidate_key" '.[] | select(.candidateKey == $key) | .candidateScore' <<< "$candidates_json")

[[ -n "$selected_candidate_key" ]] || die "Could not find a retrieve candidate key to override"
[[ -n "$selected_candidate_vid" ]] || die "Could not find a retrieve candidate VID to override"
[[ "$candidate_score" =~ ^[0-9]+$ ]] || candidate_score=0

override_score=$((candidate_score + 1000000))
echo "Boosting retrieve candidate $selected_candidate_key for VID $selected_candidate_vid to score $override_score"
cta-admin mountcandidate ch --candidatekey "$selected_candidate_key" --score "$override_score"

# Step 10: Show the mount candidate list after the manual score override.
step "Step 10: show mountcandidate ls after score override"
print_mount_candidates

# Step 11: Reactivate tapes and bring the selected drive up.
step "Step 11: reactivate tapes and bring $selected_drive up"
set_all_library_tapes ACTIVE
set_drive_state "$selected_drive" UP

# Step 12: Show that the selected drive mounts the boosted retrieve candidate.
step "Step 12: wait for the boosted retrieve mount to be selected"
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  drive_json=$(cta-admin --json dr ls "$selected_drive")
  if jq -e --arg drive "$selected_drive" --arg vid "$selected_candidate_vid" '
    .[] | select(
      .driveName == $drive
      and .vid == $vid
      and ((.mountType // "") | ascii_upcase | contains("RETRIEVE"))
      and (.driveStatus as $status | [ "MOUNTING", "TRANSFERRING", "UNLOADING", "UNMOUNTING", "DRAINING_TO_DISK" ] | index($status))
    )
  ' <<< "$drive_json" >/dev/null; then
    echo "$drive_json" | jq .
    echo "Selected drive is serving boosted retrieve candidate $selected_candidate_key on VID $selected_candidate_vid"
    exit 0
  fi

  candidate_json=$(cta-admin --json mountcandidate ls)
  if jq -e --arg key "$selected_candidate_key" --arg drive "$selected_drive" '
    .[] | select(.candidateKey == $key and .reservedByDrive == $drive)
  ' <<< "$candidate_json" >/dev/null; then
    echo "$candidate_json" | jq .
    echo "Selected drive reserved boosted retrieve candidate $selected_candidate_key"
    exit 0
  fi

  sleep 2
done

cta-admin --json dr ls "$selected_drive" | jq .
cta-admin --json mountcandidate ls | jq .
die "Did not observe $selected_drive selecting boosted retrieve candidate $selected_candidate_key"


# Archive 4 files in different tapes
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example0
until vids=$(cta-admin --json ta ls --all | jq -er '[.[] | select(.state == "ACTIVE" and (.lastFseq | tonumber) > 0) | .vid] | select(length > 0) | .[]'); do sleep 0.2; done; printf '%s\n' "$vids" | xargs -r -I {} cta-admin ta ch --state DISABLED --reason "Test" --vid {}
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example1
until vids=$(cta-admin --json ta ls --all | jq -er '[.[] | select(.state == "ACTIVE" and (.lastFseq | tonumber) > 0) | .vid] | select(length > 0) | .[]'); do sleep 0.2; done; printf '%s\n' "$vids" | xargs -r -I {} cta-admin ta ch --state DISABLED --reason "Test" --vid {}
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example2
until vids=$(cta-admin --json ta ls --all | jq -er '[.[] | select(.state == "ACTIVE" and (.lastFseq | tonumber) > 0) | .vid] | select(length > 0) | .[]'); do sleep 0.2; done; printf '%s\n' "$vids" | xargs -r -I {} cta-admin ta ch --state DISABLED --reason "Test" --vid {}
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example3
until vids=$(cta-admin --json ta ls --all | jq -er '[.[] | select(.state == "ACTIVE" and (.lastFseq | tonumber) > 0) | .vid] | select(length > 0) | .[]'); do sleep 0.2; done; printf '%s\n' "$vids" | xargs -r -I {} cta-admin ta ch --state DISABLED --reason "Test" --vid {}

# Put all drives down
cta-admin --json dr ls | jq -r '.[].driveName' | xargs -I {} cta-admin dr down {} --reason "Test"

# Try to archive two extra files
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example4
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example5

# Show some stuff
cta-admin tape ls --all
cta-admin sq
cta-admin mc ls

# Bump up the score of a mount and show again
cta-admin mc --ck RETRIEVE-ULT102 --score 999
cta-admin mc ls


