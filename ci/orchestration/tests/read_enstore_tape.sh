#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

error() {
  echo "ERROR: $*" >&2
  exit 1
}

ensure_git() {
  if command -v git >/dev/null 2>&1; then
    return 0
  fi
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y git
  elif command -v yum >/dev/null 2>&1; then
    yum install -y git
  else
    error "git not found and no supported package manager detected."
  fi
}

clone_repo() {
  local repo="$1"
  local dir="$2"

  if [[ -d "$dir/.git" ]]; then
    log "Using existing repository at $dir"
    return 0
  fi
  if [[ -e "$dir" ]]; then
    error "Path '$dir' exists but is not a git repository. Set ENSTORE_MHVTL_DIR to a free path."
  fi

  ensure_git
  log "Cloning ${repo} into ${dir}"
  git clone --depth 1 "$repo" "$dir"
}

discover_repo_sample_dir() {
  local repo_root="$1"
  local format="$2"
  local candidates=()

  [[ -d "$repo_root" ]] || { echo ""; return 0; }

  if [[ "$format" == "enstorelarge" ]]; then
    mapfile -t candidates < <(find "$repo_root" -maxdepth 2 -type d \
      \( -iname '*enstorelarge*' -o -iname '*enstore_large*' -o -iname '*enstore-large*' \) \
      | sort || true)
  else
    mapfile -t candidates < <(find "$repo_root" -maxdepth 2 -type d -iname '*enstore*' \
      | grep -viE 'enstorelarge|enstore_large|enstore-large' \
      | sort || true)
  fi

  candidates+=("$repo_root")
  for candidate in "${candidates[@]}"; do
    [[ -n "$candidate" && -d "$candidate" ]] || continue
    if find "$candidate" -maxdepth 1 -type f ! -iname 'vol1*' ! -iname '*label*' -print -quit | grep -q .; then
      echo "$candidate"
      return 0
    fi
  done

  echo ""
}

get_size_bytes() {
  local target="$1"
  if command -v stat >/dev/null 2>&1; then
    if stat -c '%s' "$target" >/dev/null 2>&1; then
      stat -c '%s' "$target"
    elif stat -f '%z' "$target" >/dev/null 2>&1; then
      stat -f '%z' "$target"
    else
      wc -c < "$target"
    fi
  else
    wc -c < "$target"
  fi
}

ensure_file() {
  local path="$1"
  [[ -f "$path" ]] || error "Required file not found: $path"
}

discover_sample_dir() {
  local explicit="$1"
  local allow_empty="${2:-0}"
  local default_dirs=("$explicit" "/pnfs/Migration/CITest/Enstore" "/pnfs/Migration/CITest/enstore" "/opt/mhvtl/enstore" "/opt/mhvtl/samples/enstore" "/enstore-sample" "/enstore")

  if [[ -n "$explicit" ]]; then
    [[ -d "$explicit" ]] || error "ENSTORE_SAMPLE_DIR set to '$explicit' but directory does not exist."
    echo "$explicit"
    return 0
  fi

  for candidate in "${default_dirs[@]}"; do
    [[ -n "$candidate" && -d "$candidate" ]] || continue
    echo "$candidate"
    return 0
  done

  if [[ "$allow_empty" == "1" ]]; then
    echo ""
    return 0
  fi

  error "No sample directory found. Set ENSTORE_SAMPLE_DIR to the directory containing the Enstore sample payload(s)."
}

select_sample_leaf_dir() {
  local root="$1"
  local subdir="${2:-}"

  [[ -n "$root" ]] || { echo ""; return 0; }

  if [[ -n "$subdir" ]]; then
    local explicit="${root%/}/$subdir"
    [[ -d "$explicit" ]] || error "ENSTORE_SAMPLE_SUBDIR set to '$subdir' but directory does not exist under $root."
    echo "$explicit"
    return 0
  fi

  if find "$root" -maxdepth 1 -type f -iname 'vol1*' -print -quit | grep -q .; then
    echo "$root"
    return 0
  fi

  local candidate
  candidate="$(find "$root" -mindepth 2 -maxdepth 2 -type f -iname 'vol1*' | sort | head -n 1 || true)"
  if [[ -n "$candidate" ]]; then
    echo "$(dirname "$candidate")"
    return 0
  fi

  echo "$root"
}

discover_label() {
  local sample_dir="$1"
  local override="$2"

  if [[ -n "$override" ]]; then
    ensure_file "$override"
    echo "$override"
    return 0
  fi

  if [[ -n "$sample_dir" ]]; then
    local candidate
    candidate="$(find "$sample_dir" -maxdepth 1 -type f \( -iname 'vol1*' -o -iname '*label*' \) | head -n 1 || true)"
    if [[ -n "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  fi

  echo ""
}

extract_vid_from_label() {
  local label_path="$1"
  python3 - "$label_path" <<'PY'
import sys
path = sys.argv[1]
with open(path, "rb") as f:
    data = f.read(16)
if len(data) < 10:
    sys.exit(1)
vsn = data[4:10].decode("ascii", errors="ignore").strip()
if vsn:
    print(vsn)
PY
}

generate_vol1_label() {
  local vid="$1"
  local lbl_standard="${2:-0}"
  local lbp_method="${3:-"  "}"
  local out="$4"

  python3 - "$vid" "$lbl_standard" "$lbp_method" "$out" <<'PY'
import sys

vid = sys.argv[1][:6].ljust(6)
lbl = sys.argv[2][:1] if sys.argv[2] else "0"
lbp = sys.argv[3][:2].ljust(2)
out = sys.argv[4]

parts = [
    b"VOL1",
    vid.encode("ascii"),
    b" ",              # accessibility
    b" " * 13,         # reserved1
    b" " * 13,         # implID
    b"CTA".ljust(14),  # ownerID
    b" " * 26,         # reserved2
    lbp.encode("ascii"),
    lbl.encode("ascii"),
]
payload = b"".join(parts)
if len(payload) != 80:
    raise SystemExit(f"Unexpected VOL1 length: {len(payload)}")
with open(out, "wb") as fh:
    fh.write(payload)
PY
}

discover_payloads() {
  local sample_dir="$1"
  local payload_list="$2"

  if [[ -n "$payload_list" ]]; then
    IFS=',' read -r -a entries <<< "$payload_list"
    local resolved=()
    for item in "${entries[@]}"; do
      [[ -n "$item" ]] || continue
      if [[ "$item" = /* || -z "$sample_dir" ]]; then
        resolved+=("$item")
      else
        resolved+=("${sample_dir%/}/$item")
      fi
    done
    printf '%s\n' "${resolved[@]}"
    return 0
  fi

  [[ -n "$sample_dir" ]] || return 0

  local matched
  matched="$(find "$sample_dir" -maxdepth 1 -type f \( -iname '*.bin' -o -iname '*.img' -o -iname '*.cpio' -o -iname '*.payload' -o -iname '*.tar' \) \
    | grep -viE 'vol1|label' \
    | sort || true)"
  if [[ -n "$matched" ]]; then
    printf '%s\n' "$matched"
    return 0
  fi

  find "$sample_dir" -maxdepth 1 -type f \
    | grep -viE 'vol1|label' \
    | sort || true
}

discover_enstore_payloads() {
  local sample_dir="$1"
  local payload_list="$2"

  if [[ -n "$payload_list" ]]; then
    IFS=',' read -r -a entries <<< "$payload_list"
    for item in "${entries[@]}"; do
      [[ -n "$item" ]] || continue
      local payload
      if [[ "$item" = /* || -z "$sample_dir" ]]; then
        payload="$item"
      else
        payload="${sample_dir%/}/$item"
      fi
      local trailer=""
      if [[ "$payload" == *_payload.* ]]; then
        local candidate="${payload/_payload./_trailer.}"
        [[ -f "$candidate" ]] && trailer="$candidate"
      fi
      printf '%s\t%s\n' "$payload" "$trailer"
    done
    return 0
  fi

  [[ -n "$sample_dir" ]] || return 0

  local payloads=()
  mapfile -t payloads < <(find "$sample_dir" -maxdepth 1 -type f -iname 'fseq*_payload.*' | sort || true)
  if [[ ${#payloads[@]} -gt 0 ]]; then
    for payload in "${payloads[@]}"; do
      local trailer=""
      local candidate="${payload/_payload./_trailer.}"
      [[ -f "$candidate" ]] && trailer="$candidate"
      printf '%s\t%s\n' "$payload" "$trailer"
    done
    return 0
  fi

  mapfile -t payloads < <(discover_payloads "$sample_dir" "")
  for payload in "${payloads[@]}"; do
    printf '%s\t%s\n' "$payload" ""
  done
}

generate_cpio_payload() {
  local out="$1"
  local block_size="$2"
  local name="$3"
  local file_size="$4"

  python3 - "$out" "$block_size" "$name" "$file_size" <<'PY'
import sys
import time

out_path = sys.argv[1]
block_size = int(sys.argv[2])
name = sys.argv[3]
file_size = int(sys.argv[4])

if block_size < 1:
    raise SystemExit("Invalid block size")
if file_size < 1:
    raise SystemExit("Invalid file size")

name_bytes = name.encode("ascii", errors="ignore")
name_size = len(name_bytes) + 1

magic = "070707"
dev = 0
ino = 1
mode = 0o100644
uid = 0
gid = 0
nlink = 1
rdev = 0
mtime = int(time.time())

header = (
    f"{magic}"
    f"{dev:06o}"
    f"{ino:06o}"
    f"{mode:06o}"
    f"{uid:06o}"
    f"{gid:06o}"
    f"{nlink:06o}"
    f"{rdev:06o}"
    f"{mtime:011o}"
    f"{name_size:06o}"
    "H"
    f"{file_size:010X}"
).encode("ascii")

with open(out_path, "wb") as fh:
    fh.write(header)
    fh.write(name_bytes + b"\0")
    remaining = file_size
    chunk = b"A" * 65536
    while remaining > 0:
        take = chunk[: min(len(chunk), remaining)]
        fh.write(take)
        remaining -= len(take)
    pad_len = (-fh.tell()) % block_size
    if pad_len:
        fh.write(b"\0" * pad_len)
PY
}

DEVICE="${1:-}"
[[ -n "$DEVICE" ]] || error "Usage: $0 <tape_device>"

MHVTL_REPO="${ENSTORE_MHVTL_REPO:-https://github.com/LTrestka/ens-mhvtl.git}"
MHVTL_DIR="${ENSTORE_MHVTL_DIR:-/ens-mhvtl}"
clone_repo "$MHVTL_REPO" "$MHVTL_DIR"
REPO_SAMPLE_DIR="$(discover_repo_sample_dir "$MHVTL_DIR" "enstore")"

SAMPLE_DIR="$(discover_sample_dir "${ENSTORE_SAMPLE_DIR:-$REPO_SAMPLE_DIR}" 1)"
SAMPLE_DIR="$(select_sample_leaf_dir "$SAMPLE_DIR" "${ENSTORE_SAMPLE_SUBDIR:-}")"
LABEL_FILE="$(discover_label "$SAMPLE_DIR" "${ENSTORE_LABEL_FILE:-}")"
PAYLOAD_BLOCK_SIZE="${ENSTORE_PAYLOAD_BLOCK_SIZE:-1048576}"
LABEL_BLOCK_SIZE="${ENSTORE_LABEL_BLOCK_SIZE:-}"
PAYLOAD_LIST="${ENSTORE_PAYLOADS:-}"
VID_OVERRIDE="${ENSTORE_VID:-}"
CHANGER_DEVICE="${MTX_CHANGER:-/dev/smc}"
DRIVE_SLOT="${ENSTORE_DRIVE_SLOT:-0}"
SLOT="${ENSTORE_SLOT:-2}"
WRITE_DOUBLE_EOF="${ENSTORE_WRITE_DOUBLE_EOF:-1}"
SYNTHETIC_NAME="${ENSTORE_SYNTHETIC_NAME:-CTA_TEST}"
SYNTHETIC_SIZE="${ENSTORE_SYNTHETIC_FILE_SIZE:-1024}"

if [[ -n "$SAMPLE_DIR" ]]; then
  log "Using sample directory: $SAMPLE_DIR"
else
  log "No sample directory found; will generate a minimal Enstore payload."
fi

if [[ -n "$LABEL_FILE" ]]; then
  log "Found label file: $LABEL_FILE"
else
  log "No label file found; will generate VOL1 on the fly."
fi

mapfile -t PAYLOAD_ENTRIES < <(discover_enstore_payloads "$SAMPLE_DIR" "$PAYLOAD_LIST")
if [[ ${#PAYLOAD_ENTRIES[@]} -eq 0 ]]; then
  log "No payload files discovered; generating a synthetic CPIO payload."
  SYNTH_DIR="$(mktemp -d /tmp/enstore_payload.XXXXXX)"
  SYNTH_PAYLOAD="${SYNTH_DIR}/enstore_payload.bin"
  generate_cpio_payload "$SYNTH_PAYLOAD" "$PAYLOAD_BLOCK_SIZE" "$SYNTHETIC_NAME" "$SYNTHETIC_SIZE"
  PAYLOAD_ENTRIES=("${SYNTH_PAYLOAD}"$'\t'"")
fi

log "Payloads to write (in order):"
PAYLOADS=()
TRAILERS=()
for entry in "${PAYLOAD_ENTRIES[@]}"; do
  payload="${entry%%$'\t'*}"
  trailer="${entry#*$'\t'}"
  ensure_file "$payload"
  [[ -n "$trailer" ]] && ensure_file "$trailer"
  PAYLOADS+=("$payload")
  TRAILERS+=("$trailer")
  if [[ -n "$trailer" ]]; then
    log " - $payload (trailer: $trailer)"
  else
    log " - $payload"
  fi
done

VID=""
if [[ -n "$VID_OVERRIDE" ]]; then
  VID="$VID_OVERRIDE"
elif [[ -n "$LABEL_FILE" ]]; then
  VID="$(extract_vid_from_label "$LABEL_FILE" || true)"
fi

if [[ -z "$VID" ]]; then
  base_name="$(basename "${PAYLOADS[0]}")"
  if [[ "$base_name" =~ ([A-Za-z0-9]{5,6}) ]]; then
    VID="${BASH_REMATCH[1]}"
  else
    VID="EN0001"
  fi
fi

log "Using VSN: $VID"

if [[ -z "$LABEL_FILE" ]]; then
  LABEL_FILE="$(mktemp /tmp/enstore_vol1.XXXXXX)"
  generate_vol1_label "$VID" "${ENSTORE_LABEL_STANDARD:-0}" "${ENSTORE_LBP_METHOD:-  }" "$LABEL_FILE"
  LABEL_BLOCK_SIZE=80
fi

ensure_file "$LABEL_FILE"

if [[ -z "$LABEL_BLOCK_SIZE" ]]; then
  LABEL_BLOCK_SIZE="$(get_size_bytes "$LABEL_FILE")"
fi

log "Loading slot ${SLOT} into drive ${DRIVE_SLOT} via changer ${CHANGER_DEVICE}"
if command -v mtx >/dev/null 2>&1; then
  mtx -f "$CHANGER_DEVICE" status || true
  mtx -f "$CHANGER_DEVICE" load "$SLOT" "$DRIVE_SLOT"
  mtx -f "$CHANGER_DEVICE" status || true
else
  log "mtx not available; skipping changer operations."
fi

mt -f "$DEVICE" status || true
mt -f "$DEVICE" rewind

log "Writing VOL1 label (bs=${LABEL_BLOCK_SIZE})"
dd if="$LABEL_FILE" of="$DEVICE" bs="$LABEL_BLOCK_SIZE"
mt -f "$DEVICE" weof

for idx in "${!PAYLOADS[@]}"; do
  payload="${PAYLOADS[$idx]}"
  trailer="${TRAILERS[$idx]:-}"
  log "Writing payload $(basename "$payload") with bs=${PAYLOAD_BLOCK_SIZE}"
  dd if="$payload" of="$DEVICE" bs="$PAYLOAD_BLOCK_SIZE"
  if [[ -n "$trailer" ]]; then
    log "Writing trailer-only block $(basename "$trailer") with bs=${PAYLOAD_BLOCK_SIZE}"
    dd if="$trailer" of="$DEVICE" bs="$PAYLOAD_BLOCK_SIZE"
  fi
  mt -f "$DEVICE" weof
done

if [[ "$WRITE_DOUBLE_EOF" != "0" ]]; then
  log "Writing extra filemark to signal logical end-of-data"
  mt -f "$DEVICE" weof
fi

mt -f "$DEVICE" rewind
log "Enstore sample prepared on ${DEVICE}"
