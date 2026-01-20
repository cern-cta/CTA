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
    error "Path '$dir' exists but is not a git repository. Set ENSTORE_LARGE_MHVTL_DIR to a free path."
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
  local default_dirs=("$explicit" "/pnfs/Migration/CITest/EnstoreLarge" "/pnfs/Migration/CITest/enstorelarge" "/opt/mhvtl/enstorelarge" "/opt/mhvtl/samples/enstorelarge" "/enstore-large-sample" "/enstore-large" "/enstorelarge")

  if [[ -n "$explicit" ]]; then
    [[ -d "$explicit" ]] || error "ENSTORE_LARGE_SAMPLE_DIR set to '$explicit' but directory does not exist."
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

  error "No sample directory found. Set ENSTORE_LARGE_SAMPLE_DIR to the directory containing the EnstoreLarge sample payload(s)."
}

select_sample_leaf_dir() {
  local root="$1"
  local subdir="${2:-}"

  [[ -n "$root" ]] || { echo ""; return 0; }

  if [[ -n "$subdir" ]]; then
    local explicit="${root%/}/$subdir"
    [[ -d "$explicit" ]] || error "ENSTORE_LARGE_SAMPLE_SUBDIR set to '$subdir' but directory does not exist under $root."
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
  local lbl_standard="${2:-3}"
  local lbp_method="${3:-"  "}"
  local out="$4"

  python3 - "$vid" "$lbl_standard" "$lbp_method" "$out" <<'PY'
import sys

vid = sys.argv[1][:6].ljust(6)
lbl = sys.argv[2][:1] if sys.argv[2] else "3"
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

discover_enstore_large_payloads() {
  local sample_dir="$1"
  local payload_list="$2"
  local block_size="$3"

  if [[ -n "$payload_list" ]]; then
    discover_payloads "$sample_dir" "$payload_list"
    return 0
  fi

  [[ -n "$sample_dir" ]] || return 0

  local header=""
  local trailer=""
  local payload=""

  header="$(find "$sample_dir" -maxdepth 1 -type f \( -iname '*header*' -o -iname '*hdr*' -o -iname '*uhl*' \) | sort | head -n 1 || true)"
  trailer="$(find "$sample_dir" -maxdepth 1 -type f \( -iname '*trailer*' -o -iname '*eof*' -o -iname '*utl*' \) | sort | head -n 1 || true)"

  if [[ -n "$header" && -n "$trailer" ]]; then
    payload="$(find "$sample_dir" -maxdepth 1 -type f \( -iname '*payload*' -o -iname '*data*' -o -iname '*file*' \) \
      | grep -viE 'vol1|label' | sort | head -n 1 || true)"
    if [[ -z "$payload" ]]; then
      payload="$(find "$sample_dir" -maxdepth 1 -type f ! -iname 'vol1*' ! -iname '*label*' \
        ! -samefile "$header" ! -samefile "$trailer" | sort | head -n 1 || true)"
    fi
    if [[ -n "$payload" ]]; then
      printf '%s\n' "$header" "$payload" "$trailer"
      return 0
    fi
  fi

  mapfile -t all_files < <(discover_payloads "$sample_dir" "")
  if [[ ${#all_files[@]} -ge 3 && "$block_size" =~ ^[0-9]+$ && "$block_size" -gt 0 ]]; then
    local small=()
    local large=()
    local size
    for file in "${all_files[@]}"; do
      size="$(get_size_bytes "$file")"
      if [[ "$size" -eq "$block_size" ]]; then
        small+=("$file")
      else
        large+=("$file")
      fi
    done
    if [[ ${#small[@]} -ge 2 && ${#large[@]} -ge 1 ]]; then
      printf '%s\n' "${small[0]}" "${large[0]}" "${small[1]}"
      return 0
    fi
  fi

  return 0
}

generate_enstore_large_payloads() {
  local out_dir="$1"
  local vid="$2"
  local block_size="$3"
  local file_size="$4"
  local fseq="$5"
  local file_id="$6"
  local site_name="$7"
  local mover_host="$8"
  local drive_vendor="$9"
  local drive_model="${10}"
  local drive_serial="${11}"

  python3 - "$out_dir" "$vid" "$block_size" "$file_size" "$fseq" "$file_id" "$site_name" "$mover_host" "$drive_vendor" "$drive_model" "$drive_serial" <<'PY'
import math
import sys
import time

out_dir = sys.argv[1]
vid = sys.argv[2]
block_size = int(sys.argv[3])
file_size = int(sys.argv[4])
fseq = int(sys.argv[5])
file_id = sys.argv[6]
site_name = sys.argv[7]
mover_host = sys.argv[8]
drive_vendor = sys.argv[9]
drive_model = sys.argv[10]
drive_serial = sys.argv[11]

if block_size < 1:
    raise SystemExit("Invalid block size")
if file_size < 1:
    raise SystemExit("Invalid file size")

def pad_right(value, length):
    return value[:length].ljust(length, " ")

def pad_int(value, length):
    mod = 10 ** length
    return str(int(value) % mod).zfill(length)

def cyyddd():
    tm = time.localtime()
    tm_year = tm.tm_year - 1900
    century = "0" if (tm_year // 100) else " "
    return f"{century}{tm_year % 100:02d}{tm.tm_yday + 1:03d}"

date_str = cyyddd()
sys_code = pad_right("CTA", 13)
vsn = pad_right(vid, 6)
file_id = pad_right(file_id, 17)

def hdr1():
    return (
        pad_right("HDR1", 4)
        + file_id
        + vsn
        + pad_right("0001", 4)
        + pad_int(fseq, 4)
        + pad_right("0001", 4)
        + pad_right("00", 2)
        + date_str
        + date_str
        + pad_right("", 1)
        + pad_right("000000", 6)
        + sys_code
        + pad_right("", 7)
    ).encode("ascii")

def eof1():
    return (
        pad_right("EOF1", 4)
        + file_id
        + vsn
        + pad_right("0001", 4)
        + pad_int(fseq, 4)
        + pad_right("0001", 4)
        + pad_right("00", 2)
        + date_str
        + date_str
        + pad_right("", 1)
        + pad_int(1, 6)
        + sys_code
        + pad_right("", 7)
    ).encode("ascii")

def hdr2(label):
    if block_size < 100000:
        blk = pad_int(block_size, 5)
    else:
        blk = "00000"
    return (
        pad_right(label, 4)
        + pad_right("D", 1)
        + blk
        + blk
        + pad_right("", 1)
        + pad_right("", 18)
        + pad_right("", 2)
        + pad_right("", 14)
        + pad_right("00", 2)
        + pad_right("", 28)
    ).encode("ascii")

def uhl1(label):
    return (
        pad_right(label, 4)
        + pad_int(fseq, 10)
        + pad_int(block_size, 10)
        + pad_int(block_size, 10)
        + pad_right(site_name.upper(), 8)
        + pad_right(mover_host.upper(), 10)
        + pad_right(drive_vendor, 8)
        + pad_right(drive_model, 8)
        + pad_right(drive_serial, 12)
    ).encode("ascii")

def uhl2():
    return (
        pad_right("UHL2", 4)
        + pad_right("", 60)
        + pad_int(file_size, 14)
        + pad_right("", 2)
    ).encode("ascii")

def build_block(payload):
    block = bytearray(b" " * block_size)
    block[: len(payload)] = payload
    return block

header = hdr1() + hdr2("HDR2") + uhl1("UHL1") + uhl2()
trailer = eof1() + hdr2("EOF2") + uhl1("UTL1")

if block_size < len(header) or block_size < len(trailer):
    raise SystemExit("Block size too small for EnstoreLarge headers/trailers")

with open(f"{out_dir}/enstore_large_header.bin", "wb") as fh:
    fh.write(build_block(header))

payload_blocks = max(1, math.ceil(file_size / block_size))
payload_bytes = payload_blocks * block_size
with open(f"{out_dir}/enstore_large_payload.bin", "wb") as fh:
    chunk = b"\0" * 65536
    remaining = payload_bytes
    while remaining > 0:
        take = min(len(chunk), remaining)
        fh.write(chunk[:take])
        remaining -= take

with open(f"{out_dir}/enstore_large_trailer.bin", "wb") as fh:
    fh.write(build_block(trailer))
PY
}

DEVICE="${1:-}"
[[ -n "$DEVICE" ]] || error "Usage: $0 <tape_device>"

MHVTL_REPO="${ENSTORE_LARGE_MHVTL_REPO:-${ENSTORE_MHVTL_REPO:-https://github.com/LTrestka/ens-mhvtl.git}}"
MHVTL_DIR="${ENSTORE_LARGE_MHVTL_DIR:-${ENSTORE_MHVTL_DIR:-/ens-mhvtl}}"
clone_repo "$MHVTL_REPO" "$MHVTL_DIR"
REPO_SAMPLE_DIR="$(discover_repo_sample_dir "$MHVTL_DIR" "enstorelarge")"

SAMPLE_DIR="$(discover_sample_dir "${ENSTORE_LARGE_SAMPLE_DIR:-$REPO_SAMPLE_DIR}" 1)"
SAMPLE_DIR="$(select_sample_leaf_dir "$SAMPLE_DIR" "${ENSTORE_LARGE_SAMPLE_SUBDIR:-}")"
LABEL_FILE="$(discover_label "$SAMPLE_DIR" "${ENSTORE_LARGE_LABEL_FILE:-}")"
PAYLOAD_BLOCK_SIZE="${ENSTORE_LARGE_BLOCK_SIZE:-262144}"
LABEL_BLOCK_SIZE="${ENSTORE_LARGE_LABEL_BLOCK_SIZE:-}"
PAYLOAD_LIST="${ENSTORE_LARGE_PAYLOADS:-}"
VID_OVERRIDE="${ENSTORE_LARGE_VID:-}"
CHANGER_DEVICE="${MTX_CHANGER:-/dev/smc}"
DRIVE_SLOT="${ENSTORE_LARGE_DRIVE_SLOT:-0}"
SLOT="${ENSTORE_LARGE_SLOT:-3}"
WRITE_DOUBLE_EOF="${ENSTORE_LARGE_WRITE_DOUBLE_EOF:-1}"
SYNTHETIC_SIZE="${ENSTORE_LARGE_SYNTHETIC_FILE_SIZE:-1024}"
SYNTHETIC_FILE_ID="${ENSTORE_LARGE_FILE_ID:-CTA0000000000001}"
SYNTHETIC_SITE="${ENSTORE_LARGE_SITE:-CERN}"
SYNTHETIC_MOVER="${ENSTORE_LARGE_MOVER_HOST:-CTAMOVER}"
SYNTHETIC_VENDOR="${ENSTORE_LARGE_DRIVE_VENDOR:-STK}"
SYNTHETIC_MODEL="${ENSTORE_LARGE_DRIVE_MODEL:-MHVTL}"
SYNTHETIC_SERIAL="${ENSTORE_LARGE_DRIVE_SERIAL:-S001L01D01}"

if [[ -n "$SAMPLE_DIR" ]]; then
  log "Using sample directory: $SAMPLE_DIR"
else
  log "No sample directory found; will generate a minimal EnstoreLarge payload."
fi

if [[ -n "$LABEL_FILE" ]]; then
  log "Found label file: $LABEL_FILE"
else
  log "No label file found; will generate VOL1 on the fly."
fi

mapfile -t PAYLOADS < <(discover_enstore_large_payloads "$SAMPLE_DIR" "$PAYLOAD_LIST" "$PAYLOAD_BLOCK_SIZE")

VID=""
if [[ -n "$VID_OVERRIDE" ]]; then
  VID="$VID_OVERRIDE"
elif [[ -n "$LABEL_FILE" ]]; then
  VID="$(extract_vid_from_label "$LABEL_FILE" || true)"
fi

if [[ -z "$VID" ]]; then
  if [[ ${#PAYLOADS[@]} -gt 0 ]]; then
    base_name="$(basename "${PAYLOADS[0]}")"
    if [[ "$base_name" =~ ([A-Za-z0-9]{5,6}) ]]; then
      VID="${BASH_REMATCH[1]}"
    else
      VID="EL0001"
    fi
  else
    VID="EL0001"
  fi
fi

log "Using VSN: $VID"

if [[ ${#PAYLOADS[@]} -eq 0 ]]; then
  log "No payload files discovered; generating synthetic EnstoreLarge header/payload/trailer."
  SYNTH_DIR="$(mktemp -d /tmp/enstore_large_payload.XXXXXX)"
  generate_enstore_large_payloads "$SYNTH_DIR" "$VID" "$PAYLOAD_BLOCK_SIZE" "$SYNTHETIC_SIZE" 1 \
    "$SYNTHETIC_FILE_ID" "$SYNTHETIC_SITE" "$SYNTHETIC_MOVER" "$SYNTHETIC_VENDOR" "$SYNTHETIC_MODEL" "$SYNTHETIC_SERIAL"
  PAYLOADS=(
    "${SYNTH_DIR}/enstore_large_header.bin"
    "${SYNTH_DIR}/enstore_large_payload.bin"
    "${SYNTH_DIR}/enstore_large_trailer.bin"
  )
fi

log "Payloads to write (in order):"
for p in "${PAYLOADS[@]}"; do
  ensure_file "$p"
  log " - $p"
done

if [[ ${#PAYLOADS[@]} -eq 3 ]]; then
  header_size="$(get_size_bytes "${PAYLOADS[0]}")"
  trailer_size="$(get_size_bytes "${PAYLOADS[2]}")"
  if [[ "$header_size" -ne "$PAYLOAD_BLOCK_SIZE" || "$trailer_size" -ne "$PAYLOAD_BLOCK_SIZE" ]]; then
    error "Header/trailer size mismatch: header=${header_size} trailer=${trailer_size} expected ${PAYLOAD_BLOCK_SIZE}"
  fi
fi

if [[ -z "$LABEL_FILE" ]]; then
  LABEL_FILE="$(mktemp /tmp/enstore_large_vol1.XXXXXX)"
  generate_vol1_label "$VID" "${ENSTORE_LARGE_LABEL_STANDARD:-3}" "${ENSTORE_LARGE_LBP_METHOD:-  }" "$LABEL_FILE"
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

for payload in "${PAYLOADS[@]}"; do
  log "Writing payload $(basename "$payload") with bs=${PAYLOAD_BLOCK_SIZE}"
  dd if="$payload" of="$DEVICE" bs="$PAYLOAD_BLOCK_SIZE"
  mt -f "$DEVICE" weof
done

if [[ "$WRITE_DOUBLE_EOF" != "0" ]]; then
  log "Writing extra filemark to signal logical end-of-data"
  mt -f "$DEVICE" weof
fi

mt -f "$DEVICE" rewind
log "EnstoreLarge sample prepared on ${DEVICE}"
