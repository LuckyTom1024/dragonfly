#!/usr/bin/env bash

# Configure and build Dragonfly, then generate a ccls project index.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_TYPE="Debug"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build-dbg}"
BUILD_DIR_EXPLICIT=0
TARGET="dragonfly"
JOBS="${JOBS:-$(nproc)}"
GENERATE_INDEX=1

if [[ "${BUILD_DIR}" != /* ]]; then
  BUILD_DIR="${ROOT_DIR}/${BUILD_DIR}"
fi

BLAZE_EXTRA=(
  "-DWITH_AWS=OFF"
  "-DWITH_GCP=OFF"
)
RELEASE_FLAG=0

# Some development images expose an old OpenSSL under /usr/local while the
# system OpenSSL 3 headers and libraries are available separately. Prefer the
# latter when present so helio/util/tls compiles consistently.
OPENSSL3_ROOT="${OPENSSL3_ROOT:-/tmp/openssl3}"
OPENSSL3_SSL=""
OPENSSL3_CRYPTO=""
for candidate in /usr/lib/*/libssl.so.3 /lib/*/libssl.so.3 /usr/lib/libssl.so.3; do
  if [[ -f "${candidate}" ]]; then
    OPENSSL3_SSL="${candidate}"
    break
  fi
done
for candidate in /usr/lib/*/libcrypto.so.3 /lib/*/libcrypto.so.3 /usr/lib/libcrypto.so.3; do
  if [[ -f "${candidate}" ]]; then
    OPENSSL3_CRYPTO="${candidate}"
    break
  fi
done
if [[ -f "${OPENSSL3_ROOT}/openssl/ssl.h" && -n "${OPENSSL3_SSL}" && -n "${OPENSSL3_CRYPTO}" ]]; then
  BLAZE_EXTRA+=(
    "-DOPENSSL_INCLUDE_DIR=${OPENSSL3_ROOT}"
    "-DOPENSSL_SSL_LIBRARY=${OPENSSL3_SSL}"
    "-DOPENSSL_CRYPTO_LIBRARY=${OPENSSL3_CRYPTO}"
  )
fi

usage() {
  cat <<EOF
Usage: $(basename "$0") [options] [-- cmake-options]

Build Dragonfly and generate a ccls index.

Options:
  --debug              Build a Debug configuration (default).
  --release            Build a Release configuration.
  --build-dir DIR     Use DIR for the CMake/Ninja build directory.
  --target TARGET     Build TARGET instead of dragonfly.
  --jobs N             Pass N parallel jobs to Ninja (default: ${JOBS}).
  --no-index           Skip ccls index generation.
  -h, --help           Show this help.

Any arguments after -- are passed to helio/blaze.sh as additional CMake options.
Environment variables BUILD_DIR and JOBS provide the corresponding defaults.
EOF
}

while (($#)); do
  case "$1" in
    --debug)
      BUILD_TYPE="Debug"
      RELEASE_FLAG=0
      if (( ! BUILD_DIR_EXPLICIT )); then
        BUILD_DIR="${ROOT_DIR}/build-dbg"
      fi
      shift
      ;;
    --release)
      BUILD_TYPE="Release"
      RELEASE_FLAG=1
      if (( ! BUILD_DIR_EXPLICIT )); then
        BUILD_DIR="${ROOT_DIR}/build-opt"
      fi
      shift
      ;;
    --build-dir)
      if (($# < 2)); then
        echo "error: --build-dir requires a path" >&2
        exit 2
      fi
      BUILD_DIR="$2"
      if [[ "${BUILD_DIR}" != /* ]]; then
        BUILD_DIR="${ROOT_DIR}/${BUILD_DIR}"
      fi
      BUILD_DIR_EXPLICIT=1
      shift 2
      ;;
    --target)
      if (($# < 2)); then
        echo "error: --target requires a target name" >&2
        exit 2
      fi
      TARGET="$2"
      shift 2
      ;;
    --jobs)
      if (($# < 2)) || ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
        echo "error: --jobs requires a positive integer" >&2
        exit 2
      fi
      JOBS="$2"
      shift 2
      ;;
    --no-index)
      GENERATE_INDEX=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      BLAZE_EXTRA+=("$@")
      break
      ;;
    -D*)
      BLAZE_EXTRA+=("$1")
      shift
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

BLAZE_ARGS=("-build-dir" "${BUILD_DIR}" "${BLAZE_EXTRA[@]}")
if (( RELEASE_FLAG )); then
  BLAZE_ARGS+=("-release")
fi

if ! command -v cmake >/dev/null 2>&1; then
  echo "error: cmake is required but was not found in PATH" >&2
  exit 1
fi
if ! command -v ninja >/dev/null 2>&1; then
  echo "error: ninja is required but was not found in PATH" >&2
  exit 1
fi
if (( GENERATE_INDEX )) && ! command -v ccls >/dev/null 2>&1; then
  echo "error: ccls is required for index generation but was not found in PATH" >&2
  exit 1
fi

# The repository is often built with a newer GCC toolchain than the host's
# default runtime loader. Make helper binaries (for example reflex) use the
# same libstdc++ that the selected compiler uses.
COMPILER_RUNTIME_DIR="$(dirname "$(g++ -print-file-name=libstdc++.so.6)")"
if [[ -f "${COMPILER_RUNTIME_DIR}/libstdc++.so.6" ]]; then
  if [[ -n "${LD_LIBRARY_PATH:-}" ]]; then
    export LD_LIBRARY_PATH="${COMPILER_RUNTIME_DIR}:${LD_LIBRARY_PATH}"
  else
    export LD_LIBRARY_PATH="${COMPILER_RUNTIME_DIR}"
  fi
fi

echo "==> Configuring ${BUILD_TYPE} build in ${BUILD_DIR}"
(
  cd "${ROOT_DIR}"
  ./helio/blaze.sh "${BLAZE_ARGS[@]}"
)

echo "==> Building ${TARGET} with ${JOBS} parallel jobs"
ninja -C "${BUILD_DIR}" -j "${JOBS}" "${TARGET}"

if (( GENERATE_INDEX )); then
  COMPILE_COMMANDS="${BUILD_DIR}/compile_commands.json"
  if [[ ! -f "${COMPILE_COMMANDS}" ]]; then
    echo "error: ${COMPILE_COMMANDS} was not generated; cannot index with ccls" >&2
    exit 1
  fi

  echo "==> Generating ccls index from ${COMPILE_COMMANDS}"
  CCLS_INIT="{\"compilationDatabaseDirectory\":\"${BUILD_DIR}\"}"
  ccls -v=-1 --index="${ROOT_DIR}" --init="${CCLS_INIT}"
fi

echo "==> Done"
echo "Binary: ${BUILD_DIR}/${TARGET}"
if (( GENERATE_INDEX )); then
  echo "ccls cache: ${ROOT_DIR}/.ccls-cache"
fi
