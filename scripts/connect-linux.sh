#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: bash scripts/connect-linux.sh [--state DIRECTORY] [--config FILE | --publish-config FILE] [--once] [--show-route] [--p2p-only] [--direct IP:PORT]'
}

client_home="${NICE_SYNC_HOME:-$HOME}"
state="$client_home/.nice-sync"
once=false
config=
config_option=
route_args=()
while (($#)); do
    case "$1" in
        --state)
            if [[ -z "${2:-}" ]]; then
                usage >&2
                exit 2
            fi
            state=$2
            shift 2
            ;;
        --once)
            once=true
            shift
            ;;
        --config|--publish-config)
            if [[ -z "${2:-}" || -n "$config_option" ]]; then
                usage >&2
                exit 2
            fi
            config_option=$1
            config=$2
            if [[ "$config" != /* ]]; then config="$PWD/$config"; fi
            shift 2
            ;;
        --direct)
            if [[ -z "${2:-}" ]]; then
                usage >&2
                exit 2
            fi
            route_args+=(--direct "$2")
            shift 2
            ;;
        --show-route|--p2p-only)
            route_args+=("$1")
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            exit 2
            ;;
    esac
done

if ! command -v cargo >/dev/null && [[ -x "$HOME/.cargo/bin/cargo" ]]; then
    export PATH="$HOME/.cargo/bin:$PATH"
fi
if ! command -v cargo >/dev/null || ! command -v cc >/dev/null; then
    printf '%s\n' 'Install Rust with Cargo and a C compiler (cc), then run this script again.' >&2
    exit 1
fi

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
install_root="$client_home/.local"
binary="$install_root/bin/nice-sync"
mkdir -p -- "$state"
state=$(cd -- "$state" && pwd)

cargo install --path "$repo_dir" --locked --force --root "$install_root"

cd -- "$client_home"
if [[ ! -f "$state/joined.json" ]]; then
    "$binary" --state "$state" join
fi

sync_args=(--state "$state" sync)
if [[ -n "$config" ]]; then
    sync_args+=("$config_option" "$config")
else
    sync_args+=(--follow-config)
fi
if [[ "$once" == false ]]; then
    sync_args+=(--every 10)
fi
exec "$binary" "${sync_args[@]}" "${route_args[@]}"
