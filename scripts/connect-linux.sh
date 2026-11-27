#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: bash scripts/connect-linux.sh [--state DIRECTORY] [--once] [--show-route] [--direct IP:PORT]'
}

client_home="${NICE_SYNC_HOME:-$HOME}"
state="$client_home/.nice-sync"
once=false
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
        --direct)
            if [[ -z "${2:-}" ]]; then
                usage >&2
                exit 2
            fi
            route_args+=(--direct "$2")
            shift 2
            ;;
        --show-route)
            route_args+=(--show-route)
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
mkdir -p -- "$state" "$client_home/Downloads/cadcraft"
state=$(cd -- "$state" && pwd)

cargo install --path "$repo_dir" --locked --force --root "$install_root"

cd -- "$client_home"
if [[ ! -f "$state/joined.json" ]]; then
    "$binary" --state "$state" join
fi

sync_args=(--state "$state" sync --follow-config)
if [[ "$once" == false ]]; then
    sync_args+=(--every 10)
fi
exec "$binary" "${sync_args[@]}" "${route_args[@]}"
