#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target="${1:?usage: $0 <clean-upstream-checkout>}"
target="$(cd "$target" && pwd)"

[[ -z "$(git -C "$target" status --porcelain)" ]] || {
  echo "FATAL: target checkout is not clean" >&2
  exit 1
}

expected_base="$(tr -d '\n' <"$root/UPSTREAM")"
actual_base="$(git -C "$target" rev-parse HEAD)"
[[ "$actual_base" == "$expected_base" ]] || {
  echo "FATAL: target HEAD $actual_base does not equal required upstream $expected_base" >&2
  exit 1
}

patches=()
while read -r patch source_commit; do
  [[ -n "$patch" && "${patch:0:1}" != "#" ]] || continue
  [[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || {
    echo "FATAL: invalid source commit for $patch" >&2
    exit 1
  }
  [[ -f "$root/patches/$patch" ]] || {
    echo "FATAL: missing patch: $patch" >&2
    exit 1
  }
  patches+=("$root/patches/$patch")
done <"$root/SERIES"

[[ ${#patches[@]} -gt 0 ]] || {
  echo "FATAL: SERIES contains no patches" >&2
  exit 1
}

git -C "$target" am --3way "${patches[@]}"
expected_tree="$(tr -d '\n' <"$root/RESULT_TREE")"
actual_tree="$(git -C "$target" rev-parse 'HEAD^{tree}')"
[[ "$actual_tree" == "$expected_tree" ]] || {
  echo "FATAL: applied tree $actual_tree does not equal expected tree $expected_tree" >&2
  exit 1
}

echo "applied ${#patches[@]} patches; result tree $actual_tree"
