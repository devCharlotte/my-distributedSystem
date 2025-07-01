#!/usr/bin/env bash
set -u
export BIN=$(cd "${1:?usage: run.sh BIN_DIR}" && pwd)
cd "$(dirname "$0")"
pass=0
fail=0
for t in [0-9]*.sh; do
	echo "== $t"
	if bash "$t"; then
		pass=$((pass + 1))
	else
		fail=$((fail + 1))
	fi
done
echo "passed $pass, failed $fail"
[ "$fail" -eq 0 ]
