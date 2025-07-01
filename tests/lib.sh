BIN=${BIN:?}
TMP=$(mktemp -d)
PIDS=()
FAILS=0
BASE=${PORT_BASE:-$((20000 + RANDOM % 20000))}

cleanup() {
	for p in "${PIDS[@]}"; do
		kill "$p" 2>/dev/null
	done
	for p in "${PIDS[@]}"; do
		wait "$p" 2>/dev/null
	done
	if [ "$FAILS" -gt 0 ] || [ -n "${KEEP_LOGS:-}" ]; then
		echo "  logs kept in $TMP"
	else
		rm -rf "$TMP"
	fi
}
trap cleanup EXIT

start() {
	local name=$1
	shift
	"$BIN/$@" >"$TMP/$name.log" 2>&1 &
	PIDS+=($!)
	eval "PID_$name=$!"
}

wait_up() {
	local port=$1
	for _ in $(seq 50); do
		"$BIN/kvcli" -s "127.0.0.1:$port" stats >/dev/null 2>&1 && return 0
		sleep 0.1
	done
	echo "server on $port did not come up"
	return 1
}

crash() {
	kill -9 "$1" 2>/dev/null
	wait "$1" 2>/dev/null
}

stop_pid() {
	kill -INT "$1" 2>/dev/null
	wait "$1"
}

check() {
	local what=$1
	shift
	if "$@"; then
		echo "  ok    $what"
	else
		echo "  FAIL  $what"
		FAILS=$((FAILS + 1))
	fi
}

eq() {
	[ "$1" = "$2" ] || { echo "    expected [$2] got [$1]"; return 1; }
}

finish() {
	for f in "$TMP"/*.log; do
		if grep -qE "ERROR: (Address|Thread|Leak)Sanitizer|runtime error|WARNING: ThreadSanitizer" "$f"; then
			echo "  FAIL  sanitizer report in $(basename "$f")"
			sed -n '1,40p' "$f"
			FAILS=$((FAILS + 1))
		fi
	done
	[ "$FAILS" -eq 0 ]
}
