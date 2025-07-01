source ./lib.sh

C=$BASE
S=$((BASE + 1))
start cache cacheserver -p "$C" -c 2000
start store kvserver -p "$S"
wait_up "$C" || exit 1
wait_up "$S" || exit 1
CA=127.0.0.1:$C
SA=127.0.0.1:$S

check "first read misses" bash -c "'$BIN/kvcli' -s $CA get k1 | grep -q MISS"
check "per-thread keys: no stale reads, cache matches storage" \
	"$BIN/lacclient" -c "$CA" -s "$SA" -t 4 -n 5000 -k 5000 -z 0.99 -r 90
check "shared keys with leases: no stale entries left" \
	"$BIN/lacclient" -c "$CA" -s "$SA" -t 8 -n 2000 -k 200 -z 1.2 -r 70 -S -D 200
check "eviction keeps size at capacity" bash -c "'$BIN/kvcli' -s $CA stats | grep -q 'size=2000 cap=2000'"
lease=$("$BIN/kvcli" -s "$CA" get fresh | awk '{print $2}')
"$BIN/kvcli" -s "$CA" del fresh >/dev/null
check "fill after invalidation is rejected" eq "$("$BIN/kvcli" -s "$CA" -a "$lease" put fresh old)" STALE
lease=$("$BIN/kvcli" -s "$CA" get fresh | awk '{print $2}')
check "fill with current lease is accepted" eq "$("$BIN/kvcli" -s "$CA" -a "$lease" put fresh new)" OK
check "filled value is served" eq "$("$BIN/kvcli" -s "$CA" get fresh)" new
check "reused lease is rejected" eq "$("$BIN/kvcli" -s "$CA" -a "$lease" put fresh again)" STALE
check "clean shutdown" stop_pid "$PID_cache"
check "storage shutdown" stop_pid "$PID_store"
finish
