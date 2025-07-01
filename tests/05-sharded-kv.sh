source ./lib.sh

L=""
for i in 0 1 2 3; do
	start "s$i" kvserver -p $((BASE + i))
	L="$L${L:+,}127.0.0.1:$((BASE + i))"
done
for i in 0 1 2 3; do
	wait_up $((BASE + i)) || exit 1
done
sk() { "$BIN/shardkv" -s "$L" "$@"; }

check "put routes to one shard" eq "$(sk put hello world)" OK
check "get finds it" eq "$(sk get hello)" world
owner=$(sk where hello)
check "value lives only on its owner" bash -c "
	for i in 0 1 2 3; do
		a=127.0.0.1:\$(($BASE + i))
		v=\$('$BIN/kvcli' -s \$a get hello)
		if [ \$a = $owner ]; then [ \"\$v\" = world ] || exit 1; else [ \"\$v\" = NOT_FOUND ] || exit 1; fi
	done"
check "delete through router" eq "$(sk del hello; sk get hello)" "$(printf 'OK\nNOT_FOUND')"
check "ring routing workload: reads match writes, nothing misplaced" sk -m ring -t 8 -n 4000 -k 4000 -r 50 run

for i in 0 1 2 3; do
	pid_var=PID_s$i
	stop_pid "${!pid_var}"
	start "m$i" kvserver -p $((BASE + i))
done
for i in 0 1 2 3; do
	wait_up $((BASE + i)) || exit 1
done
check "mod routing workload" sk -m mod -t 8 -n 4000 -k 4000 -r 50 run
"$BIN/shardkv" analyze 6 50000 > "$TMP/analyze.out"
cat "$TMP/analyze.out"
check "adding a shard moves about 1/n keys on the ring" \
	awk '$1 == 6 { gsub("%", ""); exit !($3 < 25 && $2 > 75) }' "$TMP/analyze.out"
for i in 0 1 2 3; do
	pid_var=PID_m$i
	check "shard $i shutdown" stop_pid "${!pid_var}"
done
finish
