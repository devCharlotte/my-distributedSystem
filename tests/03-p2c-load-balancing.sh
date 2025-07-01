source ./lib.sh

L=""
for i in 0 1 2 3; do
	d=200
	[ "$i" -eq 3 ] && d=2000
	start "b$i" kvserver -p $((BASE + i)) -t 1 -d "$d"
	L="$L${L:+,}127.0.0.1:$((BASE + i))"
done
for i in 0 1 2 3; do
	wait_up $((BASE + i)) || exit 1
done

share() {
	awk -v a="127.0.0.1:$2" '$1 == a { gsub(/[()%]/, "", $3); print $3 }' "$1"
}

check "round robin is exact" bash -c "'$BIN/lbclient' -b $L -p rr -t 8 -n 200 | grep -q 'max/min 1.00'"
"$BIN/lbclient" -b "$L" -p p2c-count -t 8 -n 200 > "$TMP/count.out"
check "p2c on request count stays within 2%" awk '/max\/min/ { exit !($2 <= 1.02) }' "$TMP/count.out"
"$BIN/lbclient" -b "$L" -p p2c -t 16 -n 300 > "$TMP/p2c.out"
"$BIN/lbclient" -b "$L" -p random -t 16 -n 300 > "$TMP/random.out"
cat "$TMP/p2c.out" "$TMP/random.out"
check "p2c on in-flight sends under 15% to the slow backend" \
	awk -v s="$(share "$TMP/p2c.out" $((BASE + 3)))" 'BEGIN { exit !(s < 15) }'
check "random keeps sending about a quarter to the slow backend" \
	awk -v s="$(share "$TMP/random.out" $((BASE + 3)))" 'BEGIN { exit !(s > 20) }'
check "server side counts match" bash -c "
	n=0
	for i in 0 1 2 3; do
		c=\$('$BIN/kvcli' -s 127.0.0.1:\$(($BASE + i)) stats | sed 's/.*get=\([0-9]*\).*/\1/')
		n=\$((n + c))
	done
	[ \$n -eq \$((1600 + 1600 + 4800 + 4800)) ]"
check "simulation: p2c beats random on max load" bash -c "
	'$BIN/lbclient' -B 100 -S 100000 > $TMP/sim
	r=\$(awk '/^random/ { sub(\"max-avg=\", \"\", \$6); print \$6 }' $TMP/sim)
	p=\$(awk '/^p2c-count/ { sub(\"max-avg=\", \"\", \$6); print \$6 }' $TMP/sim)
	awk -v r=\$r -v p=\$p 'BEGIN { exit !(p < r / 4) }'"
for i in 0 1 2 3; do
	pid_var=PID_b$i
	check "backend $i shutdown" stop_pid "${!pid_var}"
done
finish
