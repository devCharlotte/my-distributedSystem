source ./lib.sh

N="127.0.0.1:$BASE,127.0.0.1:$((BASE + 1)),127.0.0.1:$((BASE + 2))"
for i in 0 1 2; do
	start "n$i" pbnode -i "$i" -n "$N"
done
sleep 1
pb() { "$BIN/pbclient" -n "$N" "$@"; }
role_of() { pb status | awk -v a="127.0.0.1:$1" '$1 == a { print $3 }'; }
keys_of() { pb status | awk -v a="127.0.0.1:$1" '$1 == a { print $6 }'; }

check "node 0 starts as primary" eq "$(role_of $BASE)" role=primary
check "write 300 keys" pb fill 300 a
check "every replica has them" eq "$(keys_of $BASE) $(keys_of $((BASE + 1))) $(keys_of $((BASE + 2)))" "keys=300 keys=300 keys=300"
check "read back" pb check 300 a
check "backup refuses client writes" eq "$("$BIN/kvcli" -s 127.0.0.1:$((BASE + 1)) put x y)" "NOT_PRIMARY 1"
pb del a000007 >/dev/null
check "delete is replicated" eq "$(keys_of $BASE) $(keys_of $((BASE + 1))) $(keys_of $((BASE + 2)))" "keys=299 keys=299 keys=299"
pb put a000007 a-value-7 >/dev/null

crash "$PID_n0"
check "writes continue after primary crash" pb fill 200 b
check "node 1 took over" eq "$(role_of $((BASE + 1)))" role=primary
check "no acknowledged write lost" pb check 300 a
check "writes during failover kept" pb check 200 b
check "remaining backup in sync" eq "$(keys_of $((BASE + 2)))" keys=500

start n0b pbnode -i 0 -n "$N" -j
sleep 1.5
check "restarted node rejoins as backup" eq "$(role_of $BASE)" role=backup
check "rejoined node got full snapshot" eq "$(keys_of $BASE)" keys=500

crash "$PID_n1"
check "second failover" pb fill 100 c
check "data survives two failovers" bash -c "'$BIN/pbclient' -n $N check 300 a && '$BIN/pbclient' -n $N check 200 b && '$BIN/pbclient' -n $N check 100 c"

kill -STOP "$PID_n2"
sleep 1.5
check "paused primary is replaced" eq "$(role_of $BASE)" role=primary
pb fill 50 d >/dev/null
kill -CONT "$PID_n2"
"$BIN/kvcli" -s 127.0.0.1:$((BASE + 2)) put zombie write > "$TMP/zombie.out"
check "old primary cannot accept writes after resuming" grep -q NOT_PRIMARY "$TMP/zombie.out"
sleep 1
check "old primary steps down" eq "$(role_of $((BASE + 2)))" role=backup
check "old primary resynced" eq "$(keys_of $((BASE + 2)))" keys=650
check "zombie write is nowhere" eq "$(pb get zombie)" NOT_FOUND

kill -STOP "$PID_n2"
pb fill 30 e >/dev/null
sleep 1
kill -CONT "$PID_n2"
sleep 1.5
check "paused backup does not take over when it wakes up" eq "$(role_of $BASE) $(role_of $((BASE + 2)))" "role=primary role=backup"
check "paused backup is resynced" eq "$(keys_of $((BASE + 2)))" keys=680

start n1b pbnode -i 1 -n "$N" -j
sleep 1.5
kill -STOP "$PID_n1b"
pb fill 40 f >/dev/null
sleep 0.8
crash "$PID_n0b"
kill -CONT "$PID_n1b"
check "writes go on after losing the primary" pb fill 10 g
check "the backup that missed writes did not win" eq "$(role_of $((BASE + 2)))" role=primary
check "nothing acknowledged was lost" bash -c "'$BIN/pbclient' -n $N check 40 f && '$BIN/pbclient' -n $N check 30 e && '$BIN/pbclient' -n $N check 300 a"
sleep 1
check "lagging node caught up" eq "$(keys_of $((BASE + 1)))" keys=730

crash "$PID_n2"
start n0c pbnode -i 0 -n "$N" -j -b 50 -f 200
sleep 2
check "empty node defers to the node holding the data" grep -q "node 1 has newer data" "$TMP/n0c.log"
check "node with the data takes over" eq "$(role_of $((BASE + 1)))" role=primary
check "data intact after an empty node joined during failover" bash -c "'$BIN/pbclient' -n $N check 300 a && '$BIN/pbclient' -n $N check 40 f"
check "empty node got the snapshot" eq "$(keys_of $BASE)" keys=730

check "node 0 shutdown" stop_pid "$PID_n0c"
check "node 1 shutdown" stop_pid "$PID_n1b"
finish
