source ./lib.sh

P=$BASE
start srv kvserver -p "$P" -t 4
wait_up "$P" || exit 1
S=127.0.0.1:$P

check "get on missing key" eq "$("$BIN/kvcli" -s "$S" get nokey; echo $?)" "$(printf 'NOT_FOUND\n1')"
check "put" eq "$("$BIN/kvcli" -s "$S" put alpha "hello world")" OK
check "get after put" eq "$("$BIN/kvcli" -s "$S" get alpha)" "hello world"
check "overwrite" eq "$("$BIN/kvcli" -s "$S" put alpha second >/dev/null; "$BIN/kvcli" -s "$S" get alpha)" second
check "delete" eq "$("$BIN/kvcli" -s "$S" del alpha)" OK
check "get after delete" eq "$("$BIN/kvcli" -s "$S" get alpha)" NOT_FOUND
long=$(head -c 1024 /dev/zero | tr '\0' x)
check "1024 byte value" eq "$("$BIN/kvcli" -s "$S" put big "$long" >/dev/null; "$BIN/kvcli" -s "$S" get big)" "$long"
check "oversized value rejected" eq "$("$BIN/kvcli" -s "$S" put big "${long}y" 2>&1)" "value must be at most 1024 bytes"
check "garbage datagram ignored" bash -c "printf 'junk' > /dev/udp/127.0.0.1/$P; '$BIN/kvcli' -s '$S' stats | grep -q 'bad=1'"
check "concurrent bench, keys never mixed" "$BIN/kvbench" -s "$S" -c 8 -n 5000 -k 2000 -r 70 -v 64
check "clean shutdown" stop_pid "$PID_srv"
finish
