TRY_LOOP=10
wait_for_port()
{
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

wait_for_port "elasticsearch" "es" "9200"

case "$1" in
  test)
    echo "starting tests..."
    # python3 -m unittest
    curl -X DELETE "http://es:9200/paymentfactor"
    python3 datauploader/loadData.py --host es --index paymentfactor tests/files/2019-08-10.paymentfactor
    curl -X GET "http://es:9200/paymentfactor/_search"
  ;;
esac