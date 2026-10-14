set -uo pipefail
export HOME=/tmp

cp /opt/flink/usrlib/*.jar /opt/flink/lib/

/opt/flink/bin/start-cluster.sh

until (echo > /dev/tcp/localhost/8081) 2>/dev/null; do sleep 2; done
echo "Flink REST is up, submitting streaming SQL job..."
sleep 10

/opt/flink/bin/sql-client.sh -f /opt/flink/sql/job.sql
echo "SQL submission finished with exit code $?"

exec tail -F /opt/flink/log/*.log
