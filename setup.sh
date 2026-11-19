# Runs on the client box (exp-mb-client). Waits for the broker box to finish pulling images.
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -yq --no-install-recommends python3-venv openssh-client
python3 -m venv /root/v
/root/v/bin/pip install -q confluent-kafka nats-py aio-pika apache-iggy
/root/v/bin/pip freeze > /root/exp/versions.txt
B=$(cat /root/exp/BROKER_IP)
for i in $(seq 1 120); do
  st=$(ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 root@$B cat /root/mb/BROKER_STATUS 2>/dev/null || true)
  [ "$st" = "BROKER_READY" ] && exit 0
  sleep 15
done
echo "broker box never reached BROKER_READY"; exit 1
