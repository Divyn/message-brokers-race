# Runs on the broker box (exp-mb-broker).
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -yq --no-install-recommends docker.io docker-cli curl ca-certificates
systemctl enable --now docker
for img in apache/kafka:latest redpandadata/redpanda:latest nats:latest rabbitmq:4 apache/iggy:latest; do
  docker pull -q $img
done
docker images --digests --format '{{.Repository}}:{{.Tag}} {{.Digest}}' > /root/mb/images.txt
echo BROKER_READY > /root/mb/BROKER_STATUS
