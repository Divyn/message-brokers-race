# Start one broker with its quickstart settings, after removing the last one. Usage: sh up.sh <name> <public ip>
N=$1; IP=$2
docker rm -f $(docker ps -aq) >/dev/null 2>&1; sleep 1
case $N in
  kafka) docker run -d --name kafka --network host \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$IP:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_NUM_PARTITIONS=1 apache/kafka:latest ; PORT=9092 ;;
  redpanda) docker run -d --name redpanda --network host redpandadata/redpanda:latest \
    redpanda start --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr $IP:9092 \
    --mode dev-container --smp 1 --default-log-level=warn ; PORT=9092 ;;
  nats) docker run -d --name nats --network host nats:latest -js ; PORT=4222 ;;
  rabbitmq) docker run -d --name rabbitmq --network host \
    -e RABBITMQ_DEFAULT_USER=bench -e RABBITMQ_DEFAULT_PASS=bench rabbitmq:4 ; PORT=5672 ;;
  iggy) docker run -d --name iggy --network host --security-opt seccomp=unconfined --ulimit memlock=-1:-1 \
    -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy -e IGGY_TCP_ADDRESS=$IP:8090 \
    apache/iggy:latest ; PORT=8090 ;;
esac
for i in $(seq 1 90); do
  if (exec 3<>/dev/tcp/$IP/$PORT) 2>/dev/null; then sleep 5; echo "$N ready port=$PORT"; exit 0; fi
  sleep 1
done
echo "$N FAILED to start"; docker logs --tail 8 $N 2>&1; exit 1
