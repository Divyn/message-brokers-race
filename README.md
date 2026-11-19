# Kafka vs Redpanda vs NATS vs RabbitMQ vs Iggy on the same server

Each broker alone in Docker with its quickstart settings, on its own server. A second, identical server sends 200-byte messages at a fixed rate for 10 s with each broker's official Python client and waits for the broker to confirm each one: Kafka and Redpanda `acks=all`; NATS JetStream publish ack; RabbitMQ publisher confirms on a durable queue with persistent messages; Iggy `send_messages` returning. Rates step 1k, 2k, 5k, 10k, 20k, 50k a second, stopping at the first that fails. A rate passes when at least 95% of its messages are confirmed and the p99 confirm time is at most 100 ms.

confluent-kafka batches messages on its own; the NATS, RabbitMQ and Iggy clients are driven with one awaited publish per message, many in flight. So the score is each broker together with its Python client.

Score: highest rate passed, median of 3 runs.

| Broker | Messages/s | Runs |
|---|---|---|
| Redpanda | 0 | failed at 1,000/s: p99 105 to 107 ms |
| RabbitMQ 4 | 2,000 | 2000 1000 2000 |
| Iggy | 2,000 | 1000 2000 2000 |
| NATS JetStream | 5,000 | 5000 5000 5000 |
| Kafka (KRaft) | 50,000 | passed every rate tested |

Run 2026-11-19 on two Vultr `vc2-2c-4gb` servers (2 vCPU Intel Skylake, 3.9 GB RAM), Seattle, Debian 13, Docker 26.1.5, `latest` images pulled that day. Clients: confluent-kafka 2.16.2, nats-py 2.17.0, aio-pika 10.2.3, apache-iggy 0.10.0.

- Iggy failed to start in the first run: its new release refuses to bind `0.0.0.0` without an advertised address, and its client now numbers partitions from 0. Fixed and run on a second identical pair, with Kafka run again beside it as a control (50,000 again). `results/iggy-rerun/`.
- Redpanda (not ranked, `results/redpanda-check.txt`): at 1,000/s on quickstart settings, p50 4.1 ms and p99 105 ms; 1.15% of messages waited about 100 ms. Its `raft_replica_max_flush_delay_ms` is 100. Set to 10, the same load gave p99 6.6 ms. The ranking keeps the defaults.

- `mb/`: the broker server. `broker_setup.sh` installs Docker and pulls the images; `up.sh <name> <ip>` starts one broker.
- `setup.sh`, `mb_race.py`: the client server.

## Run it yourself

1. Make two Debian 13 servers in one region (2 vCPU, 4 GB each).
2. Broker server: copy `mb/` to `/root/mb` and run `cd /root/mb && sh broker_setup.sh`. It writes `BROKER_READY` to `/root/mb/BROKER_STATUS`. Open ports 9092, 4222, 5672 and 8090 to the client server.
3. Client server: give it SSH access to the broker server as root (it starts each broker over SSH), copy this folder plus `run.sh` to `/root/exp`, write the broker server's IP into `/root/exp/BROKER_IP`, and run `cd /root/exp && sh run.sh`.
4. The run waits for a `GO` file holding the brokers to race: `echo "kafka redpanda nats rabbitmq iggy" > /root/exp/GO`. When `STATUS` says DONE, the results are in `results.csv` and `run.log`.
