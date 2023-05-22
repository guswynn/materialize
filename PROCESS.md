## Setup

- 2 scratch instances, one with named security group.
- name them accordingly
- Instance types??

### Helper scratch instance
```
docker run --name=cockroach -d -p 26257:26257 -p 26258:8080 cockroachdb/cockroach:v22.2.0 start-single-node --insecure
```

```
docker run -d --pull=always --name=redpanda-1 --rm -p 8081:8081 -p 8082:8082 -p 9092:9092 -p 9644:9644 vectorized/redpanda:v23.1.9 redpanda start --overprovisioned --smp 1  --memory 1G --reserve-memory 0M --node-id 0 --check=false --set --advertise-kafka-addr=<private_ip_of_other_instance>:9092
```

And add "All traffic allowed" rule for the `scratch-security-group` sec group


## Testing

```
bin/environmentd --reset --postgres='postgres://root@<private_ip_of_other_instance>:26257/materialize' -- --all-features --orchestrator-process-scratch-directory=/home/ubuntu/scratch-directory
```

```
cargo run --bin testdrive -- --kafka-addr=<private_ip_of_other_instance>:9092 rocksdb.td  --schema-registry-url=<private_ip_of_other_instance>:8081
```

## Running for real

```
sudo apt-get install kafkacat
```

```
docker exec -it redpanda-1 rpk topic create <topic-name> --brokers=localhost:9092
```

```
python3 gen.py 10 10 | kafkacat -b localhost:9092 -t test-small
```

## Evaluation

On main node
- Rough estimate of time to run for getting ALL the data out of the source.
  - Manually watched
- `sar -r -h 1 -o run_name.stats`
