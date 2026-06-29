To setup the application run
```sh
docker compose up
```

In a new shell tab start the kafka shell

```sh
docker exec -it --workdir /opt/kafka/bin -it exchange_broker sh
```

Read events sent to the consumer
```sh
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic crypto_exchange_trades --from-beginning

```