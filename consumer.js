const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "test-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "first_topic", fromBeginning: true });

  await consumer.run({
    eachBatch: async ({ batch: { topic, partition, messages } }) => {
      // console.log(messages.length);
      messages.forEach((message) => {
        console.log({
          partition,
          offset: message.offset,
          value: message.value.toString(),
        });
      });
    },
  });
};

run().catch(console.error);
