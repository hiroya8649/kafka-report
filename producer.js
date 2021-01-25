const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
});
const consumer = kafka.consumer({ groupId: "test-group" });
const r = producer.events.REQUEST;
const removeListener = producer.on(r, (req) =>
  // console.log(r.id, r.type, r.payload.apiName)
  console.log(req)
);

const run = async () => {
  // Producing
  await producer.connect();
  const start = new Date();
  const promises = [];
  for (let i = 0; i < 5; i++) {
    // console.log(i);
    const promise = producer.send({
      topic: "first_topic",
      messages: [{ value: `Hello! ${start.getTime()}-${i}` }],
    });
    promises.push(promise);
  }
  const resolved = await Promise.all(promises);
  console.log(resolved);
  const end = new Date();
  console.log(`Cost: ${end - start}ms`);
};

run().catch(console.error);
