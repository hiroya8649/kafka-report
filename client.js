const { Kafka } = require("kafkajs");
const MongoClient = require("mongodb").MongoClient;

const url = "mongodb://localhost:27017/test";
let client;
let mongo;

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "test-group",
  maxWaitTimeInMs: 500,
  minBytes: 500000,
});
let msgCount = 0;
const run = async () => {
  client = await MongoClient.connect(url, {
    auth: { user: "user", password: "user" },
  });
  mongo = client.db("test");
  await consumer.connect();
  await consumer.subscribe({ topic: "benchmark-json", fromBeginning: true });

  await consumer.run({
    eachBatch: async ({ batch: { topic, partition, messages } }) => {
      // console.log(messages.length);
      msgCount += messages.length;
      const docs = await mongo.collection("benchmark").count();
      console.log(
        `Got ${messages.length}, total: ${msgCount}, mongodb: ${docs}`
      );
      // messages.forEach((message) => {
      //   console.log({
      //     partition,
      //     offset: message.offset,
      //     value: message.value.toString(),
      //   });
      // });
    },
  });
};

run().catch(console.error);
