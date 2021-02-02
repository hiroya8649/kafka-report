const fastify = require("fastify")({
  logger: false,
});
const { Kafka } = require("kafkajs");
const MongoClient = require("mongodb").MongoClient;

const url = "mongodb://localhost:27017/test";
let client;
let mongo;
let alreadyInDb = 0;

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer({
  maxInFlightRequests: 5,
});

let producedCount = 0;

fastify.get("/", async (request, reply) => {
  await producer.send({
    topic: "benchmark-json",
    messages: [
      {
        value: JSON.stringify({ value: "BENCHMARK".repeat(100) }),
      },
    ],
  });
  producedCount++;
  if (producedCount % 1000 === 0) {
    const dbCount = await mongo.count();
    console.log(`produced: ${producedCount}, db: ${dbCount - alreadyInDb}`);
  }
  return "done";
});

fastify.get("/no-await", async (request, reply) => {
  return "done";
});

const start = async () => {
  try {
    await producer.connect();
    await fastify.listen(3000);
    client = await MongoClient.connect(url, {
      auth: { user: "user", password: "user" },
    });
    mongo = await client.db("test").collection("benchmark");
    alreadyInDb = await mongo.count();
    console.log("server started");
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};
start();
