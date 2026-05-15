// Your current index.ts
import app from "./app";
import {
  KAFKA_REPLY_TOPICS_ARRAY,
  KAFKA_REQUEST_TOPICS_ARRAY,
} from "./kafka/kafka.constants";
import { kafkaConsumerManager } from "./kafka/kafka.consumer";
import { kafkaProducer } from "./kafka/kafka.producer";
import { registerKafkaConsumers } from "./kafka/register-consumers";

const PORT = 3000;

async function bootstrap() {
  await kafkaProducer.connect();
  registerKafkaConsumers();

  await kafkaConsumerManager.connect();

  await Promise.all([
    ...KAFKA_REQUEST_TOPICS_ARRAY.map((topic) =>
      kafkaConsumerManager.subscribe(topic),
    ),
    ...KAFKA_REPLY_TOPICS_ARRAY.map((topic) =>
      kafkaConsumerManager.subscribe(topic),
    ),
  ]);
  await kafkaConsumerManager.run();

  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}

bootstrap().catch(console.error);