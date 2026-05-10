import "reflect-metadata";

import { KAFKA_CONSUMER } from "../decorators/kafka-consumer.decorator";

interface ConsumerMetadata {
  topic: string;
  handler: Function;
}

class KafkaConsumerRegistry {
  private consumers: ConsumerMetadata[] = [];

  register(instance: any) {
    const prototype = Object.getPrototypeOf(instance);

    const methods = Object.getOwnPropertyNames(prototype);

    for (const method of methods) {
      const handler = instance[method];

      const topic = Reflect.getMetadata(KAFKA_CONSUMER, handler);

      if (!topic) {
        continue;
      }

      this.consumers.push({
        topic,
        handler: handler.bind(instance),
      });

      console.log(`Registered Kafka Consumer: ${topic}`);
    }
  }

  async execute(topic: string, payload: any) {
    const consumers = this.consumers.filter(
      (consumer) => consumer.topic === topic,
    );

    for (const consumer of consumers) {
      await consumer.handler(payload);
    }
  }
}

export const kafkaConsumerRegistry = new KafkaConsumerRegistry();
