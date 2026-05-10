import { Consumer, EachMessagePayload } from "kafkajs";

import { kafka } from "./kafka";

import { KAFKA_GROUP_ID } from "./kafka.constants";

import { kafkaConsumerRegistry } from "./registry/kafka-consumer.registry";

export class KafkaConsumerManager {
  private consumer: Consumer;

  constructor() {
    this.consumer = kafka.consumer({
      groupId: KAFKA_GROUP_ID,
    });
  }

  async connect() {
    await this.consumer.connect();

    console.log("Kafka Consumer Connected");
  }

  async subscribe(topic: string) {
    console.log(`Subscribing to topic: ${topic}`);
    await this.consumer.subscribe({
      topic,
      fromBeginning: true,
    });
  }

  async run() {
    await this.consumer.run({
      eachMessage: async (payload) => {
        await this.handleMessage(payload);
      },
    });
  }

  private async handleMessage(payload: EachMessagePayload) {
    const { topic } = payload;

    try {
      await kafkaConsumerRegistry.execute(topic, payload);
    } catch (error) {
      console.error("Kafka Consumer Error", error);
    }
  }
}

export const kafkaConsumerManager = new KafkaConsumerManager();
