import { Producer } from "kafkajs";

import { kafka } from "./kafka";

export class KafkaProducer {
  private producer: Producer;

  constructor() {
    this.producer = kafka.producer();
  }

  async connect() {
    await this.producer.connect();

    console.log("Kafka Producer Connected");
  }

  async emit<T>(topic: string, payload: T) {
    await this.producer.send({
      topic,
      messages: [
        {
          value: JSON.stringify({
            data: payload,
          }),
        },
      ],
    });
  }

  async sendRaw(payload: any) {
    await this.producer.send(payload);
  }

  getInstance() {
    return this.producer;
  }
}

export const kafkaProducer = new KafkaProducer();
