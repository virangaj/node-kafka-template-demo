import { EachMessagePayload } from "kafkajs";
import { KAFKA_REQUEST_TOPICS } from "../kafka/kafka.constants";
import { KafkaConsumer } from "../kafka/decorators/kafka-consumer.decorator";

export class UserUpdateConsumer {
  @KafkaConsumer(KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST)
  async handle(payload: EachMessagePayload) {
    const value = payload.message.value?.toString();

    if (!value) {
      throw new Error("Message value missing");
    }

    const parsedValue = JSON.parse(value);

    /*
     * Validate request
     */
    if (!parsedValue.data) {
      throw new Error("Data missing");
    }

    console.log("USER_UPDATE_REQUEST received");

    console.log(parsedValue);
  }
}
