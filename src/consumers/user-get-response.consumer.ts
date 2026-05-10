import { EachMessagePayload, Kafka } from "kafkajs";
import { KafkaConsumer } from "../kafka/decorators/kafka-consumer.decorator";
import { KAFKA_REQUEST_TOPICS } from "../kafka/kafka.constants";
import { kafkaRequestReply } from "../kafka/kafka.request-reply";

export class UserGetResponseConsumer {
  @KafkaConsumer(KAFKA_REQUEST_TOPICS.USER_GET_RESPONSE)
  async handle(payload: EachMessagePayload) {
    const correlationId = payload.message.headers?.correlationId?.toString();
    console.log(`[${correlationId}] - Received USER_GET_RESPONSE`);

    if (!correlationId) {
      console.error("Correlation ID missing in message headers");
      return;
    }

    const value = payload.message.value?.toString();
    if (!value) {
      console.error("Message value missing");
      return;
    }

    const parsedValue = JSON.parse(value);

    kafkaRequestReply.resolveReply(correlationId, parsedValue);
  }
}
