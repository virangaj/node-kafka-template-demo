import { EachMessagePayload } from "kafkajs";
import { KAFKA_REQUEST_TOPICS } from "../kafka/kafka.constants";
import { KafkaConsumer } from "../kafka/decorators/kafka-consumer.decorator";
import { UpdateUserOutput } from "../routes";

export class UserUpdateConsumer {
  /**
   * Handles USER_UPDATE_REQUEST messages.
   * For fire-and-forget senders the return value is discarded.
   */
  @KafkaConsumer(KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST)
  async handle(payload: EachMessagePayload): Promise<UpdateUserOutput> {
    const raw = payload.message.value?.toString();
    if (!raw) throw new Error("Message value is missing");

    const { data } = JSON.parse(raw);
    if (!data) throw new Error("data field is missing in message envelope");

    console.log(
      `[UserUpdateConsumer] Processing ${KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST}:`,
      data,
    );

    return { success: true, updatedAt: new Date().toISOString(), data: data };
  }
}
