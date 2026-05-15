import { EachMessagePayload } from "kafkajs";
import { KAFKA_REPLY_TOPICS } from "../kafka/kafka.constants";
import { KafkaReplyConsumer } from "../kafka/decorators/kafka-consumer.decorator";

export class UserUpdateReplyConsumer {
  /**
   * Listens on USER_UPDATE_REPLY.
   *
   * The registry detects the @KafkaReplyConsumer annotation and routes the
   * message directly to the matching pending Promise — this method body is
   * intentionally empty; the registry does all the work.
   */
  @KafkaReplyConsumer(KAFKA_REPLY_TOPICS.USER_UPDATE_REPLY)
  async handle(_payload: EachMessagePayload): Promise<void> {
    // Intentionally empty — the registry intercepts reply messages before
    // calling this method. You can add logging here if desired.
  }
}
