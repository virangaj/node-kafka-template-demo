import { kafkaConsumerRegistry } from "./registry/kafka-consumer.registry";
import { UserUpdateConsumer } from "../consumers/user-update.consumer";
import { UserUpdateReplyConsumer } from "../consumers/user-update-reply.consumer";

/**
 * Register every consumer class here.
 * The registry will scan each instance for @KafkaConsumer / @KafkaReplyConsumer
 * decorated methods and wire them up automatically.
 */
export function registerKafkaConsumers(): void {
  kafkaConsumerRegistry.register(new UserUpdateConsumer());
  kafkaConsumerRegistry.register(new UserUpdateReplyConsumer());
  // add more consumers here as your app grows…
}
