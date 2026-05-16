import { UserUpdateConsumer } from "../consumers/user-update.consumer";
import { kafkaConsumerRegistry } from "./registry/kafka-consumer.registry";

/**
 * Register every consumer class here.
 * The registry will scan each instance for @KafkaConsumer / @KafkaReplyConsumer
 * decorated methods and wire them up automatically.
 */
export function registerKafkaConsumers(): void {
  kafkaConsumerRegistry.register(new UserUpdateConsumer());
  // add more consumers here as your app grows…
}
