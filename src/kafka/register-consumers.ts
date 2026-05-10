import { UserUpdateConsumer } from "../consumers/user-update-response.consumer";
import { kafkaConsumerRegistry } from "./registry/kafka-consumer.registry";

export function registerKafkaConsumers() {
  kafkaConsumerRegistry.register(new UserUpdateConsumer());
}
