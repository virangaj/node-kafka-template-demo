/**
 * Kafka Consumer Decorators
 * -----------------------------------------------------------------------------
 * This module provides decorator utilities for registering Kafka-based message
 * consumers and reply handlers using metadata reflection.
 *
 * Features:
 * - Register request/message consumers
 * - Register reply-topic consumers
 * - Metadata-driven topic discovery
 * - Supports request/reply messaging patterns
 *
 * Requirements:
 * - `reflect-metadata` must be imported once before decorators are used.
 *
 */

import "reflect-metadata";

/**
 * Metadata key used to store the Kafka topic associated with a decorated method.
 */
export const KAFKA_CONSUMER_TOPIC = "KAFKA_CONSUMER_TOPIC";

/**
 * Metadata key used to identify whether a decorated method is a reply handler.
 */
export const KAFKA_REPLY_HANDLER = "KAFKA_REPLY_HANDLER";

/**
 * Decorator used to register a method as a Kafka topic consumer.
 *
 * The decorated method will be associated with the provided Kafka topic through
 * reflection metadata. During application start, a consumer registry 
 * can discover these methods and automatically subscribe them to Kafka.
 *
 * Parameters:
 * @param topic - Kafka topic name to subscribe to.
 *
 * Example:
 * ```ts
 * @KafkaConsumer("user.update.request")
 * async handleUserUpdate(payload: EachMessagePayload) {
 *   const message = payload.message.value?.toString();
 *   return JSON.parse(message ?? "{}");
 * }
 * ```
 */
export function KafkaConsumer(topic: string) {
  return function (
    _target: object,
    _propertyKey: string,
    descriptor: PropertyDescriptor,
  ) {
    Reflect.defineMetadata(
      KAFKA_CONSUMER_TOPIC,
      topic,
      descriptor.value,
    );
  };
}