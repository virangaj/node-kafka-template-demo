import "reflect-metadata";

export const KAFKA_CONSUMER_TOPIC = "KAFKA_CONSUMER_TOPIC";
export const KAFKA_REPLY_HANDLER = "KAFKA_REPLY_HANDLER";

/**
 * Marks a method as a Kafka request consumer for the given topic.
 *
 * The decorated method receives (payload: EachMessagePayload) and must return
 * a value that will be sent back to the replyTopic in the message headers.
 *
 * @example
 * @KafkaConsumer(KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST)
 * async handle(payload: EachMessagePayload) { ... }
 */
export function KafkaConsumer(topic: string) {
  return function (
    _target: object,
    _propertyKey: string,
    descriptor: PropertyDescriptor,
  ) {
    Reflect.defineMetadata(KAFKA_CONSUMER_TOPIC, topic, descriptor.value);
  };
}

/**
 * Marks a method as a handler for Kafka reply messages.
 * The consumer registry will use this to route replies back to pending requests
 * instead of treating them as normal request consumers.
 *
 * @example
 * @KafkaReplyConsumer(KAFKA_REPLY_TOPICS.USER_UPDATE_REPLY)
 * async handle(payload: EachMessagePayload) { ... }
 */
export function KafkaReplyConsumer(topic: string) {
  return function (
    _target: object,
    _propertyKey: string,
    descriptor: PropertyDescriptor,
  ) {
    Reflect.defineMetadata(KAFKA_CONSUMER_TOPIC, topic, descriptor.value);
    Reflect.defineMetadata(KAFKA_REPLY_HANDLER, true, descriptor.value);
  };
}
