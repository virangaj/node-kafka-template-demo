import "reflect-metadata";

export const KAFKA_CONSUMER = "KAFKA_CONSUMER";

export function KafkaConsumer(topic: string) {
  return function (
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor,
  ) {
    Reflect.defineMetadata(KAFKA_CONSUMER, topic, descriptor.value);
  };
}
