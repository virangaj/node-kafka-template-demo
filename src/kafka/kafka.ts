import { Kafka } from "kafkajs";

import {
  KAFKA_BROKERS,
  KAFKA_CLIENT_ID,
} from "./kafka.constants";

export const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS,
});