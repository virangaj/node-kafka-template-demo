export const KAFKA_BROKERS = ["localhost:9092"];

export const KAFKA_CLIENT_ID = "kafka-service-1";

export const KAFKA_GROUP_ID = "kafka-service-1-group";

export const DEFAULT_TIMEOUT = 10000;

export const KAFKA_REQUEST_TOPICS: { [key: string]: string } = {
  USER_CREATE_REQUEST: "user-create-request",
  USER_UPDATE_REQUEST: "user-update-request",
  USER_GET_REQUEST: "user-get-request",
};

export const KAFKA_REPLY_TOPICS: { [key: string]: string } = {
  USER_CREATE_RESPONSE: "user-create-response",
  USER_UPDATE_RESPONSE: "user-update-response",
  USER_GET_RESPONSE: "user-get-response",
};

export const KAFKA_REQUEST_TOPICS_ARRAY = Object.values(KAFKA_REQUEST_TOPICS);
export const KAFKA_REPLY_TOPICS_ARRAY = Object.values(KAFKA_REPLY_TOPICS);
