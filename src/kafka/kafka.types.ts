export interface KafkaMessage<T> {
  data: T;
}

export interface RequestReplyHeaders {
  correlationId: string;
  replyTopic: string;
}

export interface PendingRequest {
  resolve: Function;
  reject: Function;
  timeout: NodeJS.Timeout;
}
