export interface PendingRequest {
  resolve: (value: any) => void;
  reject: (reason?: any) => void;
  timeout: ReturnType<typeof setTimeout>;
}

export interface KafkaMessageEnvelope<T = unknown> {
  data: T;
}

export interface KafkaReplyEnvelope<T = unknown> {
  data: T;
  error?: string;
}
