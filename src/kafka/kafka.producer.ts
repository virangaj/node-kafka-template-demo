import { Producer } from "kafkajs";

import { kafka } from "./kafka";
import { kafkaConsumerManager } from "./kafka.consumer";
import { PendingRequest } from "./kafka.types";
import { generateCorrelationId } from "./kafka.utils";

const pendingRequests = new Map<string, PendingRequest>();
const subscribedReplyTopics = new Set<string>(); // track so we don't double-subscribe

export class KafkaProducer {
  private producer: Producer;

  constructor() {
    this.producer = kafka.producer();
  }

  async connect() {
    await this.producer.connect();
    console.log("Kafka Producer Connected");
  }

  async sendRaw(payload: any) {
    await this.producer.send(payload);
  }

  async emit<TRequest>(topic: string, payload: TRequest): Promise<void> {
    await this.sendRaw({
      topic,
      messages: [
        {
          value: JSON.stringify({ data: payload }),
        },
      ],
    });
  }

  async request<TRequest, TResponse>(
    requestTopic: string,
    replyTopic: string,
    payload: TRequest,
    timeoutMs = 10_000,
    key?: string,
  ): Promise<TResponse> {
    // Auto-subscribe to the reply topic the first time it's seen
    if (!subscribedReplyTopics.has(replyTopic)) {
      subscribedReplyTopics.add(replyTopic);
      await kafkaConsumerManager.subscribeReplyTopic(replyTopic);
    }

    const correlationId = generateCorrelationId();
    console.log(`[${correlationId}] Sending request → ${requestTopic}`);

    await this.sendRaw({
      topic: requestTopic,
      messages: [
        {
          value: JSON.stringify({ data: payload }),
          headers: {
            correlationId,
            replyTopic,
            key: key ?? "",
          },
        },
      ],
    });

    return new Promise<TResponse>((resolve, reject) => {
      const timeout = setTimeout(() => {
        pendingRequests.delete(correlationId);
        reject(
          new Error(
            `[${correlationId}] Request to ${requestTopic} timed out after ${timeoutMs} ms`,
          ),
        );
      }, timeoutMs);

      pendingRequests.set(correlationId, { resolve, reject, timeout });
    });
  }

  resolveReply(correlationId: string, data: unknown): void {
    console.log(`[${correlationId}] Reply received`);
    const pending = pendingRequests.get(correlationId);
    if (!pending) return;

    clearTimeout(pending.timeout);
    pending.resolve(data);
    pendingRequests.delete(correlationId);
  }

  rejectReply(correlationId: string, error: Error): void {
    console.error(`[${correlationId}] Reply received with error:`, error.message);
    const pending = pendingRequests.get(correlationId);
    if (!pending) return;

    clearTimeout(pending.timeout);
    pending.reject(error);
    pendingRequests.delete(correlationId);
  }

  getInstance() {
    return this.producer;
  }
}

export const kafkaProducer = new KafkaProducer();