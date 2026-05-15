import { kafkaProducer } from "./kafka.producer";
import { PendingRequest } from "./kafka.types";
import { generateCorrelationId } from "./kafka.utils";

const pendingRequests = new Map<string, PendingRequest>();

export class KafkaRequestReply {
  /**
   * Fire-and-forget: emit a message to a topic with no reply expected.
   *
   * @example
   * await kafkaRequestReply.emit(KAFKA_REQUEST_TOPICS.USER_CREATED, { id: 1 });
   */
  async emit<TRequest>(topic: string, payload: TRequest): Promise<void> {
    await kafkaProducer.sendRaw({
      topic,
      messages: [
        {
          value: JSON.stringify({ data: payload }),
        },
      ],
    });
  }

  /**
   * Request-reply: send a message and await the correlated response.
   *
   * The consumer on the other end must return a value; the registry will
   * automatically publish it to `replyTopic` with the matching correlationId.
   *
   * @param requestTopic  Topic the request is sent to.
   * @param replyTopic    Topic this instance listens on for the reply.
   * @param payload       Request body.
   * @param timeoutMs     How long to wait before rejecting (default 10 s).
   *
   * @example
   * const updated = await kafkaRequestReply.request<UpdateInput, UpdateOutput>(
   *   KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST,
   *   KAFKA_REPLY_TOPICS.USER_UPDATE_REPLY,
   *   { id: 1, name: "Alice" },
   * );
   */
  async request<TRequest, TResponse>(
    requestTopic: string,
    replyTopic: string,
    payload: TRequest,
    timeoutMs = 10_000,
  ): Promise<TResponse> {
    const correlationId = generateCorrelationId();
    console.log(`[${correlationId}] Sending request → ${requestTopic}`);

    await kafkaProducer.sendRaw({
      topic: requestTopic,
      messages: [
        {
          value: JSON.stringify({ data: payload }),
          headers: {
            correlationId,
            replyTopic,
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

  /** Called by the registry when a reply arrives for a pending correlationId. */
  resolveReply(correlationId: string, data: unknown): void {
    console.log(`[${correlationId}] Reply received ✓`);
    const pending = pendingRequests.get(correlationId);
    if (!pending) return;

    clearTimeout(pending.timeout);
    pending.resolve(data);
    pendingRequests.delete(correlationId);
  }

  /** Called by the registry when the remote handler throws an error. */
  rejectReply(correlationId: string, error: Error): void {
    console.error(`[${correlationId}] Reply received with error:`, error.message);
    const pending = pendingRequests.get(correlationId);
    if (!pending) return;

    clearTimeout(pending.timeout);
    pending.reject(error);
    pendingRequests.delete(correlationId);
  }
}

export const kafkaRequestReply = new KafkaRequestReply();
