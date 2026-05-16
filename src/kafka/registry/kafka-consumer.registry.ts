import { EachMessagePayload } from "kafkajs";
import {
  KAFKA_CONSUMER_TOPIC,
  KAFKA_REPLY_HANDLER,
} from "../decorators/kafka-consumer.decorator";
import { KafkaReplyEnvelope } from "../kafka.types";
import { kafkaProducer } from "../kafka.producer";

type HandlerFn = (payload: EachMessagePayload) => Promise<unknown>;

interface RegisteredHandler {
  fn: HandlerFn;
  isReplyHandler: boolean;
}

class KafkaConsumerRegistry {
  private handlers = new Map<string, RegisteredHandler>();

  /**
   * Scans a class instance for methods decorated with @KafkaConsumer or
   * @KafkaReplyConsumer and registers them.
   */
  register(instance: object): void {
    const proto = Object.getPrototypeOf(instance);

    for (const key of Object.getOwnPropertyNames(proto)) {
      const method = (proto as Record<string, unknown>)[key];
      if (typeof method !== "function") continue;

      const topic: string | undefined = Reflect.getMetadata(
        KAFKA_CONSUMER_TOPIC,
        method,
      );
      if (!topic) continue;

      const isReplyHandler: boolean =
        Reflect.getMetadata(KAFKA_REPLY_HANDLER, method) ?? false;

      const bound = (method as HandlerFn).bind(instance);
      this.handlers.set(topic, { fn: bound, isReplyHandler });

      console.log(
        `[KafkaRegistry] Registered ${isReplyHandler ? "reply" : "request"} handler for topic: ${topic}`,
      );
    }
  }

  /**
   * Dispatches an incoming Kafka message to the correct handler.
   *
   * - Reply topics: extract correlationId from headers and resolve the pending Promise.
   * - Request topics: call the handler and, if a replyTopic header is present,
   *   send the result back automatically.
   */
  async execute(topic: string, payload: EachMessagePayload): Promise<void> {
    const headers = payload.message.headers ?? {};
    const correlationId = headers["correlationId"]?.toString();
    const entry = this.handlers.get(topic);

    // ── Reply path ──────────────────────────────────────────────────────────
    // No registered handler + has correlationId = this is a reply message
    if (!entry) {
      if (correlationId) {
        const raw = payload.message.value?.toString();
        if (!raw) return;

        const envelope = JSON.parse(raw) as KafkaReplyEnvelope;

        if (envelope.error) {
          kafkaProducer.rejectReply(correlationId, new Error(envelope.error));
        } else {
          kafkaProducer.resolveReply(correlationId, envelope.data);
        }
      } else {
        console.warn(
          `[KafkaRegistry] No handler registered for topic: ${topic}`,
        );
      }
      return;
    }

    // ── Request path ─────────────────────────────────────────────────────────
    const replyTopic = headers["replyTopic"]?.toString();

    try {
      const result = await entry.fn(payload);

      if (replyTopic && correlationId) {
        await kafkaProducer.sendRaw({
          topic: replyTopic,
          messages: [
            {
              value: JSON.stringify({
                data: result,
              } satisfies KafkaReplyEnvelope),
              headers: { correlationId },
            },
          ],
        });
      }
    } catch (error) {
      console.error(`[KafkaRegistry] Handler error on topic ${topic}:`, error);

      if (replyTopic && correlationId) {
        await kafkaProducer.sendRaw({
          topic: replyTopic,
          messages: [
            {
              value: JSON.stringify({
                data: null,
                error: error instanceof Error ? error.message : String(error),
              } satisfies KafkaReplyEnvelope),
              headers: { correlationId },
            },
          ],
        });
      }
    }
  }
}

export const kafkaConsumerRegistry = new KafkaConsumerRegistry();
