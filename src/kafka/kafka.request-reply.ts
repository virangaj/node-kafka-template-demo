import { clear } from "node:console";
import { kafkaProducer } from "./kafka.producer";
import { PendingRequest } from "./kafka.types";
import { generateCorrelationId } from "./kafka.utils";

const pendingRequests = new Map<string, PendingRequest>();

export class KafkaRequestReply {
  async request<TRequest, TResponse>(
    requestTopic: string,
    replyTopic: string,
    payload: TRequest,
    timeoutMs = 10000,
  ): Promise<TResponse> {
    const correlationId = await generateCorrelationId();
    console.log(`[${correlationId}] - Sending request to ${requestTopic}`);

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
        reject(new Error("Request timed out"));
      }, timeoutMs);

      pendingRequests.set(correlationId, { resolve, reject, timeout });
    });
  }

  resolveReply(correlationId: string, data: unknown) {
    console.log(`[${correlationId}] - Resolving reply`);
    const request = pendingRequests.get(correlationId);

    if (!request) {
      return;
    }

    clearTimeout(request.timeout);
    request.resolve(data);
    pendingRequests.delete(correlationId);
  }
}

export const kafkaRequestReply = new KafkaRequestReply();
