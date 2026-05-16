import { Router } from "express";
import {
  KAFKA_REPLY_TOPICS,
  KAFKA_REQUEST_TOPICS,
} from "../kafka/kafka.constants";
import { kafkaProducer } from "../kafka/kafka.producer";

export interface UpdateUserInput {
  id: number;
  name: string;
}

export interface UpdateUserOutput {
  success: boolean;
  updatedAt: string;
  data: UpdateUserInput;
}

const router = Router();

router.get("/health", (_, res) => {
  res.json({
    message: "API Running",
  });
});

router.post("/emit", async (_, res) => {
  await kafkaProducer.emit<UpdateUserInput>(
    KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST,
    { id: 1, name: "Alice" },
  );

  res.json({
    success: true,
  });
});

router.post("/request", async (_, res) => {
  const result = await kafkaProducer.request<UpdateUserInput, UpdateUserOutput>(
    KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST,
    KAFKA_REPLY_TOPICS.USER_UPDATE_REPLY,
    { id: 1, name: "Alice" },
  );

  console.log("Got reply:", result);
  res.json({
    success: true,
    data: result,
  });
});

router.post("/bulk-request", async (_, res) => {
  try {
    const requests = Array.from({ length: 10 }).map((_, index) =>
      kafkaProducer.request<UpdateUserInput, UpdateUserOutput>(
        KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST,
        KAFKA_REPLY_TOPICS.USER_UPDATE_REPLY,
        {
          id: index + 1,
          name: `Alice-${index + 1}`,
        },
      ),
    );

    const results = await Promise.all(requests);

    console.log("Got replies:", results);

    res.json({
      success: true,
      count: results.length,
      data: results,
    });
  } catch (error) {
    console.error("Kafka batch request failed:", error);

    res.status(500).json({
      success: false,
      message: "Batch request failed",
    });
  }
});

export default router;
