import { Router } from "express";
import {
  KAFKA_REPLY_TOPICS,
  KAFKA_REQUEST_TOPICS,
} from "../kafka/kafka.constants";
import { kafkaProducer } from "../kafka/kafka.producer";
import { kafkaRequestReply } from "../kafka/kafka.request-reply";

interface UpdateUserInput {
  id: number;
  name: string;
}

interface UpdateUserOutput {
  success: boolean;
  updatedAt: string;
}

const router = Router();

router.get("/health", (_, res) => {
  res.json({
    message: "API Running",
  });
});

router.post("/emit", async (_, res) => {
  await kafkaRequestReply.emit<UpdateUserInput>(
    KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST,
    { id: 1, name: "Alice" },
  );

  res.json({
    success: true,
  });
});

router.post("/request", async (_, res) => {
  const result = await kafkaRequestReply.request<
    UpdateUserInput,
    UpdateUserOutput
  >(
    KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST,
    KAFKA_REPLY_TOPICS.USER_UPDATE_REPLY,
    { id: 1, name: "Alice" },
    10_000, // optional timeout in ms
  );

  console.log("Got reply:", result);
  res.json({
    success: true,
    data: result,
  });
});

export default router;
