import { Router } from "express";
import { KAFKA_REQUEST_TOPICS } from "../kafka/kafka.constants";
import { kafkaProducer } from "../kafka/kafka.producer";

const router = Router();

router.get("/health", (_, res) => {
  res.json({
    message: "API Running",
  });
});

router.post("/emit", async (_, res) => {
  await kafkaProducer.emit(KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST, {
    name: "Viranga",
    email: "viranga@example.com"
  });

  res.json({
    success: true,
  });
});

export default router;
