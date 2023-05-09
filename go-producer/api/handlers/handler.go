package handlers

import (
	"github.com/gofiber/fiber/v2"
	"github.com/uc0/kafka-golang-java-test.git/pkg/kafka/producer"
	"github.com/uc0/kafka-golang-java-test.git/proto"
	"log"
	"net/http"
	"time"
)

type Handler struct {
	p *producer.MyProducer
}

func NewHandler() *Handler {
	return &Handler{p: producer.Get()}
}

func (h *Handler) ProduceTest1Msg(ctx *fiber.Ctx) (err error) {
	type input struct {
		Title   string `json:"title"`
		Content string `json:"content"`
	}

	var body input
	if err = ctx.BodyParser(&body); err != nil {
		return ctx.SendStatus(http.StatusBadRequest)
	}
	log.Printf("request body: %+v\n", body)

	msg := proto.TestMessage{
		Title:     body.Title,
		Content:   body.Content,
		Timestamp: time.Now().Unix(),
	}

	if err = h.p.SendMessage("my-test-topic", &msg); err != nil {
		log.Printf("Failed to send message to consumer: %v\n", err.Error())
		return ctx.SendStatus(http.StatusInternalServerError)
	}
	return ctx.Status(http.StatusOK).JSON(fiber.Map{"date": body})
}

func (h *Handler) ProduceTest2Msg(ctx *fiber.Ctx) (err error) {
	type input struct {
		Message       string `json:"message"`
		MessageNumber int64  `json:"message_number"`
	}

	var body input
	if err = ctx.BodyParser(&body); err != nil {
		return ctx.SendStatus(http.StatusBadRequest)
	}
	log.Printf("request body: %+v\n", body)

	msg := proto.Test2Message{
		Message: body.Message,
		MessageNumber: body.MessageNumber,
		Timestamp: time.Now().Unix(),
	}

	if err = h.p.SendMessage("my-test-2-topic", &msg); err != nil {
		log.Printf("Failed to send message to consumer: %v\n", err.Error())
		return ctx.SendStatus(http.StatusInternalServerError)
	}
	return ctx.Status(http.StatusOK).JSON(fiber.Map{"date": body})
}
