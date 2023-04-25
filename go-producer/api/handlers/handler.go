package handlers

import (
	"github.com/gofiber/fiber/v2"
	"go-producer/pkg/kafka/producer"
	"log"
	"net/http"
)

type Handler struct {
	p *producer.MyProducer
}

func NewHandler() *Handler {
	return &Handler{p: producer.Get()}
}

func (h *Handler) ProduceTestMsg(ctx *fiber.Ctx) (err error) {
	type input struct {
		Message string `json:"message"`
	}

	var body input
	if err = ctx.BodyParser(&body); err != nil {
		return ctx.SendStatus(http.StatusBadRequest)
	}

	if err = h.p.SendMessage("my-test-topic", body.Message); err != nil {
		log.Printf("Failed to send message to consumer: %v\n", err.Error())
		return ctx.SendStatus(http.StatusInternalServerError)
	}
	return ctx.SendStatus(http.StatusOK)
}
