package handlers

import "github.com/gofiber/fiber/v2"

type Handler struct {
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) ProduceTestMsg(ctx *fiber.Ctx) (err error) {

	return
}
