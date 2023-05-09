package main

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/joho/godotenv"
	"github.com/uc0/kafka-golang-java-test.git/api/handlers"
	"log"
)

func main() {
	if err := godotenv.Load(".env"); err != nil {
		log.Fatal(err)
	}

	handler := handlers.NewHandler()

	app := fiber.New()
	app.Use(cors.New())
	app.Get("/", func(ctx *fiber.Ctx) error {
		return ctx.Send([]byte("Healthy 😍"))
	})
	api := app.Group("/api")
	v1 := api.Group("/v1")

	v1.Post("/test1", handler.ProduceTest1Msg)
	v1.Post("/test2", handler.ProduceTest2Msg)

	log.Fatal(app.Listen(":8088"))
}
