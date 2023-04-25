package main

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"go-producer/api/handlers"
	"log"
)

func main() {
	//if err := godotenv.Load(".env"); err != nil {
	//	log.Fatal(err)
	//}

	handler := handlers.NewHandler()

	app := fiber.New()
	app.Use(cors.New())
	app.Get("/", func(ctx *fiber.Ctx) error {
		return ctx.Send([]byte("Healthy 😍"))
	})
	api := app.Group("/api")
	v1 := api.Group("/v1")

	v1.Post("/", handler.ProduceTestMsg)

	log.Fatal(app.Listen(":8080"))
}
