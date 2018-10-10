//go:generate goagen bootstrap -d github.com/doriandekoning/IN4392-cloud-computing-lab/worker/design

package main

import (
	"github.com/doriandekoning/IN4392-cloud-computing-lab/worker/app"
	"github.com/goadesign/goa"
	"github.com/goadesign/goa/middleware"
)

func main() {
	// Create service
	service := goa.New("adder")

	// Mount middleware
	service.Use(middleware.RequestID())
	service.Use(middleware.LogRequest(true))
	service.Use(middleware.ErrorHandler(service, true))
	service.Use(middleware.Recover())

	// Mount "health" controller
	c := NewHealthController(service)
	app.MountHealthController(service, c)

	// Start service
	if err := service.ListenAndServe(":8080"); err != nil {
		service.LogError("startup", "err", err)
	}

}
