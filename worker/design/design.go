package design

import (
	. "github.com/goadesign/goa/design/apidsl"
)

var _ = API("adder", func() {
	Title("The master API")
	Description("A master")
	Host("localhost:8080")
	Scheme("http")
})
