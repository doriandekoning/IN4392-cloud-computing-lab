package design

import (
	. "github.com/goadesign/goa/design"
	. "github.com/goadesign/goa/design/apidsl"
)

var _ = Resource("health", func() {
	Action("health", func() {
		Routing(GET("health"))
		Description("Returns 200 if the node is helaty")
		Response(OK)
	})

})
