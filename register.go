package perflab

import "go.k6.io/k6/v2/js/modules"

// importPath is the JS import path of this extension.
const importPath = "k6/x/iroh"

func init() {
	modules.Register(importPath, new(RootModule))
}
