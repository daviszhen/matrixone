package newplan

func NewBindContext(parent *BindContext) *BindContext {
	bc := &BindContext{
		parent: parent,
	}
	if parent != nil {
		bc.defaultDatabase = parent.defaultDatabase
	}
	return bc
}
