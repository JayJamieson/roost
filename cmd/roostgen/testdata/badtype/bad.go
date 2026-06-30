package badtype

// Bad has an unsupported field type; roostgen must error rather than skip it.
type Bad struct {
	Name string
	Z    complex128 `roost:"name=z"`
}
