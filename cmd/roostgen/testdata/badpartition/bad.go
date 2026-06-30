package badpartition

// Bad marks a float column as a partition; roostgen must reject it (floats have
// no stable partition-path formatting), matching the reflection path.
type Bad struct {
	Name  string
	Ratio float64 `roost:"name=ratio,partition"`
}
