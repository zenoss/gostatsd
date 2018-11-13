package zenoss

// Set TODO
type Set map[string]struct{}

// NewSetFromStrings TODO
func NewSetFromStrings(strings []string) *Set {
	s := &Set{}
	s.UpdateFromStrings(strings)
	return s
}

// UpdateFromStrings TODO
func (s *Set) UpdateFromStrings(strings []string) {
	for _, key := range strings {
		s.Add(key)
	}
}

// Add TODO
func (s *Set) Add(key string) {
	(*s)[key] = struct{}{}
}

// Has TODO
func (s *Set) Has(key string) bool {
	_, present := (*s)[key]
	return present
}
