package connector

import "strings"

// FilterEmpty returns s with empty strings removed, preserving order.
// It returns nil when no non-empty entries remain so callers can rely on
// `len(result) == 0` and JSON encoders can omit the field.
func FilterEmpty(s []string) []string {
	out := make([]string, 0, len(s))
	for _, v := range s {
		if v != "" {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// StripHTML removes HTML tags from s. It is a deliberately simple,
// dependency-free stripper for normalizing feed and post content. It does
// not decode HTML entities; callers that need fidelity (rich text, scripts,
// CDATA) should reach for a real HTML parser.
func StripHTML(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inTag := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '<':
			inTag = true
		case c == '>':
			inTag = false
		case !inTag:
			b.WriteByte(c)
		}
	}
	return strings.TrimSpace(b.String())
}
