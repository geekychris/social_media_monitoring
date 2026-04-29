package connector

import (
	"reflect"
	"testing"
)

func TestFilterEmpty(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{"all empty", []string{"", "", ""}, nil},
		{"all populated", []string{"a", "b"}, []string{"a", "b"}},
		{"mixed preserves order", []string{"", "a", "", "b", ""}, []string{"a", "b"}},
		{"nil input", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FilterEmpty(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FilterEmpty(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestStripHTML(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"no tags", "hello world", "hello world"},
		{"single tag", "<p>hi</p>", "hi"},
		{"nested tags", "<div><b>bold</b> text</div>", "bold text"},
		{"with newlines", "line1<br/>line2", "line1line2"},
		{"trims whitespace", "  <p>hi</p>  ", "hi"},
		{"empty", "", ""},
		{"unbalanced", "<p>open only", "open only"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StripHTML(tt.in); got != tt.want {
				t.Errorf("StripHTML(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
