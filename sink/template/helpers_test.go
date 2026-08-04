package template

import "testing"

func TestPad(t *testing.T) {
	cases := []struct {
		s    string
		w    int
		c    rune
		left bool
		want string
	}{
		{"ab", 5, '0', true, "000ab"},
		{"ab", 5, '0', false, "ab000"},
		{"abcde", 3, '0', true, "abcde"}, // already wide enough → unchanged
		{"", 2, '*', true, "**"},
	}
	for _, c := range cases {
		if got := pad(c.s, c.w, c.c, c.left); got != c.want {
			t.Errorf("pad(%q,%d,%q,%v) = %q, want %q", c.s, c.w, c.c, c.left, got, c.want)
		}
	}
}

func TestFirstRune(t *testing.T) {
	if got := firstRune("xyz"); got != 'x' {
		t.Errorf("firstRune(xyz) = %q, want x", got)
	}
	if got := firstRune(""); got != ' ' {
		t.Errorf("firstRune(empty) = %q, want space", got)
	}
}

func TestSci(t *testing.T) {
	if got := sci(13326.68); got != "+1.332668E+04" {
		t.Errorf("sci(13326.68) = %q, want +1.332668E+04", got)
	}
	if got := sci("13326.68"); got != "+1.332668E+04" {
		t.Errorf("sci(\"13326.68\") = %q, want +1.332668E+04", got)
	}
	if got := sci("not-a-number"); got != "not-a-number" {
		t.Errorf("sci(non-numeric) = %q, want passthrough", got)
	}
}
