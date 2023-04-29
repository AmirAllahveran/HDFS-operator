package controllers

import "testing"

func TestStringToInt32(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want int32
	}{
		{"Test 1", "12345", 12345},
		{"Test 2", "0", 0},
		{"Test 3", "-12345", -12345},
		{"Test 4", "abcd", 0}, // parsing non-integer string should result in 0
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stringToInt32(tt.arg)
			if *got != tt.want {
				t.Errorf("stringToInt32() = %v, want %v", *got, tt.want)
			}
		})
	}
}
