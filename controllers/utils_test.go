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

// TestInt32Ptr is the unit test function for int32Ptr
func TestInt32Ptr(t *testing.T) {
	testCases := []struct {
		input  int32
		output *int32
	}{
		{0, new(int32)},
		{1, new(int32)},
		{-1, new(int32)},
		{12345, new(int32)},
		{-12345, new(int32)},
		{2147483647, new(int32)},
		{-2147483648, new(int32)},
	}

	for _, tc := range testCases {
		// Set the expected value of the pointer
		*tc.output = tc.input

		// Call int32Ptr function and compare the result with the expected output
		got := int32Ptr(tc.input)
		if *got != *tc.output {
			t.Errorf("int32Ptr(%d) = %d; want %d", tc.input, *got, *tc.output)
		}
	}
}
