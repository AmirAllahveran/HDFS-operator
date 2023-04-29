package controllers

import "strconv"

func stringToInt32(s string) *int32 {
	i, _ := strconv.ParseInt(s, 10, 32)
	result := int32(i)
	return &result
}
