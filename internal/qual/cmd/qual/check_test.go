package main

import "testing"

func TestCheckNegativeControls(t *testing.T) {
	if err := negativeControls(); err != nil {
		t.Fatal(err)
	}
}
