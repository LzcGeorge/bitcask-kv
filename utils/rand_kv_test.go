package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetRandomValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(GetRandomValue(i)))
	}
}

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(GetTestKey(i)))
	}
}
