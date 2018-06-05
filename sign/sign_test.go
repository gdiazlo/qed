package sign

import (
	"testing"

	assert "github.com/stretchr/testify/require"
)

func TestSign(t *testing.T) {
	signer := NewRSASigner(2048)

	message := []byte("send reinforcements, we're going to advance")

	ciphertext, _ := signer.Sign(message)
	plaintext, _ := signer.Open(ciphertext)

	assert.Equal(t, plaintext, message, "Must be the same strings")

}

func Benchmark2048(b *testing.B) {

	signer := NewRSASigner(2048)
	message := []byte("send reinforcements, we're going to advance")

	b.N = 1000
	for i := 0; i < b.N; i++ {
		signer.Sign(message)
	}

}

func Benchmark4096(b *testing.B) {

	signer := NewRSASigner(4096)
	message := []byte("send reinforcements, we're going to advance")

	b.N = 1000
	for i := 0; i < b.N; i++ {
		signer.Sign(message)
	}

}
