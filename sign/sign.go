package sign

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

type Signable interface {
	Sign(message []byte) ([]byte, error)
	Open(ciphertext []byte) ([]byte, error)
}

type RSASigner struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	rng        io.Reader
	label      []byte
}

func NewRSASigner(keySize int) Signable {

	privateKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		panic(err)
	}

	return &RSASigner{
		// TODO: this should be parameters external from the application.
		// for now it's for PoC porpouses.
		privateKey,
		&privateKey.PublicKey,
		rand.Reader,
		[]byte("QED_SIGNER"),
	}

}

func (s *RSASigner) Sign(message []byte) (ciphertext []byte, err error) {

	go func() {
		ciphertext, err = rsa.EncryptOAEP(sha256.New(), s.rng, s.publicKey, message, s.label)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error from encryption: %s\n", err)
			return
		}
	}()

	// Since encryption is a randomized function, ciphertext will be
	// different each time.
	// fmt.Printf("Ciphertext: %x\n", ciphertext)
	return

}

func (s *RSASigner) Open(ciphertext []byte) (plaintext []byte, err error) {

	go func() {
		plaintext, err = rsa.DecryptOAEP(sha256.New(), s.rng, s.privateKey, ciphertext, s.label)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error from decryption: %s\n", err)
			return
		}
	}()

	// fmt.Printf("Plaintext: %s\n", string(plaintext))
	return

}
