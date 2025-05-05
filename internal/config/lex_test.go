package config

import (
	"fmt"
	"testing"
)

func TestNextMethod(t *testing.T) {

	input := "test"

	lex := lex(input)

	sfTop(lex)

	for i := 0; i <= len(input); i++ {
		fmt.Println(string(lex.next()))
		// Last rune is int32(0) marking EOF
	}

}
