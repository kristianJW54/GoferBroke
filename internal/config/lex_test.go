package config

import (
	"fmt"
	"log"
	"math/bits"
	"testing"
)

func TestBitMaskClassifications(t *testing.T) {

	pattern1 := int8(identifier | digit)
	wantIndex := []int{7, 6}

	log.Printf("ident = %08b", pattern1)

	var pos = 8
	posIndex := make([]int, 2)

	for i := 0; i < 8; i++ {

		pos--

		if pattern1&(1<<i) != 0 {
			log.Printf("found bit at %d", pos)
			posIndex[i] = pos
		}

	}

	// Just doing simple bit position check
	if posIndex[0] != wantIndex[0] || posIndex[1] != wantIndex[1] {
		t.Errorf("got wrong positions - wanted %d-%d, got %d-%d", wantIndex[0], wantIndex[1], posIndex[0], posIndex[1])
	}

	testChar := rune('a')

	testLookupTable := [256]int8{}

	testLookupTable[testChar] = pattern1

	if bits.OnesCount(uint(pattern1)) != 2 {
		t.Errorf("mismatch in bit count - got %d, want 2", bits.OnesCount(uint(pattern1)))
	}

	// Now for the main test of if the char index in the lookup table has the correct bit mask
	if testLookupTable[testChar] != pattern1 {
		t.Errorf("rune 'a' in lookup table is classified incorrectly - got %08b, want %08b", testLookupTable[testChar], pattern1)
	}

	log.Printf("got %08b, want %08b", testLookupTable[testChar], pattern1)

}

func TestNextMethod(t *testing.T) {

	input := "test"

	lex := lex(input)

	sfTop(lex)

	for i := 0; i <= len(input); i++ {
		fmt.Println(string(lex.next()))
		// Last rune is int32(0) marking EOF
	}

}

func TestKetEmit(t *testing.T) {

	input := "key:"

	lex := lex(input)

	token := lex.nextToken()

	fmt.Println(token)

}
