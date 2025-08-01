package config

import (
	"context"
	"fmt"
	"math/bits"
	"strings"
	"testing"
	"time"
)

func TestBitMaskClassifications(t *testing.T) {

	for ascii := 0; ascii <= 255; ascii++ {

		fmt.Printf("%c\n", ascii)

	}

	pattern1 := identifier | digit
	wantIndex := []int{7, 6}

	fmt.Printf("ident = %08b\n", pattern1)

	var pos = 8
	posIndex := make([]int, 2)

	for i := 0; i < 8; i++ {

		pos--

		if pattern1&(1<<i) != 0 {
			fmt.Printf("found bit at %d\n", pos)
			posIndex[i] = pos
		}

	}

	// Just doing simple bit position check
	if posIndex[0] != wantIndex[0] || posIndex[1] != wantIndex[1] {
		t.Errorf("got wrong positions - wanted %d-%d, got %d-%d", wantIndex[0], wantIndex[1], posIndex[0], posIndex[1])
	}

	testChar := 'a'

	testLookupTable := [256]int8{}

	testLookupTable[testChar] = pattern1

	if bits.OnesCount(uint(pattern1)) != 2 {
		t.Errorf("mismatch in bit count - got %d, want 2", bits.OnesCount(uint(pattern1)))
	}

	// Now for the main test of if the char index in the lookup table has the correct bit mask
	if testLookupTable[testChar] != pattern1 {
		t.Errorf("rune 'a' in lookup table is classified incorrectly - got %08b, want %08b", testLookupTable[testChar], pattern1)
	}

	fmt.Printf("got %08b, want %08b\n", testLookupTable[testChar], pattern1)

}

func BitMaskToString(mask int8) string {

	var flags []string

	if mask&identifier != 0 {
		flags = append(flags, "IDENTIFIER")
	}
	if mask&digit != 0 {
		flags = append(flags, "DIGIT")
	}
	if mask&connector != 0 {
		flags = append(flags, "CONNECTOR")
	}
	if mask&whitespace != 0 {
		flags = append(flags, "WHITESPACE")
	}
	if mask&comment != 0 {
		flags = append(flags, "COMMENT")
	}
	if mask&quote != 0 {
		flags = append(flags, "STRING")
	}
	if mask&object != 0 {
		flags = append(flags, "OBJECT")
	}

	return strings.Join(flags, " | ")
}

func TestBuildTable(t *testing.T) {

	table := buildLookupTable()

	testWord := "@hello 007 look_me_up []"

	for _, char := range testWord {

		fmt.Printf("%c - %08b - %s\n", char, table[char], BitMaskToString(table[char]))

	}

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

func TestKeyStringValueEmit(t *testing.T) {
	//
	//input := "[  \"\"key\"\" :\\\"value\\\""
	input := `"some-key": "value \"is\" this"`

	lex := lex(input)

	token := lex.nextToken()

	fmt.Println(token)

	token2 := lex.nextToken()

	fmt.Println(token2)

}

func TestArrayEmit(t *testing.T) {

	input := `"some-key": 1`

	lex := lex(input)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tokenCount := 0
	tokens := make([]string, 0)

	for {
		select {
		case <-ctx.Done():
			t.Fatal("lexer timeout")
		default:
			token := lex.nextToken()
			fmt.Println(token)

			tokenCount++
			tokens = append(tokens, token.value)

			if token.typ == tokenEOF {
				fmt.Printf("total tokens = %v || tokens --> %+s\n", tokenCount, tokens)
				return
			}
		}
	}

}

func TestMapEmit(t *testing.T) {

	input := `some-key {
  				 some_map {
    				key1: "value1"
					key2: "value2"
  				 }
			  }`

	lex := lex(input)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tokenCount := 0
	tokens := make([]string, 0)

	for {
		select {
		case <-ctx.Done():
			t.Fatal("lexer timeout")
		default:
			token := lex.nextToken()
			fmt.Println(token)

			tokenCount++
			tokens = append(tokens, token.value)

			if token.typ == tokenEOF {
				fmt.Printf("total tokens = %v || tokens --> %+s\n", tokenCount, tokens)
				return
			}
		}
	}

}

func TestBasicConfig(t *testing.T) {

	input := `
Name: "test-server"
SeedServers: [
    { host: 192.168.0.1, port: 8081 },
    { host: "192.168.0.1", port: 8082 },
]
Cluster {}`

	lex := lex(input)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tokenCount := 0
	tokens := make([]string, 0)

	for {
		select {
		case <-ctx.Done():
			t.Fatal("lexer timeout")
		default:
			token := lex.nextToken()
			fmt.Println(token)

			tokenCount++
			tokens = append(tokens, token.value)

			if token.typ == tokenEOF {
				fmt.Printf("total tokens = %v || tokens --> %+s\n", tokenCount, tokens)
				return
			}
		}
	}

}
