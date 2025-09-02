package config

import (
	"context"
	"math/bits"
	"strings"
	"testing"
	"time"
)

func TestBitMaskClassifications(t *testing.T) {

	pattern1 := identifier | digit
	wantIndex := []int{7, 6}

	var pos = 8
	posIndex := make([]int, 2)

	for i := 0; i < 8; i++ {

		pos--

		if pattern1&(1<<i) != 0 {
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
		if char == 'k' {
			if table[char] != 00000001 {
				t.Errorf("wrong bit mask for char k")
			}
			if BitMaskToString(table[char]) != "IDENTIFIER" {
				t.Errorf("wrong bit flag for k - expected [IDENTIFIER], got %s", BitMaskToString(table[char]))
			}
		}

	}

}

func TestNextMethod(t *testing.T) {

	input := "test"

	lex := lex(input)

	sfTop(lex)

	check := []string{"t", "e", "s", "t"}

	for i := 0; i <= len(input)-1; i++ {

		letter := string(lex.next())

		if check[i] != letter {
			t.Errorf("wrong next char: expected %s, got %s", check[i], letter)
		}
		// Last rune is int32(0) marking EOF
	}

}

func TestKeyStringValueEmit(t *testing.T) {
	//
	//input := "[  \"\"key\"\" :\\\"value\\\""
	input := `"some-key": "value \"is\" this"`

	lex := lex(input)

	token := lex.nextToken()

	if token.value != "some-key" {
		println(token.value)
		t.Errorf("wrong token value - got %s", token.value)
	}

	token2 := lex.nextToken()

	if token2.value != "value \"is\" this" {
		t.Errorf("wrong token value - got %s", token2.value)
	}

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
			tokenCount++
			tokens = append(tokens, token.value)

			if token.typ == tokenEOF {
				if tokenCount != 3 {
					t.Errorf("wrong token count - got %d, want 3", tokenCount)
				}
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

			tokenCount++
			tokens = append(tokens, token.value)

			if token.typ == tokenEOF {
				if tokenCount != 11 {
					t.Errorf("wrong token count - got %d, want 11", tokenCount)
				}
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
			tokenCount++
			tokens = append(tokens, token.value)

			if token.typ == tokenEOF {
				if tokenCount != 21 {
					t.Errorf("wrong token count - got %d, want 21", tokenCount)
				}
				return
			}
		}
	}

}
