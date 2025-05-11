package config

import (
	"fmt"
	"log"
	"unicode"
	"unicode/utf8"
)

// Lexer inspired by https://github.com/nats-io/nats-server/blob/main/conf/lex.go
// Check out the NATS implementation for an example of what a real lexer should look like

// Bit mask inspired by Logos' lexer using stacked lookup tables in Rust https://maciej.codes/2020-04-19-stacking-luts-in-logos.html

//--------------------------------------------------------------------------------------------------------------------

// Bit mask low-level byte classification -->
// Each ASCII byte sits within an [int8, 256] lookup table. Each entry represents one ASCII character,
// and is made up of 8 bits. Using bit flags, we can assign up to 8 different classifications per byte.
//
// Example:
// 'a' = 97 --> Index 97 in the table
// IDENTIFIER = 00000001
// This means the character 'a' is classified as an IDENTIFIER.
//
// Bitmasks allow for fast and compact classification of bytes during lexing,
// enabling checks like:
//
//     if classTable[b] & IDENTIFIER != 0 { ... }
//
// Instead of chains of conditions or match statements, this approach turns
// complex logic into a single table lookup and bitwise test.
//
// Multiple flags can be combined to create high-level pattern masks like:
//
//     const identContinue = IDENTIFIER | DIGIT | CONNECTOR
//
// This allows the lexer to scan sequences efficiently and accurately,
// with each byte lookup costing O(1) and no branching.

// Bit flag constants
const (
	identifier  = 1 << iota // a-z A-Z _
	digit                   // 0-9
	connector               // -, ., _,,,
	whitespace              // space, tab, \n
	quote                   // ",'
	sectionMark             // @
	objectMark              // [, ], {, }, (, )
)

// Masks

const (
	identStart    = identifier | sectionMark
	identContinue = identifier | digit | connector
)

func buildLookupTable() [256]int8 {

	table := [256]int8{}

	// Put in identifiers
	for r := byte('a'); r <= byte('z'); r++ {
		table[r] |= identifier
	}
	for r := byte('A'); r <= byte('Z'); r++ {
		table[r] |= identifier
	}

	// Digits
	for r := byte('0'); r <= byte('9'); r++ {
		table[r] |= digit
	}

	// Connectors
	for _, b := range []byte{'-', '_', '.'} {
		table[b] |= connector
	}

	// Whitespace
	for _, b := range []byte{' ', '\n', '\t', '\r'} {
		table[b] |= whitespace
	}

	// Quote
	table['"'] |= quote

	// Section mark
	table['@'] |= sectionMark

	// Object markers
	for _, b := range []byte{'[', ']', '{', '}', '(', ')'} {
		table[b] |= objectMark
	}

	// Adding extra flags
	table['_'] |= identifier

	return table

}

// From the classification we can then lex token types more easily and transition between states.
//   We can:
// - Quickly decide what type of token is starting (e.g., string, number, key, section)
// - Efficiently scan through valid sequences using reusable bitmask patterns
// - Maintain cleaner state transitions between lexing functions (e.g., from `lexTop` to `lexKey`, `lexString`, etc.)
// - Easily extend or refine rules (just update the bit flags)

type tokenType int

const (
	tokenTop = tokenType(iota)
	tokenError
	tokenNIL
	tokenEOF
	tokenSection
	tokenColon
	tokenKey
	tokenText
	tokenString
	tokenBool
	tokenInteger
	tokenFloat
	tokenArrayStart
	tokenArrayEnd
	tokenMapStart
	tokenMapEnd
	tokenCommentStart
)

func parseTokenType(t tokenType) string {

	switch t {

	}

	return ""
}

const (
	eof           = 0
	sectionStart  = '@'
	subSectionSep = '.'
	keyValueSep   = ':'
	arrayStart    = '['
	arrayEnd      = ']'
	mapStart      = '{'
	mapEnd        = '}'
	arrayValSep   = ','
	mapValSep     = ','
	stringStart   = '"'
	stringEnd     = '"'
	optValEnd     = ';'
	commentSep    = '#'

	// May want to add json { } start and end encapsulation

)

// stateFunc is a function that takes the lexer and returns the next state function to run
type stateFunc func(*lexer) stateFunc

/*

stateFunc methods to define ->

sfTop - Start from top switch on token type to return next stateFunc

*/

// Lexer processes a continues stream of bytes (string input (runes)) - we must therefore mark and track the position of
// line starts and ends, positions and columns

type lexer struct {
	input         string
	start         int
	pos           int
	width         int
	line          int
	lineStart     int // Marks the start position of current line
	itemLineStart int // Marks the start position of current relative to the current token
	state         stateFunc
	tokens        chan token

	stack []stateFunc

	stringParts []string
}

type token struct {
	typ    tokenType
	value  string
	length int
	line   int
	pos    int // current: column where token started
}

func lex(input string) *lexer {

	return &lexer{
		input:       input,
		tokens:      make(chan token, 10),
		stack:       make([]stateFunc, 0, 10),
		state:       sfTop, // Will be lexTop function
		line:        1,
		stringParts: []string{},
	}

}

//----------------------------------------------
// Lex methods

//--

func (l *lexer) nextToken() token {

	for {
		select {
		case t := <-l.tokens:
			return t
		default:
			l.state = l.state(l) // This runs the stored state function and sets state to the output of that function -> next state function
		}
	}

}

func (l *lexer) next() (r rune) {

	// Check if we are at end and return early
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}

	// Check if we have reached a new line
	if l.input[l.pos] == '\n' {
		l.line++

		l.lineStart = l.pos
	}

	r, l.width = utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += l.width

	return r
}

func (l *lexer) ignore() {
	l.start = l.pos
	l.itemLineStart = l.start
}

func (l *lexer) backup() {
	l.pos -= l.width
	if l.width == 1 && l.input[l.pos] == '\n' {
		l.line--
	}
}

func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *lexer) push() {
	l.stack = append(l.stack, l.state)
}

func (l *lexer) pop() stateFunc {

	if len(l.stack) == 0 {
		return l.emitError("stack is empty")
	}

	// LIFO - Popping last item in the stack
	index := len(l.stack) - 1
	lastItem := l.stack[index] // Get last item
	l.stack = l.stack[0:index] // Re-assign stack to the re-sized slice (removing last item)
	return lastItem

}

func (l *lexer) emit(typ tokenType) {

	value := l.input[l.start:l.pos]
	pos := l.pos - l.itemLineStart - len(value)
	l.tokens <- token{typ: typ, value: value, line: l.line, pos: pos}
	l.start = l.pos
	l.itemLineStart = l.start

}

func (l *lexer) emitError(format string, args ...any) stateFunc {

	pos := l.pos - l.lineStart

	l.tokens <- token{
		typ:   tokenError,
		value: fmt.Sprintf(format, args...),
		line:  l.line,
		pos:   pos,
	}
	return nil
}

//====================================================================
// Lex State Functions
//====================================================================

// lTop is the starting state function. It switches on r the FIRST rune to dispatch/route to the correct state function based on the first encountered rune
// state functions which are nested such as comments, arrays, maps etc. will push their caller/parent state function or the state function to return to once it has finished
// Doing this, state functions can keep nesting and simply traverse back up the call stack to return to the original caller i.e. lTop by popping from the stack (LIFO)

func sfTop(l *lexer) stateFunc {

	r := l.next()

	if unicode.IsSpace(r) {
		// Call sfSkip, which returns a state function.
		// That returned function will be run in the next lexer loop iteration.
		// It will call ignore(), then transition to sfTop.
		return sfSkip(l, sfTop)
	}

	if r == eof {
		return nil
	}

	switch r {
	case sectionStart:
		fmt.Println("processing section")
		return nil
	case commentSep:
		fmt.Println("processing comment")
		return nil
	default:
		fmt.Println("processing token")
		l.backup()
		return sfKeyStart
	}

}

func sfKeyStart(l *lexer) stateFunc {

	r := l.peek()

	switch r {
	case stringStart:
		// If r is wrapped in string or other markers we can ignore to get the actual key
	}

	log.Println("HERE")
	return sfKey
}

func sfKey(l *lexer) stateFunc {

	// We return ourselves to keep going until we reach an end to emit a token

	r := l.peek()

	if r == keyValueSep || r == eof {
		log.Println("HERE NOW")
		l.emit(tokenKey)
		return sfKeyEnd
	}

	l.next()
	return sfKey

}

func sfKeyEnd(l *lexer) stateFunc {

	//r := l.peek()

	// TODO Finish
	return nil

}

func sfSkip(l *lexer, nextFn stateFunc) stateFunc {
	// We return a function here, so it runs as its own state in the next lexer loop.
	// That function will call `ignore()` and then return the next state.
	// This keeps us inside the state machine model, instead of executing immediately.
	// We use a return func here to craft a state function on the fly and be able to return different state functions as needed
	// Not just a single pre-defined state function every time

	// We could achieve the same by pushing before calling skip and having skip return a l.stack.pop but this is simpler as skip is
	// not a complex function
	return func(*lexer) stateFunc {
		l.ignore()
		return nextFn
	}
}
