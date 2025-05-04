package config

import (
	"fmt"
	"unicode/utf8"
)

// Lexer inspired by https://github.com/nats-io/nats-server/blob/main/conf/lex.go
// Check out the NATS implementation for an example of what a real lexer should look like

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

type lexer struct {
	input     string
	start     int
	pos       int
	width     int
	line      int
	lineStart int // Marks the start position of current line for multi line items
	state     stateFunc
	tokens    chan token

	stack []stateFunc

	stringParts []string
}

type token struct {
	typ    tokenType
	value  string
	length int
	line   int
	pos    int
}

func lex(input string) *lexer {

	return &lexer{
		input:       input,
		tokens:      make(chan token, 10),
		stack:       make([]stateFunc, 0, 10),
		state:       nil, // Will be lexTop function
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

//====================================================================
// Lex State Functions
//====================================================================

// lTop is the starting state function. It switches on r the FIRST rune to dispatch/route to the correct state function based on the first encountered rune
// state functions which are nested such as comments, arrays, maps etc. will push their caller/parent state function or the state function to return to once it has finished
// Doing this, state functions can keep nesting and simply traverse back up the call stack to return to the original caller i.e. lTop by popping from the stack (LIFO)

func lTop(l *lexer) {

	r := l.next()
	l.backup()

	if r == eof {
		return
	}

	switch r {
	case sectionStart:
		fmt.Println("processing section")
		return
	case commentSep:
		fmt.Println("processing comment")
		return
	default:
		fmt.Println("processing token")
	}

}
