package config

import (
	"fmt"
	"log"
	"strings"
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
	identifier int8 = 1 << iota // a-z A-Z _
	digit                       // 0-9
	connector                   // -, ., _,,,
	whitespace                  // space, tab, \n
	comment                     // #, //
	quote                       // ' or "
	object                      // [, {, (, ), }, ]
)

// Masks

const (
	identContinue = identifier | digit | connector
	identStart    = identifier | digit | quote
	check         = identifier | digit | connector | whitespace | comment | quote | object
	valueCheck    = identContinue | quote | object
	junk          = ^check
)

func buildLookupTable() [256]int8 {

	table := [256]int8{}

	// Put in identifiers
	for r := byte('a'); r <= byte('z'); r++ {
		table[r] |= int8(identifier)
	}
	for r := byte('A'); r <= byte('Z'); r++ {
		table[r] |= int8(identifier)
	}

	// Digits
	for r := byte('0'); r <= byte('9'); r++ {
		table[r] |= int8(digit)
	}

	// Connectors
	for _, b := range []byte{'-', '_', '.'} {
		table[b] |= int8(connector)
	}

	// Whitespace
	for _, b := range []byte{' ', '\n', '\t', '\r'} {
		table[b] |= int8(whitespace)
	}

	// Comment
	for _, b := range []byte{'#', '/'} {
		table[b] |= int8(comment)
	}

	// quote or string markers
	for _, b := range []byte{'"', '\'', '\\'} {
		table[b] |= int8(quote)
	}

	// Adding extra flags
	table['_'] |= int8(identifier)

	for _, b := range []byte{'(', ')', '{', '}', '[', ']'} {
		table[b] |= int8(object)
	}

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
	keyValueEqSep = '='
	arrayStart    = '['
	arrayEnd      = ']'
	mapStart      = '{'
	mapEnd        = '}'
	arrayValSep   = ','
	mapValSep     = ','
	dStringStart  = '"'
	dStringEnd    = '"'
	sStringStart  = '\''
	sStringEnd    = '\''
	escaped       = '\\'
	optValEnd     = ';'
	commentSep    = '#'

	// May want to add json { } start and end encapsulation

)

// stateFunc is a function that takes the lexer and returns the next state function to run
type stateFunc func(lx *lexer) stateFunc

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
	stringState stateFunc

	lookup [256]int8
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
		lookup:      buildLookupTable(),
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
	l.itemLineStart = l.lineStart
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

func (l *lexer) push(state stateFunc) {
	l.stack = append(l.stack, state)
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

	value := strings.Join(l.stringParts, "") + l.input[l.start:l.pos]

	pos := l.pos - l.itemLineStart - len(value)

	l.tokens <- token{typ: typ, value: value, line: l.line, pos: pos}
	l.start = l.pos
	l.itemLineStart = l.lineStart

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

func (l *lexer) emitString() {

	var str string
	if len(l.stringParts) > 0 {
		str = strings.Join(l.stringParts, "") + l.input[l.start:l.pos]
		l.stringParts = []string{}
	} else {
		str = l.input[l.start:l.pos]
	}

	pos := l.pos - l.itemLineStart - len(str)
	l.tokens <- token{typ: tokenString, value: str, line: l.line, pos: pos}
	l.start = l.pos
	l.itemLineStart = l.lineStart

}

func (l *lexer) addToStringParts(offset int) {
	l.stringParts = append(l.stringParts, l.input[l.start:l.pos-offset])
	l.start = l.pos
}

func (l *lexer) addStringPart(s string) stateFunc {
	l.stringParts = append(l.stringParts, s)
	l.start = l.pos
	return l.stringState
}

//====================================================================
// Lex State Functions
//====================================================================

// lTop is the starting state function. It switches on r the FIRST rune to dispatch/route to the correct state function based on the first encountered rune
// state functions which are nested such as comments, arrays, maps etc. will push their caller/parent state function or the state function to return to once it has finished
// Doing this, state functions can keep nesting and simply traverse back up the call stack to return to the original caller i.e. lTop by popping from the stack (LIFO)

func sfTop(l *lexer) stateFunc {

	r := l.next()

	if r < 0 || r > 255 {
		return l.emitError("unexpected non-ASCII input")
	}

	if r == eof {
		if l.pos > l.start {
			return l.emitError("Unexpected EOF")
		}
		l.emit(tokenEOF)
		return nil
	}

	if l.lookup[r]&check == 0 {
		log.Printf("skipping junk = %s", string(r))
		return sfSkip(l, sfTop)
	}

	if l.lookup[r]&whitespace != 0 {
		// Call sfSkip, which returns a state function.
		// That returned function will be run in the next lexer loop iteration.
		// It will call ignore(), then transition to sfTop.
		return sfSkip(l, sfTop)
	}

	if l.lookup[r]&comment != 0 {
		fmt.Println("processing comment")
		l.emitError("i have not implemented this yet lol")
		return nil
	}

	l.backup()
	l.push(sfTopValueEnd)
	return sfKeyStart

}

func sfTopValueEnd(l *lexer) stateFunc {

	r := l.next()

	switch {

	case l.lookup[r]&comment != 0:
		l.push(sfTop)
		return sfCommentStart

	case r == keyValueEqSep || r == keyValueSep || r == '\n' || r == '\r' || r == eof:
		l.ignore()
		return sfTop

	case l.lookup[r]&whitespace != 0:
		return sfTopValueEnd

	}

	return l.emitError("expected a value to end with new line, comment or EOF - got (%s)", string(r))

}

func sfKeyStart(l *lexer) stateFunc {

	r := l.peek()

	if l.lookup[r]&whitespace != 0 {
		l.next()
		return sfSkip(l, sfKeyStart)
	}

	if l.lookup[r]&quote != 0 {

		l.next()

		switch {
		case r == dStringStart:
			return sfSkip(l, sfDQuotedKey)
		case r == sStringStart:
			return sfSkip(l, sfSQuotedKey)
		}
	}

	l.ignore()
	l.next()
	return sfKey

}

func sfDQuotedKey(l *lexer) stateFunc {

	r := l.peek()

	log.Printf("quoted key: %s", string(r))

	if r == eof {
		l.emitError("unexpected EOF")
		return nil
	}

	if r == dStringEnd {
		l.emit(tokenKey)
		l.next()
		return sfSkip(l, sfKeyEnd)
	}

	l.next()
	return sfDQuotedKey

}

func sfSQuotedKey(l *lexer) stateFunc {

	l.emitError("not implemented")
	return nil

}

func sfKey(l *lexer) stateFunc {

	// We return ourselves to keep going until we reach an end to emit a token

	r := l.peek()

	if l.lookup[r]&whitespace != 0 {
		l.emit(tokenKey)
		return sfKeyEnd
	}

	if r == keyValueSep || r == keyValueEqSep || r == eof {

		l.emit(tokenKey)
		return sfKeyEnd
	}

	log.Printf("r = %s", string(r))

	l.next()
	return sfKey

}

func sfKeyEnd(l *lexer) stateFunc {

	// We use key end to reach the beginning of a value and transition state to lex a value
	// We are not emitting the key separator, it will be ignored along with white space and quotes

	r := l.next()

	if l.lookup[r]&whitespace != 0 {
		return sfSkip(l, sfKeyEnd)
	}

	if r == keyValueSep || r == keyValueEqSep {
		log.Printf("found key separator (%s)", string(r))
		log.Printf("next char = %s", string(l.peek()))
		return sfSkip(l, sfValue)
	}

	if r == eof {
		l.emit(tokenEOF)
		return nil
	}

	l.backup()
	return sfValue

}

func sfValue(l *lexer) stateFunc {

	r := l.next()

	//log.Printf("r = %s", string(r))

	// Here we must identify the start of a value and ignore all junk before this after reaching key end
	// If there is whitespace we ignore it
	// Now anything that isn't something we expect from a value we error on and switch on all value cases

	if l.lookup[r]&whitespace != 0 || l.lookup[r]&comment != 0 {
		return sfSkip(l, sfValue)
	}

	switch {

	case r == keyValueEqSep || r == keyValueSep:
		return sfSkip(l, sfValue)

	case l.lookup[r]&object != 0:

		log.Printf("object = %s", string(r))
		// Switch on what object

		if r == arrayStart {
			log.Printf("found array start")
			log.Printf("object = %s", string(l.input[l.start:l.pos]))
			l.ignore()
			l.emit(tokenArrayStart)
			log.Printf("next token will be --> %s", string(l.peek()))
			return sfArrayValue
		}
		if r == mapStart {
			log.Printf("found map start")
			l.ignore()
			l.emit(tokenMapStart)
			log.Printf("next token will be --> %s", string(l.peek()))
			return sfMapKeyStart
		}

		// TODO finish array tokenizing - should we backup before changing state?

	case l.lookup[r]&quote != 0:
		if r == dStringStart {
			l.ignore()
			l.stringState = dQuotedString
			return dQuotedString
		}
		if r == sStringStart {
			l.ignore()
			return sQuotedString
		}
		// Else we have an escaped string
		l.backup()
		l.stringState = sfStringValue // We set our string state function to where we want to return back to after processing sub string elements
		return sfStringValue

	case l.lookup[r]&digit != 0:
		l.backup()
		return sfDigitStart

	}

	l.backup()
	l.stringState = sfStringValue
	return sfStringValue

}

func sfArrayValue(l *lexer) stateFunc {

	r := l.next()

	if l.lookup[r]&whitespace != 0 {
		return sfSkip(l, sfArrayValue)
	}

	if l.lookup[r]&comment != 0 {
		l.push(sfArrayValue)
		return sfCommentStart
	}

	if r == ',' {
		return l.emitError("unexpected ',' in array start")
	}

	if r == arrayEnd {
		return sfArrayEnd
	}

	l.backup()
	l.push(sfArrayValueEnd)
	return sfValue // Move to string lexer to process inner string - this will pop last state from stack

}

func sfArrayValueEnd(l *lexer) stateFunc {

	r := l.next()

	log.Printf("array value end reached %s", string(l.input[l.start:l.pos]))

	if l.lookup[r]&whitespace != 0 {
		return sfSkip(l, sfArrayValueEnd)
	}

	if l.lookup[r]&comment != 0 {
		l.push(sfArrayValueEnd)
		return sfCommentStart
	}

	if r == arrayValSep || r == '\n' || r == '\r' {
		log.Printf("found separator so switching to sfArrayValue to process next in array")
		return sfSkip(l, sfArrayValue)
	}

	if r == arrayEnd {
		return sfArrayEnd
	}

	return l.emitError("expected array value separator or comment - got (%s) instead", string(r))

}

func sfArrayEnd(l *lexer) stateFunc {
	l.ignore()
	l.emit(tokenArrayEnd)
	return l.pop()
}

func sfMapKeyStart(l *lexer) stateFunc {

	r := l.peek()

	switch {
	case l.lookup[r]&whitespace != 0:
		l.next()
		return sfSkip(l, sfMapKeyStart)
	case r == mapEnd:
		l.next()
		return sfSkip(l, sfMapEnd)
	case r == keyValueSep || r == '=':
		return l.emitError("unexpected character - (%s)", string(r))
	case r == arrayEnd:
		return l.emitError("unexpected array end - (%s)", string(r))
	case l.lookup[r]&comment != 0:
		l.next()
		l.push(sfMapKeyStart)
		return sfCommentStart
	case l.lookup[r]&quote != 0:
		l.next()
		l.ignore()
		if r == sStringStart {
			return sfMapQuoteKey
		} else {
			return sfMapDQuoteKey
		}
	}

	l.ignore()
	l.next()
	return sfMapKey

}

func sfMapQuoteKey(l *lexer) stateFunc {

	r := l.peek()

	if r == eof {
		return l.emitError("unexpected EOF processing quoted map key - (%s)", string(r))
	} else if r == sStringEnd {
		l.emit(tokenKey)
		l.next()
		return sfSkip(l, sfMapKeyEnd)
	}

	l.next()
	return sfMapQuoteKey
}

func sfMapDQuoteKey(l *lexer) stateFunc {

	r := l.peek()

	if r == eof {
		return l.emitError("unexpected EOF processing quoted map key - (%s)", string(r))
	} else if r == dStringEnd {
		l.emit(tokenKey)
		l.next()
		return sfSkip(l, sfMapKeyEnd)
	}

	l.next()
	return sfMapDQuoteKey
}

func sfMapKey(l *lexer) stateFunc {

	// We could check for space and do keyword check in future

	r := l.peek()

	switch {
	case r == eof:
		return l.emitError("unexpected EOF processing map key - (%s)", string(r))
	case l.lookup[r]&whitespace != 0:
		l.emit(tokenKey)
		return sfMapKeyEnd
	case r == keyValueSep || r == keyValueEqSep:
		log.Printf("found key separator in map - (%s)", string(r))
		l.emit(tokenKey)
		return sfMapKeyEnd
	}

	l.next()
	return sfMapKey

}

func sfMapKeyEnd(l *lexer) stateFunc {

	r := l.next()

	switch {
	case l.lookup[r]&whitespace != 0:
		return sfSkip(l, sfMapKeyEnd)
	case r == keyValueSep || r == keyValueEqSep:
		return sfSkip(l, sfMapValue)
	}

	l.backup()

	return sfMapValue
}

func sfMapValue(l *lexer) stateFunc {

	r := l.next()

	switch {
	case l.lookup[r]&whitespace != 0:
		return sfSkip(l, sfMapValue)
	case r == mapValSep:
		return l.emitError("unexpected map terminator - (%s)", string(r))
	case r == mapEnd:
		return sfSkip(l, sfMapEnd)
	}

	l.backup()
	l.push(sfMapValueEnd)
	return sfValue
}

func sfMapValueEnd(l *lexer) stateFunc {

	r := l.next()

	switch {
	case r == optValEnd || r == mapValSep || r == '\n' || r == '\r': // We put this at the top to not skip the whitespace which could signal a termination of map value
		return sfSkip(l, sfMapKeyStart)
	case l.lookup[r]&whitespace != 0:
		return sfSkip(l, sfMapValueEnd)
	case l.lookup[r]&comment != 0:
		l.push(sfMapValueEnd)
		return sfCommentStart
	case r == mapEnd:
		return sfSkip(l, sfMapEnd)
	}

	return l.emitError("expected a terminator (%q | %q) or map end (%q) but got (%s) instead", mapValSep, optValEnd, mapEnd, string(r))
}

func sfMapEnd(l *lexer) stateFunc {
	l.ignore()
	log.Printf("emitting map end")
	l.emit(tokenMapEnd)
	return l.pop()
}

func dQuotedString(l *lexer) stateFunc {

	r := l.next()

	switch {

	case r == '\\':
		l.addToStringParts(1)
		return sfEscapedString

	case r == dStringEnd:
		l.backup() // We back up so we don't emit the end quote char
		l.emitString()
		l.next()       // We move back to the quote
		l.ignore()     // And then ignore to move past and reset the position and start
		return l.pop() // Pop then brings us back up the stack in state functions

	case r == eof:
		if l.pos > l.start {
			return l.emitError("Unexpected EOF")
		}
		l.emit(tokenEOF)
		return nil
	}

	return dQuotedString
}

func sQuotedString(l *lexer) stateFunc {

	r := l.next()

	l.printPosition()

	switch {
	case r == sStringEnd:
		l.backup()
		l.emit(tokenString)
		l.next()
		return l.pop()
	case r == eof:
		if l.pos > l.start {
			return l.emitError("Unexpected EOF")
		}
		l.emit(tokenEOF)
		return nil
	}

	return sQuotedString

}

func sfStringValue(l *lexer) stateFunc {

	r := l.next()

	log.Printf("string value = %s", string(r))

	if r == '\\' {
		log.Printf("found escaped string")
		l.addToStringParts(1)
		return sfEscapedString
	}

	if r == eof || l.lookup[r]&whitespace != 0 || r == arrayEnd || r == arrayValSep || r == mapEnd {

		l.backup()
		log.Printf("found terminator (%s) - backing up and popping from stack", string(r))
		if l.stringState != nil {
			l.emitString()

			// Add other checks such as variable/bool etc

		} else {
			l.emitString()
		}

		return l.pop()
	}

	if r == sStringEnd {
		log.Printf("found end string")
		l.backup()
		l.emitString()
		l.next()
		l.ignore()
		return l.pop()
	}

	return sfStringValue

}

func sfEscapedString(l *lexer) stateFunc {

	// If we are here, we need to determine what type of escaped string we are dealing with
	// \t \n \r \" \\

	// We add the string part to the stringParts and return the stringStateFunc which we have stored

	r := l.next()

	switch r {
	case 't':
		return l.addStringPart("\t")
	case 'n':
		return l.addStringPart("\n")
	case 'r':
		return l.addStringPart("\r")
	case '"':
		return l.addStringPart("\"")
	case '\\':
		return l.addStringPart("\\")
	}

	return l.emitError("Invalid escape sequence '%s' - escapes allowed [\\t, \\n, \\r, \\\", \\\\]", string(r))

}

func sfDigitStart(l *lexer) stateFunc {

	r := l.next()

	log.Printf("digit start = %s", string(r))

	if !unicode.IsDigit(r) {
		return l.emitError("expected digit but got (%s)", string(r))
	}

	return sfDigit

}

func sfDigit(l *lexer) stateFunc {

	//TODO Here we can do a few things like NATS does with convenient numbers (1kb etc) if we want to but for our config use case
	// simple numbers and IPs are ok for now

	r := l.next()

	switch {

	case l.lookup[r]&digit != 0:
		return sfDigit

	case r == '.':
		// We could process float here, but we currently do not have floats available in our config
		return sfIPStart

	case !(r == mapEnd || r == keyValueSep || r == keyValueEqSep || l.lookup[r]&whitespace != 0 || l.lookup[r]&digit != 0):
		l.stringState = sfStringValue
		return sfStringValue

	}

	l.backup()
	l.emit(tokenInteger)
	return l.pop()

}

func sfIPStart(l *lexer) stateFunc {

	r := l.next()

	if !unicode.IsDigit(r) {
		return l.emitError("expected digit but got (%s)", string(r))
	}

	return sfIP

}

func sfIP(l *lexer) stateFunc {

	r := l.next()

	if l.lookup[r]&digit != 0 || r == '.' || r == ':' || r == '-' {
		return sfIP
	}

	l.backup()
	l.emit(tokenString)
	return l.pop()

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

func sfCommentStart(l *lexer) stateFunc {
	l.emit(tokenCommentStart)
	l.ignore()
	return sfComment
}

func sfComment(l *lexer) stateFunc {

	r := l.peek()
	if r == '\n' || r == '\r' || r == eof {
		l.emit(tokenText)
		return l.pop()
	}

	l.next()
	return sfComment
}

func (l *lexer) printCurrentToken() {

	log.Printf("current token = %s", l.input[l.start:l.pos])

}

func (l *lexer) printPosition() {
	log.Printf("start = %d | position = %d", l.start, l.pos)
}

func (l *lexer) isBool() bool {

	str := strings.ToLower(l.input[l.start:l.pos])
	return str == "true" || str == "false" || str == "on" || str == "off" || str == "yes" || str == "no"

}

func (l *lexer) isVariable() bool {

	if l.start >= len(l.input) {
		return false
	}

	if l.input[l.start] == '$' {
		l.start += 1
		return true
	}

	return false

}
