package config

// Lexer inspired by https://github.com/nats-io/nats-server/blob/main/conf/lex.go
// Check out the NATS implementation for an example of what a real lexer should look like

type configItemType int

const (
	ciError configItemType = iota
	ciNIL
	ciEOF
	ciSection
	ciColon
	ciKey
	ciText
	ciString
	ciBool
	ciInteger
	ciFloat
	ciArrayStart
	ciArrayEnd
	ciMapStart
	ciMapEnd
	ciCommentStart
)

const (
	eof         = 0
	keyValueSep = ":"
	arrayStart  = "["
	arrayEnd    = "]"
	mapStart    = "{"
	mapEnd      = "}"
	commentSep  = "#"
)

// stateFunc is a function that takes the lexer, reads and emits tokens, returns the next state function to run
type stateFunc func(*lexer) stateFunc

type token struct {
	typ   configItemType
	value string
	line  int
	pos   int
}

type lexer struct {
	input string
	start int
	pos   int
	width int
	line  int

	stack []stateFunc

	stringParts []string
}
