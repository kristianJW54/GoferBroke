package config

import (
	"fmt"
	"log"
)

type ParseContext int

const (
	ContextRoot ParseContext = iota
	ContextArrayStart
	ContextArrayEnd
	ContextMapStart
	ContextMapEnd
	ContextEOF
)

type ParserCtx struct {
	ctx  ParseContext
	node Node
	key  string
}

type parser struct {
	lex *lexer

	cur token

	ctxStack []*ParserCtx
	currCtx  *ParserCtx

	fp string
}

func (p *parser) pushContext(ctx ParseContext, node Node, key string) {
	p.ctxStack = append(p.ctxStack, &ParserCtx{ctx, node, key})
}

func (p *parser) popContext() *ParserCtx {
	if len(p.ctxStack) == 0 {
		panic("stack underflow - context stack is empty")
	}

	// Do we check context type first before popping?

	ctx := p.ctxStack[len(p.ctxStack)-1]
	p.ctxStack = p.ctxStack[:len(p.ctxStack)-1]

	return ctx

}

func (p *parser) peekContext() *ParserCtx {
	if len(p.ctxStack) == 0 {
		panic("stack underflow - context stack is empty")
	}
	ctx := p.ctxStack[len(p.ctxStack)-1]
	return ctx
}

// Replace top context method?

func (p *parser) next() token {
	t := p.cur
	p.cur = p.lex.nextToken()
	return t
}

func (p *parser) peek() token {
	return p.cur
}

func parseConfig(data string) (*RootConfig, error) {

	cfg := &RootConfig{}

	p := &parser{lex: lex(data)}
	if tok := p.next(); tok.typ == tokenEOF || tok.typ == tokenError {
		return nil, fmt.Errorf("error in token stream when lexing data")
	}

	// Establish context frame before entering the loop
	p.pushContext(ContextRoot, cfg, "")

	for {

		t := p.peek()
		log.Printf("t = %s", t.value)

		if t.typ == tokenEOF {
			break
		}

		if t.typ == tokenError {
			// May want to dump error logs?
			break
		}

		// We will only break when we have either EOF or Error
		// We can then use this loop to have a manual stack with no recursion

		p.parseToken()

	}

	ctx := p.popContext()

	if config, ok := ctx.node.(*RootConfig); !ok {
		return nil, fmt.Errorf("wrong config type for last context on stack - got (%T) wanted (RootConfig)", ctx.node)
	} else {
		return config, nil
	}

}

func (p *parser) parseToken() token {

	t := p.next()

	switch t.typ {

	case tokenKey:
		log.Printf("found me a key boii --> %s", t.value)
		if err := p.parseKey(t); err != nil {
			log.Printf("error parsing key - %s", err)
		}
	}

	return t

}

func (p *parser) parseKey(t token) error {

	next := p.peek()

	switch {

	case next.typ == tokenEOF || next.typ == tokenError:
		return fmt.Errorf("next token is invalid - got (%T) expected either Value, List or Map", next.typ)

	case next.typ == tokenString:
		log.Printf("oh yeah baby --> %s", next.value)
	}

	return nil

}
