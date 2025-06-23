package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

type ParseContext int

const (
	ContextRoot ParseContext = iota
	ContextArray
	ContextMap
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

func (p *parser) addToParent(child Node, key string) error {

	currCtx := p.peekContext()

	// If we're nested under a KeyValueNode (i.e., context is a kv pair), unwrap the value
	if kv, ok := currCtx.node.(*KeyValueNode); ok {
		// Redirect to the inner node, which should be a ListNode or ObjectNode
		switch val := kv.Value.(type) {
		case *ListNode:
			val.Items = append(val.Items, child)
		case *ObjectNode:
			if kvChild, ok := child.(*KeyValueNode); ok {
				val.Children = append(val.Children, kvChild)
			} else {
				val.Children = append(val.Children, &KeyValueNode{key, child})
			}
		default:
			return fmt.Errorf("unexpected child type for KeyValueNode: %T", val)
		}
		return nil
	}

	switch currCtx.ctx {

	case ContextRoot:

		root, ok := currCtx.node.(*RootConfig)
		if !ok {
			return fmt.Errorf("expected root config, got %T", currCtx.ctx)
		}

		root.Nodes = append(root.Nodes, child)

	case ContextArray:

		list, ok := currCtx.node.(*ListNode)
		if !ok {
			return fmt.Errorf("expected list node, got %T", currCtx.ctx)
		}

		list.Items = append(list.Items, child)

	case ContextMap:

		obj, ok := currCtx.node.(*ObjectNode)
		if !ok {
			return fmt.Errorf("expected object node, got %T", currCtx.ctx)
		}

		if kv, ok := child.(*KeyValueNode); ok {
			obj.Children = append(obj.Children, kv)
		} else {
			// If we get a raw node and key, wrap it
			obj.Children = append(obj.Children, &KeyValueNode{key, child})
		}

	default:
		return fmt.Errorf("unknown context %v", currCtx.ctx)
	}

	return nil

}

func ParseConfig(data string) (*RootConfig, error) {

	cfg := &RootConfig{}

	p := &parser{lex: lex(data)}
	if tok := p.next(); tok.typ == tokenEOF || tok.typ == tokenError {
		return nil, fmt.Errorf("error in token stream when lexing data")
	}

	// Establish context frame before entering the loop
	p.pushContext(ContextRoot, cfg, "")

	for {

		t := p.peek()

		if t.typ == tokenEOF {
			break
		}

		if t.typ == tokenError {
			// May want to dump error logs?
			break
		}

		// We will only break when we have either EOF or Error
		// We can then use this loop to have a manual stack with no recursion

		_, err := p.parseToken()
		if err != nil {
			return nil, err
		}

	}

	ctx := p.popContext()

	if config, ok := ctx.node.(*RootConfig); !ok {
		return nil, fmt.Errorf("wrong config type for last context on stack - got (%T) wanted (RootConfig)", ctx.node)
	} else {
		return config, nil
	}

}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

func ParseConfigFromFile(filePath string) (*RootConfig, error) {

	data, err := os.ReadFile(filePath)
	checkErr(err)

	return ParseConfig(string(data))

}

func (p *parser) parseToken() (token, error) {

	t := p.next()

	switch t.typ {

	case tokenEOF:
		return t, nil

	case tokenError:
		return t, fmt.Errorf("received error token")

	case tokenKey:
		if err := p.parseKey(t); err != nil {
			log.Printf("error parsing key - %s", err)
		}

	// Account for all starts as potential nested children

	case tokenMapStart:

		//tok := p.next()

		obj := &ObjectNode{
			make([]*KeyValueNode, 0, 4),
		}

		p.pushContext(ContextMap, obj, "")

		err := p.parseMap(t)
		if err != nil {
			return t, err
		}

	case tokenArrayStart:

		list := &ListNode{
			make([]Node, 0, 4),
		}

		p.pushContext(ContextArray, list, "")

		err := p.parseList(t)
		if err != nil {
			return t, err
		}

	case tokenMapEnd:

		ctx := p.popContext()
		if ctx.ctx != ContextMap {
			return t, fmt.Errorf("expected ContextMap, got %T", ctx.node)
		}
		err := p.addToParent(ctx.node, ctx.key)
		if err != nil {
			return t, fmt.Errorf("error closing map context: %v", err)
		}

	}

	return t, nil

}

func (p *parser) parseKey(t token) error {

	next := p.peek()

	key := t.value

	switch {

	case next.typ == tokenEOF || next.typ == tokenError:
		return fmt.Errorf("next token is invalid - got (%T) expected either Value, List or Map", next.typ)

	case next.typ == tokenString:
		p.next()
		kv := &KeyValueNode{key, &StringNode{next.value}}
		err := p.addToParent(kv, key)
		if err != nil {
			return err
		}

	case next.typ == tokenInteger:
		p.next()
		intValue, err := strconv.Atoi(next.value)
		if err != nil {
			return err
		}
		kv := &KeyValueNode{key, &DigitNode{Value: intValue}}
		err = p.addToParent(kv, key)
		if err != nil {
			return err
		}

	case next.typ == tokenBool:
		p.next()
		boolValue, err := strconv.ParseBool(next.value)
		if err != nil {
			return err
		}
		kv := &KeyValueNode{key, &BoolNode{boolValue}}
		err = p.addToParent(kv, key)
		if err != nil {
			return err
		}

	case next.typ == tokenArrayStart:
		tok := p.next() // consume tokenArrayStart
		list := &ListNode{Items: make([]Node, 0, 4)}
		kv := &KeyValueNode{key, list}
		p.pushContext(ContextArray, list, key)
		err := p.parseList(tok)
		if err != nil {
			return err
		}
		err = p.addToParent(kv, key)
		if err != nil {
			return err
		}

	case next.typ == tokenMapStart:

		tok := p.next()

		obj := &ObjectNode{
			make([]*KeyValueNode, 0, 4),
		}

		kv := &KeyValueNode{key, obj}

		p.pushContext(ContextMap, kv, key)
		err := p.parseMap(tok)
		if err != nil {
			return err
		}

	case next.typ == tokenMapEnd:
		p.next()
		ctx := p.popContext()
		if ctx.ctx != ContextMap {
			return fmt.Errorf("expected ContextMap, got %T", ctx.node)
		}
		return p.addToParent(ctx.node, ctx.key)

	}

	return nil

}

func (p *parser) parseList(t token) error {
	// Defensive: ensure tokenArrayStart was consumed
	if t.typ != tokenArrayStart {
		return fmt.Errorf("expected tokenArrayStart, got %v", t.typ)
	}

	for {
		next := p.peek()

		switch {
		case next.typ == tokenEOF || next.typ == tokenError:
			return fmt.Errorf("error in parsing list - line %d - position %v", t.line, t.pos)

		case next.typ == tokenKey:
			return fmt.Errorf("tokenKey not a valid entry in list")

		case next.typ == tokenString:
			p.next()
			err := p.addToParent(&StringNode{next.value}, "")
			if err != nil {
				return err
			}
			continue

		case next.typ == tokenInteger:
			p.next()
			intValue, err := strconv.Atoi(next.value)
			if err != nil {
				return err
			}
			err = p.addToParent(&DigitNode{intValue}, "")
			if err != nil {
				return err
			}
			continue

		case next.typ == tokenBool:
			log.Printf("-------------------parsing bool = %v", next.value)
			p.next()
			boolValue, err := strconv.ParseBool(next.value)
			if err != nil {
				return err
			}
			err = p.addToParent(&BoolNode{boolValue}, "")
			if err != nil {
				return err
			}
			continue

		case next.typ == tokenMapStart:
			tok := p.next()
			obj := &ObjectNode{
				make([]*KeyValueNode, 0, 4),
			}
			p.pushContext(ContextMap, obj, "")
			err := p.parseMap(tok)
			if err != nil {
				return err
			}

			continue

		case next.typ == tokenArrayEnd:
			p.next()
			ctx := p.popContext()
			if ctx.ctx != ContextArray {
				return fmt.Errorf("expected ContextArray, got %T", ctx.ctx)
			}
			return nil

		default:
			return fmt.Errorf("unexpected token %v in list", next.typ)
		}
	}
}

func (p *parser) parseMap(t token) error {
	// Defensive: ensure tokenMapStart was consumed
	if t.typ != tokenMapStart {
		return fmt.Errorf("expected tokenMapStart, got %v", t.typ)
	}

	for {
		next := p.peek()

		switch {
		case next.typ == tokenKey:
			err := p.parseKey(p.next())
			if err != nil {
				return err
			}

		case next.typ == tokenMapEnd:
			p.next()
			ctx := p.popContext()
			if ctx.ctx != ContextMap {
				return fmt.Errorf("expected ContextMap, got %T", ctx.node)
			}
			return p.addToParent(ctx.node, ctx.key)

		case next.typ == tokenEOF:
			return fmt.Errorf("unexpected EOF while parsing map")

		default:
			return fmt.Errorf("unexpected token %v in map", next.typ)
		}
	}
}
