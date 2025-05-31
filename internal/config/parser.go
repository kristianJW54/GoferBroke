package config

import (
	"fmt"
	"log"
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

		obj := &ObjectNode{
			make([]*KeyValueNode, 0, 4),
		}

		p.pushContext(ContextMap, obj, "")

		err := p.parseMap(p.next())
		if err != nil {
			return t, err
		}

	case tokenArrayStart:

		list := &ListNode{
			make([]Node, 0, 4),
		}

		p.pushContext(ContextArray, list, "")

		err := p.parseList(p.next())
		if err != nil {
			return t, err
		}

	case tokenArrayEnd:

		ctx := p.popContext()
		if ctx.ctx != ContextArray {
			return t, fmt.Errorf("expected ContextArray, got %T", ctx.node)
		}

		err := p.addToParent(ctx.node, ctx.key)
		if err != nil {
			return t, fmt.Errorf("error closing array context: %v", err)
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
		kv := &KeyValueNode{key, &StringNode{next.value}}
		err := p.addToParent(kv, key)
		if err != nil {
			return err
		}

	case next.typ == tokenInteger:
		intValue, err := strconv.Atoi(next.value)
		if err != nil {
			return err
		}
		kv := &KeyValueNode{key, &DigitNode{Value: intValue}}
		err = p.addToParent(kv, key)
		if err != nil {
			return err
		}

	case next.typ == tokenArrayStart:

		list := &ListNode{
			Items: make([]Node, 0, 4), // Pre-allocate to a reasonable capacity to avoid re-sizing
		}

		kv := &KeyValueNode{key, list}

		p.pushContext(ContextArray, kv, key)
		err := p.parseList(p.next())
		if err != nil {
			return err
		}

	case next.typ == tokenMapStart:

		obj := &ObjectNode{
			make([]*KeyValueNode, 0, 4),
		}

		kv := &KeyValueNode{key, obj}

		p.pushContext(ContextMap, kv, key)
		err := p.parseMap(p.next())
		if err != nil {
			return err
		}

	}

	return nil

}

func (p *parser) parseList(t token) error {

	next := p.peek()

	switch {

	case next.typ == tokenEOF || next.typ == tokenError:
		return fmt.Errorf("error in parsing list")

	case next.typ == tokenKey:
		return fmt.Errorf("tokenKey not a valid entry in list")

	case next.typ == tokenString:
		err := p.addToParent(&StringNode{next.value}, "")
		if err != nil {
			return err
		}
		return nil

	case next.typ == tokenMapStart:

		// If we encounter map here it is anonymous and does not have a key

		obj := &ObjectNode{
			make([]*KeyValueNode, 0, 4),
		}

		p.pushContext(ContextMap, obj, "")

		err := p.parseMap(p.next())
		if err != nil {
			return err
		}

	}

	return nil
}

func (p *parser) parseMap(t token) error {

	next := p.peek()

	key := t.value

	switch {

	case next.typ == tokenKey:

		err := p.parseKey(p.next())
		if err != nil {
			return err
		}

	case next.typ == tokenMapEnd:

		if ctx := p.popContext(); ctx.ctx == ContextMap {
			err := p.addToParent(ctx.node, key)
			if err != nil {
				return err
			}

		} else {
			return fmt.Errorf("wrong context on the stack - expected ContextMap - got (%T)", ctx.node)
		}

	default:
		return fmt.Errorf("expected map key, got (%T) instead", next.typ)
	}

	return nil

}
