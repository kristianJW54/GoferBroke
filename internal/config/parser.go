package config

import "log"

type parser struct {
	lex *lexer

	fp string
}

func newParser() *parser {
	return &parser{}
}

func (p *parser) next() token {
	t := p.lex.nextToken()
	return t
}

func parse(data string) any {

	p := &parser{lex: lex(data)}

	t := p.next()

	for {

		if t.typ == tokenKey || t.typ == tokenEOF {
			log.Printf("token = %s", t.value)
			break
		}

	}

	// TODO test casting to ast struct and returning

	return nil

}
