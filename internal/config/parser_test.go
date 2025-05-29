package config

import (
	"log"
	"testing"
)

func TestParserToken(t *testing.T) {

	d := `key1: "value2"`

	token, err := parseConfig(d)
	if err != nil {
		t.Error(err)
	}

	log.Println(token)

}
