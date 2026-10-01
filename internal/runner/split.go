package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/token"
	"github.com/go-playground/errors/v5"
)

// Kind is what a Spanner statement is: DDL goes to the database admin API as part of a
// batch, DML runs inside a read-write transaction.
type Kind int

const (
	// DDL is a schema statement.
	DDL Kind = iota
	// DML is an INSERT, UPDATE or DELETE statement.
	DML
)

// String names the kind in messages.
func (k Kind) String() string {
	if k == DML {
		return "DML"
	}

	return "DDL"
}

// Group is a run of consecutive statements of one kind, in file order.
type Group struct {
	Kind Kind
	// Start is the index of the group's first statement in the file's statements.
	Start      int
	Statements []string
}

// Split splits a Spanner migration file into its statements. The split honors semicolons
// inside string literals and comments; every statement comes back with its comments
// stripped, since Spanner's DDL call refuses them, and its leading and trailing whitespace
// trimmed. Empty statements are dropped, so a file that is only comments splits to nothing.
func Split(sql string) ([]string, error) {
	raws, err := memefish.SplitRawStatements("", sql)
	if err != nil {
		return nil, errors.Wrap(err, "memefish.SplitRawStatements()")
	}

	stmts := make([]string, 0, len(raws))
	for _, raw := range raws {
		stmt, err := strip(raw.Statement)
		if err != nil {
			return nil, err
		}
		if stmt == "" {
			continue
		}
		stmts = append(stmts, stmt)
	}

	return stmts, nil
}

// strip rebuilds a statement from its tokens' raw text, which leaves the comments out and
// keeps the whitespace between tokens, inner newlines included. Where a comment sat between
// two tokens with no whitespace of its own, one space keeps them apart.
func strip(raw string) (string, error) {
	lex := &memefish.Lexer{File: &token.File{Buffer: raw}}

	var b strings.Builder
	for {
		if err := lex.NextToken(); err != nil {
			return "", errors.Wrap(err, "memefish.Lexer.NextToken()")
		}
		if lex.Token.Kind == token.TokenEOF {
			break
		}

		if b.Len() > 0 {
			b.WriteString(separator(&lex.Token))
		}
		b.WriteString(lex.Token.Raw)
	}

	return b.String(), nil
}

// separator is the whitespace that goes before a token once the comments ahead of it are
// dropped: the whitespace around each comment, the line break a line comment ends with, and
// the whitespace before the token itself.
func separator(t *token.Token) string {
	if len(t.Comments) == 0 {
		return t.Space
	}

	var b strings.Builder
	for _, c := range t.Comments {
		b.WriteString(c.Space)
		if strings.HasSuffix(c.Raw, "\n") {
			b.WriteString("\n")
		}
	}
	b.WriteString(t.Space)
	if b.Len() == 0 {
		return " "
	}

	return b.String()
}

// KindOf classifies a statement by its first token: INSERT, UPDATE and DELETE are DML,
// everything else is DDL.
func KindOf(stmt string) Kind {
	lex := &memefish.Lexer{File: &token.File{Buffer: stmt}}
	if err := lex.NextToken(); err != nil {
		return DDL
	}

	for _, keyword := range []string{"INSERT", "UPDATE", "DELETE"} {
		if lex.Token.IsKeywordLike(keyword) {
			return DML
		}
	}

	return DDL
}

// Groups cuts the statements from index start on into runs of one kind, in file order.
func Groups(stmts []string, start int) []Group {
	var groups []Group
	for i := start; i < len(stmts); i++ {
		kind := KindOf(stmts[i])
		if len(groups) == 0 || groups[len(groups)-1].Kind != kind {
			groups = append(groups, Group{Kind: kind, Start: i})
		}
		last := &groups[len(groups)-1]
		last.Statements = append(last.Statements, stmts[i])
	}

	return groups
}

// Checkpoint is the hex SHA-256 over the statements' text, each statement preceded by its
// length so that no two statement lists hash alike: the record of which statements a stopped
// file had applied, so a later run can tell whether the file changed in that part. Zero
// statements hash to the SHA-256 of nothing.
func Checkpoint(stmts []string) string {
	h := sha256.New()
	for _, stmt := range stmts {
		fmt.Fprintf(h, "%d\n%s\n", len(stmt), stmt)
	}

	return hex.EncodeToString(h.Sum(nil))
}
