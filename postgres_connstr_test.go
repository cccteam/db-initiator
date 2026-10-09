package dbinitiator

import (
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestPostgresConnStr: the URL carries no bare space, pgx parses it back to the user, the
// password and the database name it was built from, and the sslmode is the one asked for.
func TestPostgresConnStr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		user        *url.Userinfo
		database    string
		sslMode     SSLMode
		wantSSLMode SSLMode
	}{
		{name: "plain values", user: url.UserPassword("u", "p"), database: "db", sslMode: SSLModeRequire, wantSSLMode: SSLModeRequire},
		{name: "an empty sslMode is require", user: url.UserPassword("u", "p"), database: "db", wantSSLMode: SSLModeRequire},
		{name: "a database name with spaces", user: url.UserPassword("u", "p"), database: "schema forced below -1 refuses", sslMode: SSLModeDisable, wantSSLMode: SSLModeDisable},
		{name: "reserved characters in the user, the password and the name", user: url.UserPassword("us er", "p@ss/w?rd#1"), database: "a/b?c", sslMode: SSLModeDisable, wantSSLMode: SSLModeDisable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			password, _ := tt.user.Password()
			got := PostgresConnStr(tt.user.Username(), password, "localhost", "5432", tt.database, tt.sslMode)
			if strings.Contains(got, " ") {
				t.Errorf("PostgresConnStr() = %q carries a bare space", got)
			}
			config, err := pgx.ParseConfig(got)
			if err != nil {
				t.Fatalf("pgx.ParseConfig(%q) error = %v", got, err)
			}
			if config.User != tt.user.Username() || config.Password != password || config.Database != tt.database || config.Host != "localhost" || config.Port != 5432 {
				t.Errorf("pgx.ParseConfig(%q) = user %q, password %q, database %q, host %q, port %d; want %q, %q, %q, localhost, 5432", got, config.User, config.Password, config.Database, config.Host, config.Port, tt.user.Username(), password, tt.database)
			}
			u, err := url.Parse(got)
			if err != nil {
				t.Fatalf("url.Parse(%q) error = %v", got, err)
			}
			if mode := u.Query().Get("sslmode"); mode != string(tt.wantSSLMode) {
				t.Errorf("sslmode = %q, want %q", mode, tt.wantSSLMode)
			}
		})
	}
}
