package dbinitiator

import "github.com/cccteam/db-initiator/internal/runner"

// Version is what a migrations table says about a database: no version yet, clean at a
// version, or dirty at one with the progress the runner recorded. Its String prints one
// phrase, such as "version 41, dirty: 2 statements applied".
type Version = runner.Version

// DirtyError is returned when a Spanner migration file stops before its end: the version
// is dirty with the applied part recorded, and the next run continues from the failed
// statement once the cause is fixed. Test for it with errors.As.
type DirtyError = runner.DirtyError

// MigrationsReaderRole is the database role the migration runner keeps beside every
// version table on Spanner, with select on that table alone (CREATE ROLE and GRANT SELECT
// at the first run that finds it missing): a deploy identity of another environment,
// given the role, reads where a database stands without reaching its data.
const MigrationsReaderRole = runner.ReaderRole
