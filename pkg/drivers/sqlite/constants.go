package sqlite

// The database version that designates whether table migration
// from the key_value table to the Kine table has been done.
// Databases with this version should not have the key_value table
// present anymore, and unexpired rows of the key_value table with
// the latest revisions must have been recorded in the Kine table
// already
const databaseSchemaVersion = 1
