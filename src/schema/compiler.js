// ClickHouse Schema Compiler
// -------
const SchemaCompiler = require('knex/lib/schema/compiler');

class SchemaCompilerClickHouse extends SchemaCompiler {
    // Rename a table on the schema.
    renameTable(tableName, to) {
        this.pushQuery(
            `rename table ${this.formatter.wrap(tableName)} to ${this.formatter.wrap(to)}`,
        );
    }

    // Check whether a table exists on the query.
    hasTable(tableName) {
        let sql = 'select name from system.tables where name = ? and database = currentDatabase()';
        const bindings = [tableName];

        this.pushQuery({
            sql,
            bindings,
            output: function output(resp) {
                return resp.length > 0;
            },
        });
    }

    // Check whether a column exists on the schema.
    hasColumn(tableName, column) {
        let sql = 'SELECT name FROM system.columns where table = ? and name = ? and database = currentDatabase()';
        const bindings = [tableName, column];

        this.pushQuery({
            sql,
            bindings,
            output: function output(resp) {
                return resp.length > 0;
            },
        });
    }
}
module.exports = SchemaCompilerClickHouse;
