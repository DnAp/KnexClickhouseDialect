// Clickhouse Query Compiler
// ------
const QueryCompiler = require('knex/lib/query/querycompiler');
const {dateToString} = require('knex/lib/util/string');
const _ = require('lodash');

class QueryCompilerClickhouse extends QueryCompiler {
    _emptyInsertValue = '() values ()';

    constructor(client, builder, bindings) {
        super(client, builder, bindings);
        const {returning} = this.single;

        if (returning) {
            this.client.logger.warn(
                '.returning() is not supported by Clickhouse and will not have any effect.',
            );
        }
    }

    // Update method, including joins, wheres, order & limits.
    update() {
        const {tableName} = this;
        const updateData = this._prepUpdate(this.single.update);
        const wheres = this.where();
        return `ALTER TABLE ${tableName} UPDATE ` +
            updateData.join(', ') +
            (wheres ? ` ${wheres}` : '');
    }

    insert() {
        let rows = this.single.insert;
        if (!(rows instanceof Array)) {
            rows = [rows];
        }
        const values = [];
        for (const originalRow of rows) {
            const row = {...originalRow};
            for (const key in row) {
                if (row[key] instanceof Date) {
                    row[key] = dateToString(row[key]);
                } else if (row[key] === undefined) {
                    row[key] = null;
                }
            }
            values.push(row);
        }

        return {
            sql: 'insert',
            bindings: {
                table: this.tableName,
                values,
                clickhouse_settings: {date_time_input_format: 'best_effort'},
            }
        };
        // // insertData
        // this._ooo = 1;
        // this.single._ooo= 2;
        // return {sql: 'INSERT INTO ' + tableName + ' FORMAT JSONEachRow \n', bindings: values};
    }

    // Compiles a `columnInfo` query.
    columnInfo() {
        const column = this.single.columnInfo;

        // The user may have specified a custom wrapIdentifier function in the config. We
        // need to run the identifiers through that function, but not format them as
        // identifiers otherwise.
        const table = this.client.customWrapIdentifier(this.single.table, _.identity);

        return {
            sql:
                'select * from information_schema.columns where table_name = ? and table_schema = ?',
            bindings: [table, this.client.database()],
            output(resp) {
                const out = resp.reduce(function r(columns, val) {
                    columns[val.COLUMN_NAME] = {
                        defaultValue: val.COLUMN_DEFAULT,
                        type: val.DATA_TYPE,
                        maxLength: val.CHARACTER_MAXIMUM_LENGTH,
                        nullable: val.IS_NULLABLE === 'YES',
                    };
                    return columns;
                }, {});
                return (column && out[column]) || out;
            },
        };
    }

    limit() {
        // Workaround for offset only.
        // see: http://stackoverflow.com/questions/255517/mysql-offset-infinite-rows
        if (this.single.offset && !this.single.limit && this.single.limit !== 0)
            return 'limit 18446744073709551615';

        return super.limit();
    }
}

// Set the QueryBuilder & QueryCompiler on the client object,
// in case anyone wants to modify things to suit their own purposes.
module.exports = QueryCompilerClickhouse;
