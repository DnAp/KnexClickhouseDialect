// ClicHouse Table Builder & Compiler
// -------
import TableCompiler from "knex/lib/schema/tablecompiler.js";
// Table Compiler
// ------

export default class TableCompilerClickHouse extends TableCompiler {
    addColumnsPrefix = 'add ';

    alterColumnsPrefix = 'modify ';

    dropColumnPrefix = 'drop ';

    createQuery(columns, ifNot) {
        const createStatement = ifNot ? 'create table if not exists ' : 'create table ';
        let sql = createStatement + this.tableName() + ' (' + columns.sql.join(', ') + ')';

        const engine = this.single.engine || 'TinyLog';

        if (engine)
            sql += ` engine = ${engine}`;

        if (this.single.comment) {
            const comment = this.single.comment || '';
            if (comment.length > 60)
                this.client.logger.warn('The max length for a table comment is 60 characters');
            sql += ` comment = '${comment}'`;
        }

        this.pushQuery(sql);
    }

    // Compiles the comment on the table.
    comment(comment) {
        this.pushQuery(`alter table ${this.tableName()} comment = '${comment}'`);
    }

    changeType() {
        // alter table + table + ' modify ' + wrapped + '// type';
    }

    // Renames a column on the table.
    renameColumn(from, to) {
        const compiler = this;
        const table = this.tableName();
        const wrapped = this.formatter.wrap(from) + ' ' + this.formatter.wrap(to);

        this.pushQuery({
            sql:
                `show fields from ${table} where field = `
                + this.formatter.parameter(from),
            output(resp) {
                const column = resp[0];
                const runner = this;
                return compiler.getFKRefs(runner)
                    .then(([refs]) => new Promise((resolve, reject) => {
                        try {
                            if (!refs.length) {
                                resolve();
                            }
                            resolve(compiler.dropFKRefs(runner, refs));
                        } catch (e) {
                            reject(e);
                        }
                    })
                        .then(function f() {
                            let sql = `alter table ${table} change ${wrapped} ${column.Type}`;

                            if (String(column.Null)
                                .toUpperCase() !== 'YES') {
                                sql += ' NOT NULL';
                            } else {
                                // This doesn't matter for most cases except Timestamp, where this is important
                                sql += ' NULL';
                            }
                            if (column.Default) {
                                sql += ` DEFAULT '${column.Default}'`;
                            }

                            return runner.query({
                                sql,
                            });
                        })
                        .then(function f() {
                            if (!refs.length) {
                                return undefined;
                            }
                            return compiler.createFKRefs(
                                runner,
                                refs.map(function m(ref) {
                                    if (ref.REFERENCED_COLUMN_NAME === from) {
                                        ref.REFERENCED_COLUMN_NAME = to;
                                    }
                                    if (ref.COLUMN_NAME === from) {
                                        ref.COLUMN_NAME = to;
                                    }
                                    return ref;
                                }),
                            );
                        }));
            },
        });
    }

    getFKRefs(runner) {
        // todo wtf?
        const formatter = this.client.formatter(this.tableBuilder);
        const sql = `SELECT KCU.CONSTRAINT_NAME, KCU.TABLE_NAME, KCU.COLUMN_NAME,
                KCU.REFERENCED_TABLE_NAME, KCU.REFERENCED_COLUMN_NAME,
                RC.UPDATE_RULE,
                RC.DELETE_RULE FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC
                USING(CONSTRAINT_NAME)WHERE KCU.REFERENCED_TABLE_NAME = ${formatter.parameter(this.tableNameRaw)}   AND KCU.CONSTRAINT_SCHEMA = ${formatter.parameter(this.client.database())}   AND RC.CONSTRAINT_SCHEMA = ${formatter.parameter(this.client.database())}`;

        return runner.query({
            sql,
            bindings: formatter.bindings,
        });
    }

    dropFKRefs(runner, refs) {
        const formatter = this.client.formatter(this.tableBuilder);

        return Promise.all(
            refs.map(function f(ref) {
                const constraintName = formatter.wrap(ref.CONSTRAINT_NAME);
                const tableName = formatter.wrap(ref.TABLE_NAME);
                return runner.query({
                    sql: `alter table ${tableName} drop foreign key ${constraintName}`,
                });
            }),
        );
    }

    createFKRefs(runner, refs) {
        const formatter = this.client.formatter(this.tableBuilder);

        return Promise.all(
            refs.map(function f(ref) {
                const tableName = formatter.wrap(ref.TABLE_NAME);
                const keyName = formatter.wrap(ref.CONSTRAINT_NAME);
                const column = formatter.columnize(ref.COLUMN_NAME);
                const references = formatter.columnize(ref.REFERENCED_COLUMN_NAME);
                const inTable = formatter.wrap(ref.REFERENCED_TABLE_NAME);
                const onUpdate = ` ON UPDATE ${ref.UPDATE_RULE}`;
                const onDelete = ` ON DELETE ${ref.DELETE_RULE}`;

                return runner.query({
                    sql:
                        `alter table ${tableName} add constraint ${keyName} `
                        + 'foreign key ('
                        + column
                        + ') references '
                        + inTable
                        + ' ('
                        + references
                        + ')'
                        + onUpdate
                        + onDelete,
                });
            }),
        );
    }

    index(columns, indexName, indexType) {
        indexName = indexName
            ? this.formatter.wrap(indexName)
            : this._indexCommand('index', this.tableNameRaw, columns);
        this.pushQuery(
            `alter table ${this.tableName()} add${
                indexType ? ` ${indexType}` : ''
            } index ${indexName}(${this.formatter.columnize(columns)})`,
        );
    }

    primary(columns, constraintName) {
        constraintName = constraintName
            ? this.formatter.wrap(constraintName)
            : this.formatter.wrap(`${this.tableNameRaw}_pkey`);
        this.pushQuery(
            `alter table ${this.tableName()} add primary key ${constraintName}(${this.formatter.columnize(
                columns,
            )})`,
        );
    }

    unique(columns, indexName) {
        indexName = indexName
            ? this.formatter.wrap(indexName)
            : this._indexCommand('unique', this.tableNameRaw, columns);
        this.pushQuery(
            `alter table ${this.tableName()} add unique ${indexName}(${this.formatter.columnize(
                columns,
            )})`,
        );
    }

    // Compile a drop index command.
    dropIndex(columns, indexName) {
        indexName = indexName
            ? this.formatter.wrap(indexName)
            : this._indexCommand('index', this.tableNameRaw, columns);
        this.pushQuery(`alter table ${this.tableName()} drop index ${indexName}`);
    }

    // Compile a drop foreign key command.
    dropForeign(columns, indexName) {
        indexName = indexName
            ? this.formatter.wrap(indexName)
            : this._indexCommand('foreign', this.tableNameRaw, columns);
        this.pushQuery(
            `alter table ${this.tableName()} drop foreign key ${indexName}`,
        );
    }

    // Compile a drop primary key command.
    dropPrimary() {
        this.pushQuery(`alter table ${this.tableName()} drop primary key`);
    }

    // Compile a drop unique key command.
    dropUnique(column, indexName) {
        indexName = indexName
            ? this.formatter.wrap(indexName)
            : this._indexCommand('unique', this.tableNameRaw, column);
        this.pushQuery(`alter table ${this.tableName()} drop index ${indexName}`);
    }
}
