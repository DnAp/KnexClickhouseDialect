/* eslint-disable consistent-return */
// ClickHouse Client
// -------
const Client = require('knex/lib/client');
const QueryCompiler = require('./query/compiler');
const SchemaCompilerClickHouse = require('./schema/compiler');
const _ = require('lodash');
const makeEscape = require("knex/lib/util/string").makeEscape;
const TableBuilder = require("knex/lib/schema/tablebuilder");
const sqlString = require('sqlstring');

const ColumnCompilerClickHouse = require('./schema/columncompiler');
const TableCompilerClickHouse = require('./schema/tablecompiler');
const TransactionClickHouse = require('./transaction');

const clickhouse = require("clickhouse");

function ltrimSlashes(v) {
    return v.replace(/^\//g, '');
}

// Always initialize with the "QueryBuilder" and "QueryCompiler"
// objects, which extend the base 'lib/query/builder' and
// 'lib/query/compiler', respectively.
/**
 * @implements {Knex.Client}
 */
class ClientClickhouse extends Client {
    dialect = 'clickhouse';

    driverName = 'clickhouse';

    _escapeBinding = makeEscape();

    canCancelQuery = true;

    _migrationLockTableName = '`knex_migrations_lock`';

    constructor(config = {}) {
        super(config);
        this.initializeDriver();
        this.initializePool();
        if(config.migrations && config.migrations.tableName) {
            const migrationCfg = config.migrations;
            if(migrationCfg.schemaName) {
                this._migrationLockTableName = '`' + migrationCfg.schemaName + '`.`' + migrationCfg.tableName + '_lock`';
            }else {
                this._migrationLockTableName = '`' + migrationCfg.tableName + '_lock`';
            }
        }
    }

    _driver() {
        return clickhouse;
    }

    tableBuilder(type, tableName, tableNameLike, fn) {
        const builder = new TableBuilder(this, type, tableName, tableNameLike, fn);
        builder.engine = function (val) {
            this._single.engine = val;
        }
        return builder;
    }

    queryCompiler(...args) {
        return new QueryCompiler(this, ...args);
    }

    schemaCompiler(builder) {
        return new SchemaCompilerClickHouse(this, builder);
    }

    tableCompiler(tableBuilder) {
        return new TableCompilerClickHouse(this, tableBuilder);
    }

    columnCompiler() {
        return new ColumnCompilerClickHouse(this, ...arguments);
    }

    async transaction() {
        return new TransactionClickHouse(this, ...arguments);
    }

    wrapIdentifierImpl(value) {
        return value !== '*' ? `\`${value.replace(/`/g, '``')}\`` : '*';
    }

    // Get a raw connection, called by the `pool` whenever a new
    // connection needs to be added to the pool.
    async acquireRawConnection() {
        let config = await this.getConfiguration();
        return new (this.driver.ClickHouse)(config);
    }

    async getConfiguration() {
        let config = this.config.connection;
        if (_.isFunction(config)) {
            config = await config();
        }
        if (_.isString(config)) {
            const url = new URL(config);
            config = {
                url: url.hostname,
                port: url.port ? url.port : 8123,
                user: url.username,
                password: url.password,
                database: ltrimSlashes(url.pathname),
                debug: false,
                basicAuth: null,
                isUseGzip: false,
                config: {
                    session_timeout: 60,
                    output_format_json_quote_64bit_integers: 0,
                    enable_http_compression: 0,
                },
            };
        }

        return config;
    }

    // Used to explicitly close a connection, called internally by the pool
    // when a connection times out or the pool is shutdown.
    destroyRawConnection(connection) {
        connection.destroy();
    }

    // eslint-disable-next-line no-unused-vars
    validateConnection(connection) {
        return true;
    }

    // Grab a connection, run the query via the MySQL streaming interface,
    // and pass that through to the stream we've sent back to the client.
    _stream(connection, obj, stream, options) {
        options = options || {};
        const queryOptions = _.assign({ sql: obj.sql }, obj.options);
        return new Promise((resolver, rejecter) => {
            stream.on('error', rejecter);
            stream.on('end', resolver);
            const queryStream = connection
                .query(queryOptions, obj.bindings)
                .stream(options);

            queryStream.on('error', (err) => {
                rejecter(err);
                stream.emit('error', err);
            });

            queryStream.pipe(stream);
        });
    }

    /**
     * Runs the query on the specified connection, providing the bindingsand any other necessary prep work.
     */
    async _query(connection, obj) {
        if (!obj || typeof obj === 'string')
            obj = { sql: obj };
        // dirty hack for knex-migrator
        if(['insert', 'update'].includes(obj.method) && obj.sql.indexOf(this._migrationLockTableName) > -1) {
            obj.response = [[], []]
            return obj;
        }
        return new Promise((resolver, rejecter) => {
            if (!obj.sql) {
                resolver();
                return;
            }

            const queryOptions = _.assign({ sql: obj.sql }, obj.options);
            const query = this._applyBindings(queryOptions.sql, obj.bindings);

            connection.query(query, (err, rows, fields) => {
                if (err)
                    return rejecter(err);
                obj.response = [rows, fields];
                resolver(obj);
            });
        });
    }

    _applyBindings(sql, bindings) {
        for (let i = 0; i < bindings.length; i++) {
            if (bindings[i] instanceof Date) {
                bindings[i] = bindings[i].toISOString()
                    .replace(/T/, ' ')
                    .replace(/\..+/, '');
            }
        }
        return sqlString.format(sql, bindings);
    }

    // Process the response as returned from the query.
    processResponse(obj, runner) {
        if (obj == null)
            return;
        const { response } = obj;
        const { method } = obj;
        const rows = response[0];
        const fields = response[1];
        if (obj.output)
            return obj.output.call(runner, rows, fields);
        switch (method) {
            case 'select':
            case 'pluck':
            case 'first': {
                if (method === 'pluck')
                    return _.map(rows, obj.pluck);
                return method === 'first' ? rows[0] : rows;
            }
            case 'insert':
                return [rows.insertId];
            case 'del':
            case 'update':
            case 'counter':
                return rows.affectedRows;
            default:
                return response;
        }
    }

    cancelQuery(connectionToKill) {
        const acquiringConn = this.acquireConnection();

        // Error out if we can't acquire connection in time.
        // Purposely not putting timeout on `KILL QUERY` execution because erroring
        // early there would release the `connectionToKill` back to the pool with
        // a `KILL QUERY` command yet to finish.
        return acquiringConn
            .timeout(100)
            .then((conn) => this.query(conn, {
                method: 'raw',
                sql: 'KILL QUERY ?',
                bindings: [connectionToKill.threadId],
                options: {},
            }))
            .finally(() => {
                // NOT returning this promise because we want to release the connection
                // in a non-blocking fashion
                acquiringConn.then((conn) => this.releaseConnection(conn));
            });
    }
}

module.exports = ClientClickhouse;
