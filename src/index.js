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

const clickhouse = require("@clickhouse/client");

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
        if (!config?.session_id) {
            config.session_id = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
        }
        return clickhouse.createClient(config);
    }

    async getConfiguration() {
        let config = this.config.connection;
        if (_.isFunction(config)) {
            config = await config();
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
        if (!obj.sql) {
            return;
        }
        if(obj.sql === 'insert') {
            const res = await connection.insert({
                format: 'JSONEachRow',
                ...obj.bindings
            });
            console.log(res);
        }
        const queryOptions = _.assign({ sql: obj.sql }, obj.options);
        const query = this._applyBindings(queryOptions.sql, obj.bindings);
        const response = await connection.exec({
            query,
            format: 'JSONEachRow',
        });
        obj.rawResponse = response;
        obj.rows = await response.json();
        return obj;
    }

    // Process the response as returned from the query.
    processResponse(obj, runner) {
        if (obj == null)
            return;

        const { response, method, rows } = obj;
        if (obj.output)
            return obj.output.call(runner, rows, null);
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
