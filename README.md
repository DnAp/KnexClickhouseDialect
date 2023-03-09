# Knex ClickHouse dialect

ClickHouse dialect for Knex.js

## Install

```bash
npm install @dnap/knex-clickhouse-dialect
```

## Usage

```js
import knex from 'knex';
import clickhouse from '@dnap/knex-clickhouse-dialect';

export default knex({
    client: clickhouse,
    connection: () => {
        return 'clickhouse://login:password@localhost:8123/db_name';
    },
    // optional migrations config
    migrations: {
        directory: 'migrations_clickhouse',
        disableTransactions: true,
        disableMigrationsListValidation: true,
    },
});
```
