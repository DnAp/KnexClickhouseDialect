const knex = require("knex");
const client = require("../src/index");
const expect = require("chai").expect;

const ClickHouseURL = "clickhouse://default@localhost:8123/default"

describe("Basic operations", () => {
    let db = knex({
        client: client,
        connection: () => ClickHouseURL,
    });

    it("Test connection", async () => {
        await db.raw("SELECT 1");
    });

    it("Create tables", async () => {
        await db.schema.dropTableIfExists("test");
        expect(await db.schema.hasTable("test")).to.be.equal(false);

        await db.schema
            .createTable("test", function (table) {
                table.increments("uuid");
                table.string("stringA");
                table.string("stringB").nullable();
                table.text("stringC");
                table.smallint("smallintA");
                table.mediumint("mediumintA");
                table.integer("integerA");
                table.bigint("bigintA");
                table.date("dateA");
                table.datetime("datetimeA");
                table.timestamp("datetimeB");
                table.engine("MergeTree order by dateA")
            });

        expect(await db.schema.hasTable("test")).to.be.equal(true);
        expect(await db.schema.hasColumn("test", "bigintA")).to.be.equal(true);
    });

    it("Insert data into tables", async () => {

        await db
            .insert({
                stringA: "test1",
                stringB: "test1",
                stringC: "test1",
                smallintA: 1,
                mediumintA: 1,
                integerA: 1,
                bigintA: 1,
                dateA: new Date(),
                datetimeA: new Date(),
                datetimeB: new Date(),
            })
            .into("test");

        await db
            .table("test")
            .insert({
                stringA: "test2",
                stringB: "test2",
                stringC: "test2",
                smallintA: 2,
                mediumintA: 2,
                integerA: 2,
                bigintA: 2,
                dateA: new Date(),
                datetimeA: new Date(),
                datetimeB: new Date(),
            });

        const users = await db.select("*").from("test").orderBy("smallintA");
        expect(users.length).to.be.equal(2);
        let user = users[0];
        expect(user.uuid).to.be.a("string");
        expect(user.stringA).to.be.equal("test1");
        expect(user.stringB).to.be.equal("test1");
        expect(user.stringC).to.be.equal("test1");
        expect(user.smallintA).to.be.equal(1);
        expect(user.mediumintA).to.be.equal(1);
        expect(user.integerA).to.be.equal(1);
        expect(user.bigintA).to.be.equal(1);
        expect(user.dateA).to.be.a("string");
        expect(user.datetimeA).to.be.a("string");
        expect(user.datetimeB).to.be.a("string");
        user = users[1];
        expect(user.stringA).to.be.equal("test2");
        expect(user.stringB).to.be.equal("test2");
        expect(user.stringC).to.be.equal("test2");
        expect(user.smallintA).to.be.equal(2);
        expect(user.mediumintA).to.be.equal(2);
        expect(user.integerA).to.be.equal(2);
        expect(user.bigintA).to.be.equal(2);
        expect(user.dateA).to.be.a("string");
        expect(user.datetimeA).to.be.a("string");
        expect(user.datetimeB).to.be.a("string");
    });

    it("Sorting", async () => {
        const rows = await db.select("smallintA").from("test").orderBy("smallintA", "desc");
        expect(rows).to.be.deep.equal([
            { smallintA: 2 },
            { smallintA: 1 },
        ]);
    });

    it("Filtering", async () => {
        const rows = await db("test").where({
            stringA: "test2",
            stringB: "test2",
        })
        expect(rows.length).to.be.equal(1);
        expect(rows[0].stringA).to.be.equal("test2");
    });
    it("Filtering 2", async () => {
        const rows = await db("test")
            .andWhereRaw(
                'datetimeA >= ?',
                [new Date(Date.now() - 24 * 60 * 60 * 1000)],
            )
            .andWhere(self => {
                self.where({ stringA: 'test1' })
                    .orWhere({ stringB: 'unknown' });
            })
        expect(rows.length).to.be.equal(1);
        expect(rows[0].stringA).to.be.equal("test1");
    });

    it("Test limits", async () => {
        const rows = await db("test")
            .orderBy("smallintA")
            .offset(1)
            .limit(1);
        expect(rows.length).to.be.equal(1);
        expect(rows[0].stringA).to.be.equal("test2");
    });

    it("Select one", async () => {
        expect(await db("test").first("smallintA").orderBy('smallintA')).to.be.deep.equal({
            "smallintA": 1,
        });
    });

    it("Test pluck", async () => {
        const column = await db("test").pluck("integerA");
        column.sort();
        expect(column).to.be.deep.equal([1, 2]);
    });

    it.skip('Test update', async () => {
        await db("test").update({ stringA: 'updated' }).where({ stringA: 'test1' });
        const rows = await db("test").where({ stringA: 'updated' });
        expect(rows.length).to.be.equal(1);
        expect(rows[0].stringA).to.be.equal("updated");
    });

    it("Insert Big Data", async () => {
        await db.schema.dropTableIfExists("test_storage");
        await db.schema.raw(`
            create table test_storage
            (
                date         Date,
                date_time    DATETIME64,
                server_id    LowCardinality(String),
                item_id      LowCardinality(String),
                quantity     UInt32,
                type         Enum8('take' = 0, 'put' = 1),
                reason       LowCardinality(String),
                go_id_target LowCardinality(String),
                user_id      Nullable(Int64),
                extra_data   String,
                operation_id UUID,
                alliance_id  Array(String),
                faction_id   Array(String),
                position Tuple(x Nullable(UInt16), y Nullable(UInt16)),
                remainder UInt32
            )
            engine = MergeTree order by date
        `);

        const data = [];
        const dateNow = Date.now();
        for (let i = 0; i < 5000; i++) {
            data.push({
                date: dateNow / 1000,
                date_time: dateNow,
                server_id: 'prod1',
                item_id: 'Defibrillator',
                quantity: 99,
                type: 'take',
                reason: 'StorageOperationReason',
                go_id_target: 'Player999999/Storage',
                alliance_id: null,
                faction_id: null,
                user_id: 999999,
                operation_id: 'e7ea5d80-1a8a-11ef-b7e6-bddd9c560874',
                extra_data: '{}',
                remainder: 0,
                position: db.raw(
                    'tuple(?, ?)',
                    [1, 1],
                ),
            });
        }
        let error = false;
        await db
            .insert(data)
            .into("test_storage")
            .catch((e) => {
                console.error(e);
                error = true;
            });
        expect(error).to.be.equal(false);
    });

    it("Drop tables", async () => {
        expect(await db.schema.hasTable("test")).to.be.equal(true);
        await db.schema.dropTable("test");
        await db.schema.dropTableIfExists("test_storage");
        expect(await db.schema.hasTable("test")).to.be.equal(false);
    });

    it("Migrations", async() => {
        const dbMigration = knex({
            client,
            migrations: {
                directory: 'tests/migrations',
                disableTransactions: true,
                disableMigrationsListValidation: true,
            },
            connection: () => ClickHouseURL,
        });

        await dbMigration.schema.dropTableIfExists("test_migration");
        await dbMigration.schema.dropTableIfExists("knex_migrations");
        await dbMigration.schema.dropTableIfExists("knex_migrations_lock");

        let migrateResult = await dbMigration.migrate.latest();
        expect(migrateResult[0]).to.be.equal(1);
        expect(migrateResult[1]).to.be.deep.equal(['20221024231500_test_migration']);
        expect(await dbMigration.migrate.currentVersion()).to.be.equal('20221024231500');
        expect(await dbMigration.schema.hasTable("test_migration")).to.be.equal(true);
    });
});

// describe("DDL", () => {
//     let knex;
//     const tableName = "ddl";
//     const knexConfig = generateConfig();
//
//     beforeAll(async () => {
//         knex = knexLib(knexConfig);
//         await new Promise((resolve) => setTimeout(resolve, 400));
//     });
//
//     afterAll(async () => {
//         await knex.destroy();
//         await fs.promises.unlink(knexConfig.connection.database).catch(() => {});
//     });
//
//     it("Test connection", async () => {
//         await knex.raw("SELECT 1 FROM RDB$DATABASE");
//     });
//
//     it("Create table", async () => {
//         expect(await knex.schema.hasTable("ddl")).to.be.equal(false);
//
//         await knex.schema.createTable("ddl", function (table) {
//             table.string("id").primary();
//             table.string("col_a").nullable();
//             table.integer("col_b").notNullable();
//             table.integer("col_d").nullable();
//         });
//
//         expect(await knex.schema.hasTable("ddl")).to.be.equal(true);
//     });
//
//     it("Rename table columns", async () => {
//         const oldName = "col_d";
//         const newName = "col_d_renamed";
//
//         expect(await knex.schema.hasColumn(tableName, oldName)).to.be.equal(true);
//         expect(await knex.schema.hasColumn(tableName, newName)).to.be.equal(false);
//
//         await knex.schema
//             .table(tableName, (table) => table.renameColumn(oldName, newName))
//             .then();
//
//         expect(await knex.schema.hasColumn(tableName, oldName)).to.be.equal(false);
//         expect(await knex.schema.hasColumn(tableName, newName)).to.be.equal(true);
//
//         await knex.schema
//             .table(tableName, (table) => table.renameColumn(newName, oldName))
//             .then();
//
//         expect(await knex.schema.hasColumn(tableName, oldName)).to.be.equal(true);
//         expect(await knex.schema.hasColumn(tableName, newName)).to.be.equal(false);
//     });
//
//     it("Create & Drop column", async () => {
//         expect(await knex.schema.hasColumn(tableName, "tmp")).to.be.equal(false);
//         await knex.schema
//             .alterTable(tableName, (table) => table.string("tmp").nullable())
//             .then();
//         expect(await knex.schema.hasColumn(tableName, "tmp")).to.be.equal(true);
//         await knex.schema
//             .table(tableName, (table) => table.dropColumn("tmp"))
//             .then();
//         expect(await knex.schema.hasColumn(tableName, "tmp")).to.be.equal(false);
//     });
// });
