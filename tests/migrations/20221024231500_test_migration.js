export async function up(knex) {
    await knex.raw(`
        create table test_migration
        (
            date Date,
            date_time DateTime,
            server_id LowCardinality(String),
            session_id UUID,
            user_id Int64,
            ip String
        )
        engine = MergeTree ORDER BY date_time
    `);
}

export async function down(knex) {
    await knex.raw('drop table test_migration');
}
