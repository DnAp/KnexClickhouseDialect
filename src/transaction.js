/**
 * @implements {Knex.Transaction}
 */
export default class TransactionClickHouse {
  executionPromise = Promise.resolve(undefined);

  commit(value) {
    return undefined;
  }

  isCompleted() {
    return true;
  }

  query(conn, sql, status, value) {
    return undefined;
  }

  rollback(error) {
    return undefined;
  }

  savepoint(transactionScope) {
    return undefined;
  }
};
