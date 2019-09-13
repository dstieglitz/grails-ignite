environments {
    production {
        dataSource {
            properties {
                // See http://grails.org/doc/latest/guide/conf.html#dataSource for documentation
                defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_COMMITTED
            }
        }
    }
}
