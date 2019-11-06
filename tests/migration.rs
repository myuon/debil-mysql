use debil::*;
use debil_mysql::*;
use mysql_async::OptsBuilder;

#[derive(Table)]
#[sql(table_name = "migration_test", sql_type = "debil_mysql::MySQLValue")]
struct Before {
    n: i32,
    #[sql(size = 10)]
    pk: String,
    still_remaining: i32,
}

#[derive(Table)]
#[sql(table_name = "migration_test", sql_type = "debil_mysql::MySQLValue")]
struct After {
    n: i64,
    #[sql(size = 100)]
    extra: String,
    #[sql(size = 11)]
    pk: String,
}

#[tokio::test]
async fn it_should_migrate() -> Result<(), debil_mysql::Error> {
    let raw_conn = mysql_async::Conn::new(
        OptsBuilder::new()
            .ip_or_hostname("127.0.0.1")
            .user(Some("root"))
            .pass(Some("password"))
            .db_name(Some("db"))
            .prefer_socket(Some(false))
            .pool_options(Some(mysql_async::PoolOptions::with_constraints(
                mysql_async::PoolConstraints::new(1, 1).unwrap(),
            )))
            .clone(),
    )
    .await?;
    let mut conn = DebilConn::from_conn(raw_conn);

    // setup
    conn.drop_table::<After>().await?;

    // migration creates table
    conn.migrate::<Before>().await?;

    // migrate
    conn.migrate::<After>().await?;

    Ok(())
}
