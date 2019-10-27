#![feature(async_closure)]

use debil::*;
use debil_mysql::*;
use mysql_async::OptsBuilder;

#[derive(Table, PartialEq, Debug, Clone)]
#[sql(table_name = "user", sql_type = "MySQLValue")]
struct User {
    #[sql(size = 50, primary_key = true)]
    user_id: String,
    #[sql(size = 50, unqiue = true)]
    name: String,
    #[sql(size = 256)]
    email: String,
    age: i32,
}

async fn setup(conn: &mut DebilConn) -> Result<(), mysql_async::error::Error> {
    // drop table
    conn.drop_table::<User>().await?;

    // create
    conn.create_table::<User>().await?;

    Ok(())
}

#[tokio::test]
async fn it_should_create_and_select() -> Result<(), mysql_async::error::Error> {
    let raw_conn = mysql_async::Conn::new(
        OptsBuilder::new()
            .ip_or_hostname("127.0.0.1")
            .user(Some("root"))
            .pass(Some("password"))
            .db_name(Some("db"))
            .prefer_socket(Some(false))
            .pool_constraints(mysql_async::PoolConstraints::new(1, 1))
            .clone(),
    )
    .await?;
    let mut conn = DebilConn::from_conn(raw_conn);
    setup(&mut conn).await?;

    let result = conn.first::<User>().await?;
    assert!(result.is_none());

    let user1 = User {
        user_id: "user-123456".to_string(),
        name: "foo".to_string(),
        email: "dddd@example.com".to_string(),
        age: 20,
    };
    let user2 = User {
        user_id: "user-456789".to_string(),
        name: "bar".to_string(),
        email: "quux@example.com".to_string(),
        age: 55,
    };
    conn.save::<User>(user1.clone()).await?;
    conn.save::<User>(user2.clone()).await?;

    let result = conn.load::<User>().await?;
    assert_eq!(result.len(), 2);
    assert_eq!(result, vec![user1, user2]);

    Ok(())
}
