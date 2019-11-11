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

#[derive(Table, Clone)]
#[sql(table_name = "user_item_relation", sql_type = "MySQLValue")]
struct UserItem {
    #[sql(size = 50)]
    user_id: String,
    #[sql(size = 50)]
    item_id: String,
}

#[derive(Debug, PartialEq)]
struct JoinedUserItemsView {
    user: User,
    item_id: Option<String>,
}

impl SQLMapper for JoinedUserItemsView {
    type ValueType = MySQLValue;

    fn map_from_sql(h: std::collections::HashMap<String, Self::ValueType>) -> JoinedUserItemsView {
        let item_id = h["item_id"].clone();
        let user = map_from_sql::<User>(h);

        JoinedUserItemsView {
            user,
            item_id: <Self::ValueType>::deserialize(item_id),
        }
    }
}

async fn setup(conn: &mut DebilConn) -> Result<(), Error> {
    // drop table
    conn.drop_table::<User>().await?;
    conn.drop_table::<UserItem>().await?;

    // create
    conn.create_table::<User>().await?;
    conn.create_table::<UserItem>().await?;

    Ok(())
}

// for sequential testing, we use only one function to test
#[tokio::test]
async fn it_should_create_and_select() -> Result<(), Error> {
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
    setup(&mut conn).await?;

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

    conn.save_all::<User>(vec![
        User {
            user_id: "_a".to_string(),
            name: "".to_string(),
            email: "".to_string(),
            age: 0,
        },
        User {
            user_id: "_b".to_string(),
            name: "".to_string(),
            email: "".to_string(),
            age: 0,
        },
    ])
    .await?;

    let result = conn.load::<User>().await?;
    assert_eq!(result.len(), 4);
    assert_eq!(result[0..2].to_vec(), vec![user1, user2]);

    // join query
    let user_id = "user-join-and-load".to_string();
    let user = User {
        user_id: user_id.clone(),
        name: "foo".to_string(),
        email: "dddd@example.com".to_string(),
        age: 20,
    };
    conn.save(user.clone()).await?;
    conn.save_all(vec![
        UserItem {
            user_id: user_id.clone(),
            item_id: "item-abcd".to_string(),
        },
        UserItem {
            user_id: user_id.clone(),
            item_id: "item-defg".to_string(),
        },
        UserItem {
            user_id: user_id.clone(),
            item_id: "item-pqrs".to_string(),
        },
    ])
    .await?;

    let j = conn
        .load_with2::<User, JoinedUserItemsView>(
            QueryBuilder::new()
                .left_join(table_name::<UserItem>(), ("user_id", "user_id"))
                .filter(format!("{}.user_id = '{}'", table_name::<User>(), user_id))
                .append_selects(vec![format!("{}.item_id", table_name::<UserItem>())]),
        )
        .await?;

    assert_eq!(
        j,
        vec![
            JoinedUserItemsView {
                user: user.clone(),
                item_id: Some("item-abcd".to_string()),
            },
            JoinedUserItemsView {
                user: user.clone(),
                item_id: Some("item-defg".to_string()),
            },
            JoinedUserItemsView {
                user: user.clone(),
                item_id: Some("item-pqrs".to_string()),
            }
        ]
    );

    // check thread safety
    async fn conn_load(mut conn: debil_mysql::DebilConn) {
        conn.load::<User>().await;
    }
    std::thread::spawn(|| conn_load(conn));

    Ok(())
}
