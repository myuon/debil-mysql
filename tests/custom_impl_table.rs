#![feature(async_closure)]

use debil::*;
use debil_mysql::*;
use mysql_async::OptsBuilder;

struct R {
    s: String,
    n: i32,
}

// custom implementation
impl SQLTable for R {
    type ValueType = MySQLValue;

    fn table_name(_: std::marker::PhantomData<Self>) -> String {
        "r_table".to_string()
    }

    fn schema_of(_: std::marker::PhantomData<Self>) -> Vec<(String, String, FieldAttribute)> {
        vec![
            (
                "s".to_string(),
                "varchar(50)".to_string(),
                Default::default(),
            ),
            ("n".to_string(), "int".to_string(), Default::default()),
        ]
    }

    fn map_to_sql(self) -> Vec<(String, Self::ValueType)> {
        let mut result = Vec::new();
        result.push(("s".to_string(), String::serialize(self.s)));
        result.push(("n".to_string(), i32::serialize(self.n)));

        result
    }

    fn map_from_sql(values: std::collections::HashMap<String, Self::ValueType>) -> Self {
        R {
            s: String::deserialize(values["s"].clone()),
            n: i32::deserialize(values["n"].clone()),
        }
    }
}

async fn it_should_create_and_select() -> Result<(), mysql_async::error::Error> {
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

    // check thread safety
    std::thread::spawn(async move || {
        conn.load::<R>().await;
        conn.first::<R>().await;
    });

    Ok(())
}
