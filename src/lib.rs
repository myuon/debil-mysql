use debil::SQLValue;

mod types;
pub use types::*;

mod conn;
pub use conn::*;

impl SQLValue<MySQLValue> for String {
    fn column_type(_: std::marker::PhantomData<Self>, size: i32) -> String {
        if size > 0 {
            format!("varchar({})", size)
        } else {
            "text".to_string()
        }
    }

    fn serialize(self) -> MySQLValue {
        MySQLValue(From::from(self))
    }
    fn deserialize(val: MySQLValue) -> Self {
        mysql_async::from_value(val.0)
    }
}

impl SQLValue<MySQLValue> for i32 {
    fn column_type(_: std::marker::PhantomData<Self>, _: i32) -> String {
        "int".to_string()
    }

    fn serialize(self) -> MySQLValue {
        MySQLValue(From::from(self))
    }
    fn deserialize(val: MySQLValue) -> Self {
        mysql_async::from_value(val.0)
    }
}

impl SQLValue<MySQLValue> for i64 {
    fn column_type(_: std::marker::PhantomData<Self>, _: i32) -> String {
        "bigint".to_string()
    }

    fn serialize(self) -> MySQLValue {
        MySQLValue(From::from(self))
    }
    fn deserialize(val: MySQLValue) -> Self {
        mysql_async::from_value(val.0)
    }
}
