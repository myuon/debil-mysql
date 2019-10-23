use debil::SQLValue;

pub struct MySQLValue(mysql_async::Value);

impl SQLValue<MySQLValue> for String {
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

impl SQLValue<MySQLValue> for i32 {
    fn column_type(_: std::marker::PhantomData<Self>, size: i32) -> String {
        format!("varchar({})", size)
    }

    fn serialize(self) -> MySQLValue {
        MySQLValue(From::from(self))
    }
    fn deserialize(val: MySQLValue) -> Self {
        mysql_async::from_value(val.0)
    }
}
