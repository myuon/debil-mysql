use debil::*;
use debil_mysql::*;

#[derive(Table, PartialEq, Debug)]
#[sql(table_name = "ex_1", sql_type = "MySQLValue")]
struct Ex1 {
    #[sql(size = 50)]
    field1: String,
    aaaa: i32,
}
