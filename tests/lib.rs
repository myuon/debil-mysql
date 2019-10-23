use debil::*;

#[derive(Table, PartialEq, Debug)]
#[sql(table_name = "ex_1", sql_type = "Vec<u8>")]
struct Ex1 {
    #[sql(size = 50)]
    field1: String,
    aaaa: i32,
}
