use crate::types::MySQLValue;
use mysql_async::error::Error;
use mysql_async::prelude::*;

pub struct DebilConn(Option<mysql_async::Conn>);

impl DebilConn {
    pub fn as_conn(self) -> mysql_async::Conn {
        self.0.unwrap()
    }

    pub fn from_conn(conn: mysql_async::Conn) -> Self {
        DebilConn(Some(conn))
    }

    pub async fn sql_query_with_map<U>(
        &mut self,
        query: impl AsRef<str>,
        parameters: impl Into<params::Params>,
        mapper: impl FnMut(mysql_async::Row) -> U,
    ) -> Result<Vec<U>, Error> {
        let conn = self.0.take().unwrap();

        let result = conn.prep_exec(query, parameters).await?;
        let (conn, vs) = result.map_and_drop(mapper).await?;
        self.0.replace(conn);

        Ok(vs)
    }

    /// Execute given SQL and maps the results to some SQLTable structure
    pub async fn sql_query<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
        query: String,
        parameters: params::Params,
    ) -> Result<Vec<T>, Error> {
        let conn = self.0.take().unwrap();

        let result = conn.prep_exec(query, parameters).await?;
        let (conn, vs) = result
            .map_and_drop(|row| {
                let column_names = row
                    .columns_ref()
                    .iter()
                    .map(|c| c.name_str().into_owned())
                    .collect::<Vec<_>>();
                let values = row.unwrap().into_iter().map(MySQLValue).collect::<Vec<_>>();

                debil::SQLTable::map_from_sql(
                    column_names
                        .into_iter()
                        .zip(values)
                        .collect::<std::collections::HashMap<_, _>>(),
                )
            })
            .await?;
        self.0.replace(conn);

        Ok(vs)
    }

    /// Execute given SQL and return the number of affected rows
    pub async fn sql_exec(
        &mut self,
        query: String,
        parameters: params::Params,
    ) -> Result<u64, Error> {
        let conn = self.0.take().unwrap();
        let result = conn.prep_exec(query, parameters).await?;

        let rows = result.affected_rows();
        let conn = result.drop_result().await?;
        self.0.replace(conn);

        Ok(rows)
    }

    pub async fn create_table<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
    ) -> Result<(), Error> {
        self.sql_exec(
            debil::SQLTable::create_table_query(std::marker::PhantomData::<T>),
            params::Params::Empty,
        )
        .await?;

        Ok(())
    }

    pub async fn drop_table<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
    ) -> Result<(), Error> {
        self.sql_exec(
            format!(
                "DROP TABLE IF EXISTS {}",
                debil::SQLTable::table_name(std::marker::PhantomData::<T>),
            ),
            params::Params::Empty,
        )
        .await?;

        Ok(())
    }

    pub async fn migrate<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
    ) -> Result<(), Error> {
        self.create_table::<T>().await?;

        let table_name = debil::SQLTable::table_name(std::marker::PhantomData::<T>);
        let schema = debil::SQLTable::schema_of(std::marker::PhantomData::<T>);

        for (column_name, column_type, attr) in schema {
            let vs = self.sql_query_with_map("SELECT DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :table_name AND COLUMN_NAME = :column_name", mysql_async::params!{
                "table_name" => table_name.clone(),
                "column_name" => column_name.clone(),
            }, mysql_async::from_row::<(String, String, String, String)>).await?;

            if vs.is_empty() {
                self.sql_exec(
                    format!(
                        "ALTER TABLE {} ADD COLUMN {}",
                        table_name,
                        debil::create_column_query(column_name, column_type, attr)
                    ),
                    params::Params::Empty,
                )
                .await?;
            } else if (vs[0].0 != column_type && vs[0].1 != column_type)
                || (attr.not_null != Some(vs[0].2 == "NO"))
                || (attr.primary_key != Some(vs[0].3 == "PRI"))
                || (attr.unique != Some(vs[0].3 == "UNI"))
            {
                // check not only DATA_TYPE but also COLUMN_TYPE (for varchar)
                self.sql_exec(
                    format!(
                        "ALTER TABLE {} MODIFY COLUMN {}",
                        table_name,
                        debil::create_column_query(
                            column_name,
                            column_type,
                            // skip primary key to avoid "Multiple primary key defined" error
                            debil::FieldAttribute {
                                primary_key: None,
                                ..attr
                            }
                        )
                    ),
                    params::Params::Empty,
                )
                .await?;
            }
        }

        Ok(())
    }

    pub async fn save<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
        data: T,
    ) -> Result<(), Error> {
        let (query, ps) = data.save_query_with_params();
        let param: params::Params =
            From::from(ps.into_iter().map(|(x, y)| (x, y.0)).collect::<Vec<_>>());

        self.sql_exec(query, param).await?;

        Ok(())
    }

    pub async fn load<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
    ) -> Result<Vec<T>, Error> {
        let schema = debil::SQLTable::schema_of(std::marker::PhantomData::<T>);

        self.sql_query::<T>(
            format!(
                "SELECT {} FROM {}",
                schema
                    .iter()
                    .map(|(k, _, _)| k.as_str())
                    .collect::<Vec<_>>()
                    .as_slice()
                    .join(", "),
                debil::SQLTable::table_name(std::marker::PhantomData::<T>),
            ),
            params::Params::Empty,
        )
        .await
    }

    pub async fn first<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
    ) -> Result<Option<T>, Error> {
        let schema = debil::SQLTable::schema_of(std::marker::PhantomData::<T>);

        self.sql_query::<T>(
            format!(
                "SELECT {} FROM {} LIMIT 1",
                schema
                    .iter()
                    .map(|(k, _, _)| k.as_str())
                    .collect::<Vec<_>>()
                    .as_slice()
                    .join(", "),
                debil::SQLTable::table_name(std::marker::PhantomData::<T>),
            ),
            params::Params::Empty,
        )
        .await
        .map(|mut vs| vs.pop())
    }
}
