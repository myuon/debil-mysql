use crate::error::Error;
use crate::types::MySQLValue;
use async_trait::async_trait;
use debil::SQLConn;
use mysql_async::prelude::*;

pub struct DebilConn {
    conn: Option<mysql_async::Conn>,
}

impl debil::HasNotFound for Error {
    fn not_found() -> Self {
        Error::NotFoundError
    }
}

#[async_trait]
impl debil::SQLConn<MySQLValue> for DebilConn {
    type Error = Error;

    async fn sql_exec(
        &mut self,
        query: String,
        params: debil::Params<MySQLValue>,
    ) -> Result<u64, Error> {
        let conn = self.conn.take().unwrap();
        let result = conn
            .prep_exec(
                query,
                params
                    .0
                    .into_iter()
                    .map(|(k, v)| (k, v.0))
                    .collect::<Vec<_>>(),
            )
            .await?;

        let rows = result.affected_rows();
        let conn = result.drop_result().await?;
        self.conn.replace(conn);

        Ok(rows)
    }

    async fn sql_query<T: debil::SQLMapper<ValueType = MySQLValue> + Sync + Send>(
        &mut self,
        query: String,
        params: debil::Params<MySQLValue>,
    ) -> Result<Vec<T>, Self::Error> {
        let conn = self.conn.take().unwrap();

        let result = conn
            .prep_exec(
                query,
                params
                    .0
                    .into_iter()
                    .map(|(k, v)| (k, v.0))
                    .collect::<Vec<_>>(),
            )
            .await?;
        let (conn, vs) = result
            .map_and_drop(|row| {
                let column_names = row
                    .columns_ref()
                    .iter()
                    .map(|c| c.name_str().into_owned())
                    .collect::<Vec<_>>();
                let values = row.unwrap().into_iter().map(MySQLValue).collect::<Vec<_>>();

                debil::map_from_sql::<T>(
                    column_names
                        .into_iter()
                        .zip(values)
                        .collect::<std::collections::HashMap<_, _>>(),
                )
            })
            .await?;
        self.conn.replace(conn);

        Ok(vs)
    }

    async fn sql_batch_exec(
        &mut self,
        query: String,
        params: debil::Params<MySQLValue>,
    ) -> Result<(), Self::Error> {
        let conn = self.conn.take().unwrap();
        let conn = conn
            .batch_exec(
                query,
                params
                    .0
                    .into_iter()
                    .map(|(x, y)| (x, y.0))
                    .collect::<Vec<_>>(),
            )
            .await?;
        self.conn.replace(conn);

        Ok(())
    }
}

impl DebilConn {
    pub fn as_conn(self) -> mysql_async::Conn {
        self.conn.unwrap()
    }

    pub fn from_conn(conn: mysql_async::Conn) -> Self {
        DebilConn { conn: Some(conn) }
    }

    pub async fn sql_query_with_map<U>(
        &mut self,
        query: impl AsRef<str>,
        parameters: impl Into<params::Params>,
        mapper: impl FnMut(mysql_async::Row) -> U,
    ) -> Result<Vec<U>, Error> {
        let conn = self.conn.take().unwrap();

        let result = conn.prep_exec(query, parameters).await?;
        let (conn, vs) = result.map_and_drop(mapper).await?;
        self.conn.replace(conn);

        Ok(vs)
    }

    pub async fn drop_table<T: debil::SQLTable<ValueType = MySQLValue> + Sync + Send>(
        &mut self,
    ) -> Result<(), Error> {
        self.sql_exec(
            format!(
                "DROP TABLE IF EXISTS {}",
                debil::SQLTable::table_name(std::marker::PhantomData::<T>),
            ),
            debil::Params::<MySQLValue>::new(),
        )
        .await?;

        Ok(())
    }

    pub async fn migrate<T: debil::SQLTable<ValueType = MySQLValue> + Sync + Send>(
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
                    debil::Params::<MySQLValue>::new(),
                )
                .await?;
            } else if (vs[0].0 != column_type && vs[0].1 != column_type)
                || (attr.not_null != Some(vs[0].2 == "NO"))
                || (attr.unique != Some(vs[0].3 == "UNI"))
            {
                // check not only DATA_TYPE but also COLUMN_TYPE (for varchar)
                self.sql_exec(
                    format!(
                        "ALTER TABLE {} MODIFY COLUMN {}",
                        table_name,
                        debil::create_column_query(column_name, column_type, attr)
                    ),
                    debil::Params::<MySQLValue>::new(),
                )
                .await?;
            }
        }

        Ok(())
    }

    pub async fn save_all<T: debil::SQLTable<ValueType = MySQLValue> + Clone>(
        &mut self,
        datas: Vec<T>,
    ) -> Result<(), Error> {
        if datas.len() == 0 {
            return Ok(());
        }

        let (query, _) = datas[0].clone().save_query_with_params();
        let mut parameters = Vec::new();
        for data in datas {
            let (_, mut ps) = data.save_query_with_params();
            parameters.append(&mut ps);
        }

        self.sql_batch_exec(query, debil::Params(parameters))
            .await?;

        Ok(())
    }
}
