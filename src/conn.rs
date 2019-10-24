use crate::types::MySQLValue;
use mysql_async::error::Error;
use mysql_async::prelude::*;

pub struct DebilConn(mysql_async::Conn);

impl DebilConn {
    pub fn as_conn(self) -> mysql_async::Conn {
        self.0
    }

    pub fn from_conn(conn: mysql_async::Conn) -> Self {
        DebilConn(conn)
    }

    pub async fn create_table<T: debil::SQLTable<ValueType = MySQLValue>>(
        self,
    ) -> Result<Self, Error> {
        Ok(DebilConn::from_conn(
            self.as_conn()
                .drop_exec(
                    debil::SQLTable::create_table_query(std::marker::PhantomData::<T>),
                    params::Params::Empty,
                )
                .await?,
        ))
    }

    pub async fn save<T: debil::SQLTable<ValueType = MySQLValue>>(
        self,
        data: T,
    ) -> Result<Self, Error> {
        let (query, ps) = data.save_query_with_params();
        let param: params::Params =
            From::from(ps.into_iter().map(|(x, y)| (x, y.0)).collect::<Vec<_>>());

        Ok(DebilConn::from_conn(
            self.as_conn().drop_exec(query, param).await?,
        ))
    }

    pub async fn load<T: debil::SQLTable<ValueType = MySQLValue>>(
        self,
    ) -> Result<(Self, Vec<T>), Error> {
        let schema = debil::SQLTable::schema_of(std::marker::PhantomData::<T>);

        let result = self
            .as_conn()
            .prep_exec(
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
            .await?;

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

        Ok((DebilConn::from_conn(conn), vs))
    }

    pub async fn first<T: debil::SQLTable<ValueType = MySQLValue>>(
        self,
    ) -> Result<(Self, Option<T>), Error> {
        let schema = debil::SQLTable::schema_of(std::marker::PhantomData::<T>);

        let result = self
            .as_conn()
            .prep_exec(
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
            .await?;

        let (conn, mut vs) = result
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

        Ok((DebilConn::from_conn(conn), vs.pop()))
    }
}
