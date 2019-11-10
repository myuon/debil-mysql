use crate::error::Error;
use crate::types::MySQLValue;
use mysql_async::prelude::*;

pub struct DebilConn {
    conn: Option<mysql_async::Conn>,
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

    /// Execute given SQL and maps the results to some SQLTable structure
    pub async fn sql_query<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
        query: String,
        parameters: params::Params,
    ) -> Result<Vec<T>, Error> {
        let conn = self.conn.take().unwrap();

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
        self.conn.replace(conn);

        Ok(vs)
    }

    /// Execute given SQL and return the number of affected rows
    pub async fn sql_exec(
        &mut self,
        query: String,
        parameters: params::Params,
    ) -> Result<u64, Error> {
        let conn = self.conn.take().unwrap();
        let result = conn.prep_exec(query, parameters).await?;

        let rows = result.affected_rows();
        let conn = result.drop_result().await?;
        self.conn.replace(conn);

        Ok(rows)
    }

    /// Execute given all SQLs
    pub async fn sql_batch_exec(
        &mut self,
        query: String,
        parameters: Vec<params::Params>,
    ) -> Result<(), Error> {
        let conn = self.conn.take().unwrap();
        let conn = conn.batch_exec(query, parameters).await?;
        self.conn.replace(conn);

        Ok(())
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

    pub async fn save_all<T: debil::SQLTable<ValueType = MySQLValue> + Clone>(
        &mut self,
        datas: Vec<T>,
    ) -> Result<(), Error> {
        if datas.len() == 0 {
            return Ok(());
        }

        let (query, _) = datas[0].clone().save_query_with_params();
        let mut parameters = Vec::<params::Params>::new();
        for data in datas {
            let (_, ps) = data.save_query_with_params();
            let param: params::Params =
                From::from(ps.into_iter().map(|(x, y)| (x, y.0)).collect::<Vec<_>>());

            parameters.push(param);
        }

        self.sql_batch_exec(query, parameters).await?;

        Ok(())
    }

    pub async fn load_with<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
        builder: debil::QueryBuilder,
    ) -> Result<Vec<T>, Error> {
        self.load_with2::<T, T>(builder).await
    }

    pub async fn load_with2<
        T: debil::SQLTable<ValueType = MySQLValue>,
        U: debil::SQLTable<ValueType = MySQLValue>,
    >(
        &mut self,
        builder: debil::QueryBuilder,
    ) -> Result<Vec<U>, Error> {
        let schema = debil::SQLTable::schema_of(std::marker::PhantomData::<T>);
        let table_name = debil::SQLTable::table_name(std::marker::PhantomData::<T>);
        let query = builder
            .table(table_name.clone())
            .append_selects(
                schema
                    .iter()
                    .map(|(k, _, _)| format!("{}.{}", table_name, k))
                    .collect::<Vec<_>>(),
            )
            .build();
        self.sql_query::<U>(query, params::Params::Empty).await
    }

    pub async fn first_with<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
        builder: debil::QueryBuilder,
    ) -> Result<T, Error> {
        let schema = debil::SQLTable::schema_of(std::marker::PhantomData::<T>);
        let table_name = debil::SQLTable::table_name(std::marker::PhantomData::<T>);
        let query = builder
            .table(table_name.clone())
            .append_selects(
                schema
                    .iter()
                    .map(|(k, _, _)| format!("{}.{}", table_name, k))
                    .collect::<Vec<_>>(),
            )
            .limit(1)
            .build();

        self.sql_query::<T>(query, params::Params::Empty)
            .await
            .and_then(|mut vs| vs.pop().ok_or(Error::NotFoundError))
    }

    pub async fn load<T: debil::SQLTable<ValueType = MySQLValue>>(
        &mut self,
    ) -> Result<Vec<T>, Error> {
        self.load_with(debil::QueryBuilder::new()).await
    }

    pub async fn first<T: debil::SQLTable<ValueType = MySQLValue>>(&mut self) -> Result<T, Error> {
        self.first_with(debil::QueryBuilder::new()).await
    }
}
