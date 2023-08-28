use anyhow::Result;
use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

pub(crate) fn get_columns_from_create_table_sql(sql: &String) -> Result<Vec<String>> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)?;
    let mut columns = vec![];
    if let Statement::CreateTable {
        columns: ast_columns,
        ..
    } = &ast[0]
    {
        for column in ast_columns {
            columns.push(column.name.to_string());
        }
    }
    Ok(columns)
}
