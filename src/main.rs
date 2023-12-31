mod ast;
mod db;
mod structs;

use anyhow::{anyhow, bail, Result};
use db::{get_columns, get_table_columns_data, get_table_pages};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::db::{get_table_page, get_table_rootpage, parse_first_page, parse_int};

enum QueryType {
    Count,
    Select,
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let (page_size, schema_page, _) = parse_first_page(&args[1])?;
            println!("database page size: {}", page_size);
            let number_of_tables = u16::from_be_bytes(schema_page[3..5].try_into()?);
            println!("number of tables: {}", number_of_tables);
        }
        ".tables" => {
            let table_root_pages = parse_first_page(&args[1])?.2;
            let table_names = table_root_pages
                .into_iter()
                .map(|(name, _)| name)
                .filter(|name| !name.starts_with("sqlite_"))
                .collect::<Vec<_>>()
                .join(" ");
            println!("{}", table_names);
        }
        sql => {
            let dialect = GenericDialect {};
            let ast = Parser::parse_sql(&dialect, sql)?;
            let ast = ast[0].clone();
            let mut table_name = None;
            if let Statement::Query(query) = ast {
                if let SetExpr::Select(select) = *query.body {
                    if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        table_name = Some(name.0[0].value.clone());
                    }

                    let table_name = table_name.ok_or(anyhow!("Invalid table name"))?;
                    let columns = get_columns(&args[1], &table_name)?;
                    let mut column_indexes: Vec<usize> = vec![];

                    let (table_rootpage, rootpage_number) =
                        get_table_rootpage(&args[1], &table_name)?;
                    let table_pages = get_table_pages(&table_rootpage, rootpage_number)?;
                    let mut query_type = QueryType::Select;

                    for item in &select.projection {
                        if let SelectItem::UnnamedExpr(expr) = &item {
                            match expr {
                                Expr::Identifier(ident) => {
                                    let column_idx = columns
                                        .iter()
                                        .position(|c| *c == ident.value)
                                        .ok_or(anyhow!(
                                            "Column {} not found in table {}",
                                            ident.value,
                                            table_name
                                        ))?;
                                    column_indexes.push(column_idx);
                                }
                                Expr::Function(..) => {
                                    query_type = QueryType::Count;
                                    // let count = parse_int(&page[3..5]);
                                    // println!("count: {}", count);
                                    // return Ok(());
                                }
                                _ => bail!("Unsupported expression type"),
                            }
                        }
                    }

                    let mut count = 0;
                    for page_number in table_pages {
                        let page = get_table_page(&args[1], page_number)?;
                        if let QueryType::Count = query_type {
                            count += parse_int(&page[3..5]);
                            continue;
                        }
                        let data = get_table_columns_data(&page, &column_indexes)?
                            .iter()
                            .map(|row| row.join("|"))
                            .collect::<Vec<_>>();
                        println!("{}", data.join("\n"));
                    }

                    if let QueryType::Count = query_type {
                        println!("{:?}", count);
                    }
                }
            }
        }
    }

    Ok(())
}
