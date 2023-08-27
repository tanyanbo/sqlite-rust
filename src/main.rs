mod db;

use anyhow::{anyhow, bail, Result};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

use crate::db::{parse_first_page, parse_int};

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
            let (page_size, schema_page, _) = parse_first_page(args[1].clone())?;
            println!("database page size: {}", page_size);
            let number_of_tables = u16::from_be_bytes(schema_page[3..5].try_into()?);
            println!("number of tables: {}", number_of_tables);
        }
        ".tables" => {
            let table_root_pages = parse_first_page(args[1].clone())?.2;
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
            let ast = Parser::parse_sql(&dialect, sql).unwrap();
            let ast = ast[0].clone();
            let mut column = None;
            let mut table_name = None;
            if let Statement::Query(query) = ast {
                if let SetExpr::Select(select) = *query.body {
                    if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        table_name = Some(name.0[0].value.clone());
                    }
                    if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                        match expr {
                            Expr::Identifier(ident) => {
                                column = Some(ident);
                            }
                            Expr::Function(..) => {
                                let (page_size, _, table_root_pages) =
                                    parse_first_page(args[1].clone())?;
                                let table_name = table_name.ok_or(anyhow!("Unsupported query"))?;
                                let table_root_page =
                                    table_root_pages.get(&table_name.clone()).ok_or(anyhow!(
                                        "Table {} not found in database {}",
                                        table_name,
                                        args[1]
                                    ))?;

                                let mut file = File::open(args[1].clone())?;
                                let mut table_page = vec![0; page_size];
                                file.seek(SeekFrom::Start(
                                    page_size as u64 * (*table_root_page as u64 - 1),
                                ))?;
                                file.read_exact(&mut table_page)?;
                                let count = parse_int(&table_page[3..5]);
                                println!("{:?}", count);
                            }
                            _ => bail!("Unsupported expression type"),
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
