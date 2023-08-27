mod db;

use anyhow::{anyhow, bail, Result};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

use crate::db::parse_first_page;

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
            let (page_size, _, table_root_pages) = parse_first_page(args[1].clone())?;
            let split_sql = sql.split([' ']).collect::<Vec<_>>();
            let table_name = split_sql
                .last()
                .ok_or(anyhow!("Missing table name"))?
                .replace(";", "");
            let table_root_page = table_root_pages.get(&table_name).ok_or(anyhow!(
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
            println!("{:?}", &table_page[..8]);
        }
    }

    Ok(())
}
