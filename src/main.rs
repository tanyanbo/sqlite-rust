mod db;

use anyhow::{bail, Result};
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
            let (page_size, _, _) = parse_first_page(args[1].clone())?;
            let split_sql = sql.split([' ']).collect::<Vec<_>>();
            println!("{:?}", split_sql);

            let mut file = File::open(args[1].clone())?;
            let mut fourth_page = vec![0; page_size];
            file.seek(SeekFrom::Start(page_size as u64 * 3))?;
            file.read_exact(&mut fourth_page)?;
            println!("{:?}", &fourth_page[..8]);
        }
    }

    Ok(())
}
