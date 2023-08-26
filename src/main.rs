use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

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
            let page_size = get_page_size(args[1].clone())?;
            println!("database page size: {}", page_size);

            let schema_page = get_schema_page(args[1].clone())?;
            let number_of_tables = u16::from_be_bytes(schema_page[3..5].try_into()?);
            println!("number of tables: {}", number_of_tables);
        }
        ".tables" => {
            let _schema_page = get_schema_page(args[1].clone())?;
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn get_page_size(database: String) -> Result<u32> {
    let header = get_database_header(database)?;
    let page_size = u16::from_be_bytes(header[16..18].try_into()?);
    Ok(if page_size == 1 {
        65536
    } else {
        page_size as u32
    })
}

fn get_database_header(database: String) -> Result<[u8; 100]> {
    let mut file = File::open(database)?;
    let mut header: [u8; 100] = [0; 100];
    file.read_exact(&mut header)?;
    return Ok(header);
}

fn get_schema_page(database: String) -> Result<Vec<u8>> {
    let page_size = get_page_size(database.clone())? as usize;
    let mut file = File::open(database)?;
    let mut header = vec![0; page_size];
    file.read_exact(&mut header)?;
    Ok(header[100..].to_vec())
}
