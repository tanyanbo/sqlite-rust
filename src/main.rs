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
            let (page_size, schema_page) = parse_first_page(args[1].clone())?;
            println!("database page size: {}", page_size);
            let number_of_tables = u16::from_be_bytes(schema_page[3..5].try_into()?);
            println!("number of tables: {}", number_of_tables);
        }
        ".tables" => {
            let (_, schema_page) = parse_first_page(args[1].clone())?;
            let number_of_tables = u16::from_be_bytes(schema_page[3..5].try_into()?);

            let mut cell_locations = vec![];
            for i in 0..number_of_tables as usize {
                let location = &schema_page[8 + i * 2..8 + i * 2 + 2];
                let location = (location[0] as usize) << 8 | location[1] as usize;
                let location = location - 100;
                cell_locations.push(location);
            }
            let cell_locations = cell_locations.into_iter().rev().collect::<Vec<_>>();

            let mut table_names = vec![];
            for (index, location) in cell_locations.iter().enumerate() {
                let end_location = if index == cell_locations.len() - 1 {
                    schema_page.len()
                } else {
                    cell_locations[index + 1]
                };
                let cell = &schema_page[*location as usize..end_location as usize];

                let mut cursor = 0;

                let (_, size) = parse_varint(&cell);
                cursor += size;
                let (_, size) = parse_varint(&cell[cursor..]);
                cursor += size;
                let (mut header_size, size) = parse_varint(&cell[cursor..]);
                cursor += size;
                header_size -= size;

                let mut columns = vec![];
                while header_size > 0 {
                    let (coltype, size) = parse_varint(&cell[cursor..]);
                    columns.push(coltype);
                    cursor += size;
                    header_size -= size;
                }

                let size_of_first_two_columns =
                    columns.iter().take(2).fold(0, |acc, v| acc + (v - 13) / 2);
                let table_name = String::from_utf8_lossy(
                    &cell[cursor + size_of_first_two_columns
                        ..cursor + size_of_first_two_columns + (columns[2] - 13) / 2],
                );
                table_names.push(table_name);

                let size_of_first_three_columns =
                    columns.iter().take(3).fold(0, |acc, v| acc + (v - 13) / 2);
                let (root_page, _) = parse_varint(&cell[cursor + size_of_first_three_columns..]);
                println!("{:?}", root_page);
            }

            let table_names = table_names
                .into_iter()
                .filter(|name| !name.starts_with("sqlite_"))
                .collect::<Vec<_>>()
                .join(" ");
            println!("{:?}", table_names);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn parse_varint(cell: &[u8]) -> (usize, usize) {
    let mut value: usize = 0;
    let mut index = 0;
    loop {
        let cur_value = cell[index] as usize;
        let has_more = cur_value & 1 << 8;
        value = value << 7 | (cur_value & 0b01111111);
        index += 1;
        if has_more == 0 {
            break;
        }
    }
    (value, index)
}

fn parse_first_page(database: String) -> Result<(usize, Vec<u8>)> {
    let mut file = File::open(database)?;
    let mut header: [u8; 100] = [0; 100];
    file.read_exact(&mut header)?;
    let page_size = u16::from_be_bytes(header[16..18].try_into()?);
    let page_size: usize = if page_size == 1 {
        65536
    } else {
        page_size as usize
    };
    let mut schema_page = vec![0; page_size];
    file.read_exact(&mut schema_page)?;
    Ok((page_size, schema_page))
}
