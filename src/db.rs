use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

use crate::structs::{get_data_type, Table};

pub(crate) fn parse_varint(cell: &[u8]) -> (usize, usize) {
    let mut value: usize = 0;
    let mut index = 0;
    loop {
        let cur_value = cell[index] as usize;
        let has_more = cur_value & (1 << 8);
        value = value << 7 | (cur_value & 0b01111111);
        index += 1;
        if has_more == 0 {
            break;
        }
    }
    (value, index)
}

pub(crate) fn parse_first_page(
    database: String,
) -> Result<(usize, Vec<u8>, HashMap<String, Table>)> {
    let mut file = File::open(database.clone())?;
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

    let table_root_pages = get_table_info(&schema_page)?;
    Ok((page_size, schema_page, table_root_pages))
}

pub(crate) fn parse_int(bytes: &[u8]) -> usize {
    let mut value: usize = 0;
    for byte in bytes {
        value = value << 8 | *byte as usize;
    }
    value
}

pub(crate) fn get_table_page(database: String, table_name: String) -> Result<Vec<u8>> {
    let (page_size, _, table_info) = parse_first_page(database.clone())?;
    let table_root_page = table_info
        .get(&table_name.clone())
        .ok_or(anyhow!(
            "Table {} not found in database {}",
            table_name,
            database.clone()
        ))?
        .rootpage;
    let mut file = File::open(database)?;
    let mut table_page = vec![0; page_size];
    file.seek(SeekFrom::Start(
        page_size as u64 * (table_root_page as u64 - 1),
    ))?;
    file.read_exact(&mut table_page)?;
    Ok(table_page)
}

pub(crate) fn get_columns(table_page: Vec<u8>, columns: Vec<String>) -> Result<Vec<String>> {
    let _cell_addrs = get_cell_addrs(table_page, 8)?;
    let create_sql = "".to_string();
    Ok(vec![])
}

fn get_table_columns(table_page: Vec<u8>) -> Result<Vec<String>> {
    Ok(vec![])
}

fn get_table_info(schema_page: &Vec<u8>) -> Result<HashMap<String, Table>> {
    let cell_addrs = get_cell_addrs(schema_page.to_vec(), 8)?;

    let mut table_info = HashMap::default();
    for (index, location) in cell_addrs.iter().enumerate() {
        let mut table = Table::default();
        let end_location = if index == cell_addrs.len() - 1 {
            schema_page.len()
        } else {
            cell_addrs[index + 1]
        };
        let cell = &schema_page[*location as usize..end_location as usize];
        println!("{:?}", &cell[..10]);

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

        let type_size = get_data_type(columns[0]).get_content_size();
        let r#type = String::from_utf8_lossy(&cell[cursor..cursor + type_size]);
        cursor += type_size;
        let name_size = get_data_type(columns[1]).get_content_size();
        let name = String::from_utf8_lossy(&cell[cursor..cursor + name_size]);
        cursor += name_size;
        let tbl_name_size = get_data_type(columns[2]).get_content_size();
        let tbl_name = String::from_utf8_lossy(&cell[cursor..cursor + tbl_name_size]);
        cursor += tbl_name_size;

        let rootpage_size = get_data_type(columns[3]).get_content_size();
        let mut rootpage = vec![];
        for i in 0..rootpage_size {
            rootpage.push(cell[cursor + i]);
        }
        let rootpage = parse_int(&rootpage);

        let sql_size = get_data_type(columns[4]).get_content_size();
        let sql = String::from_utf8_lossy(&cell[cursor..cursor + sql_size]);

        table.r#type = r#type.to_string();
        table.name = name.to_string();
        table.tbl_name = tbl_name.to_string();
        table.rootpage = rootpage;
        table.sql = sql.to_string();

        table_info.insert(tbl_name.to_string(), table);
    }

    Ok(table_info)
}

fn get_cell_addrs(table_page: Vec<u8>, header_size: usize) -> Result<Vec<usize>> {
    let number_of_cells = u16::from_be_bytes(table_page[3..5].try_into()?);
    let mut cell_locations = vec![];
    for i in 0..number_of_cells as usize {
        let location = &table_page[header_size + i * 2..header_size + i * 2 + 2];
        let location = (location[0] as usize) << 8 | location[1] as usize;
        let location = location - 100;
        cell_locations.push(location);
    }
    Ok(cell_locations.into_iter().rev().collect::<Vec<_>>())
}
