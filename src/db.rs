use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

use crate::ast::get_columns_from_create_table_sql;
use crate::structs::{get_data_type, ColumnDataType, Table};

pub(crate) fn parse_varint(cell: &[u8]) -> (usize, usize) {
    let mut value: usize = 0;
    let mut index = 0;
    loop {
        let cur_value = cell[index] as usize;
        let has_more = cur_value & (1 << 7);
        value = value << 7 | (cur_value & 0b01111111);
        index += 1;
        if has_more == 0 {
            break;
        }
    }
    (value, index)
}

pub(crate) fn parse_first_page(
    database: &String,
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

pub(crate) fn get_table_rootpage(
    database: &String,
    table_name: &String,
) -> Result<(Vec<u8>, usize)> {
    let (page_size, _, table_info) = parse_first_page(database)?;
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
    Ok((table_page, table_root_page))
}

pub(crate) fn get_table_pages(rootpage: &Vec<u8>, rootpage_number: usize) -> Result<Vec<usize>> {
    let mut result = vec![];
    if rootpage[0] == 0x0d {
        result.push(rootpage_number);
        return Ok(result);
    }

    if rootpage[0] == 0x05 {
        let cell_addrs = get_cell_addrs(rootpage, 12)?;
        for addr in cell_addrs {
            let page_number = parse_int(&rootpage[addr..addr + 4]);
            result.push(page_number);
        }
        let rightmost_page_number = parse_int(&rootpage[8..12]);
        result.push(rightmost_page_number);
        return Ok(result);
    }

    unimplemented!("indexes are not yet supported");
}

pub(crate) fn get_table_page(database: &String, page_number: usize) -> Result<Vec<u8>> {
    let (page_size, _, _) = parse_first_page(database)?;
    let mut file = File::open(database)?;
    let mut table_page = vec![0; page_size];
    file.seek(SeekFrom::Start(page_size as u64 * (page_number as u64 - 1)))?;
    file.read_exact(&mut table_page)?;
    Ok(table_page)
}

pub(crate) fn get_columns(database: &String, table_name: &String) -> Result<Vec<String>> {
    let (_, schema_page, _) = parse_first_page(database)?;
    let sql = get_table_info(&schema_page)?
        .get(table_name)
        .ok_or(anyhow!(
            "Column {} not found in database {}",
            table_name,
            database
        ))?
        .sql
        .clone();
    Ok(get_columns_from_create_table_sql(&sql)?)
}

pub(crate) fn get_table_columns_data(
    table_page: &Vec<u8>,
    column_indexes: &Vec<usize>,
) -> Result<Vec<Vec<String>>> {
    let mut result: Vec<Vec<String>> = vec![];

    let cell_addrs = get_cell_addrs(table_page, 8)?;
    for (index, location) in cell_addrs.iter().enumerate() {
        result.push(vec![]);
        let end_location = if index == cell_addrs.len() - 1 {
            table_page.len()
        } else {
            cell_addrs[index + 1]
        };
        let cell = &table_page[*location as usize..end_location as usize];

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

        for (index, column) in columns.iter().enumerate() {
            let data_type = get_data_type(*column);
            let data_size = data_type.get_content_size();
            if !column_indexes.contains(&index) {
                cursor += data_size;
                continue;
            }

            let data = match data_type {
                ColumnDataType::EightBit
                | ColumnDataType::SixteenBit
                | ColumnDataType::TwentyFourBit
                | ColumnDataType::ThirtyTwoBit
                | ColumnDataType::FortyEightBit
                | ColumnDataType::SixtyFourBit => {
                    parse_int(&cell[cursor..cursor + data_size]).to_string()
                }
                ColumnDataType::Text(..) => {
                    String::from_utf8_lossy(&cell[cursor..cursor + data_size]).to_string()
                }
                ColumnDataType::Null
                | ColumnDataType::Float
                | ColumnDataType::IntegerOne
                | ColumnDataType::IntegerZero
                | ColumnDataType::Blob(..) => unimplemented!("Data type not implemented"),
            };
            let len = result.len() - 1;
            result[len].push(data);
            cursor += data_size;
        }
    }

    Ok(result)
}

fn get_table_info(schema_page: &Vec<u8>) -> Result<HashMap<String, Table>> {
    let cell_addrs = get_cell_addrs(schema_page, 8)?
        .iter()
        .map(|addr| addr - 100)
        .collect::<Vec<_>>();

    let mut table_info = HashMap::default();
    for (index, location) in cell_addrs.iter().enumerate() {
        let mut table = Table::default();
        let end_location = if index == cell_addrs.len() - 1 {
            schema_page.len()
        } else {
            cell_addrs[index + 1]
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
        for _ in 0..rootpage_size {
            rootpage.push(cell[cursor]);
            cursor += 1;
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

fn get_cell_addrs(table_page: &Vec<u8>, header_size: usize) -> Result<Vec<usize>> {
    let number_of_cells = u16::from_be_bytes(table_page[3..5].try_into()?);
    let mut cell_locations = vec![];
    for i in 0..number_of_cells as usize {
        let location = &table_page[header_size + i * 2..header_size + i * 2 + 2];
        let location = (location[0] as usize) << 8 | location[1] as usize;
        let location = location;
        cell_locations.push(location);
    }
    if cell_locations.len() > 1 && cell_locations[0] > cell_locations[1] {
        Ok(cell_locations.into_iter().rev().collect::<Vec<_>>())
    } else {
        Ok(cell_locations)
    }
}
