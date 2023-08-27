#[derive(Default, Debug)]
pub(crate) struct Table {
    pub(crate) r#type: String,
    pub(crate) name: String,
    pub(crate) tbl_name: String,
    pub(crate) rootpage: usize,
    pub(crate) sql: String,
}

pub(crate) enum ColumnDataType {
    Null,
    EightBit,
    SixteenBit,
    TwentyFourBit,
    ThirtyTwoBit,
    FortyEightBit,
    SixtyFourBit,
    Float,
    IntegerZero,
    IntegerOne,
    Text(usize),
    Blob(usize),
}

impl ColumnDataType {
    pub(crate) fn get_content_size(&self) -> usize {
        match self {
            ColumnDataType::Null => 0,
            ColumnDataType::EightBit => 1,
            ColumnDataType::SixteenBit => 2,
            ColumnDataType::TwentyFourBit => 3,
            ColumnDataType::ThirtyTwoBit => 4,
            ColumnDataType::FortyEightBit => 6,
            ColumnDataType::SixtyFourBit => 8,
            ColumnDataType::Float => 8,
            ColumnDataType::IntegerZero => 0,
            ColumnDataType::IntegerOne => 0,
            ColumnDataType::Text(size) => *size,
            ColumnDataType::Blob(size) => *size,
        }
    }
}

pub(crate) fn get_data_type(coltype: usize) -> ColumnDataType {
    match coltype {
        0 => ColumnDataType::Null,
        1 => ColumnDataType::EightBit,
        2 => ColumnDataType::SixteenBit,
        3 => ColumnDataType::TwentyFourBit,
        4 => ColumnDataType::ThirtyTwoBit,
        5 => ColumnDataType::FortyEightBit,
        6 => ColumnDataType::SixtyFourBit,
        7 => ColumnDataType::Float,
        8 => ColumnDataType::IntegerZero,
        9 => ColumnDataType::IntegerOne,
        x if x >= 12 && x % 2 == 0 => ColumnDataType::Blob((x - 12) / 2),
        x if x >= 13 => ColumnDataType::Text((x - 13) / 2),
        _ => unreachable!("Invalid column type"),
    }
}
