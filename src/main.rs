use std::io;
use std::io::prelude::*;
use std::iter;
use std::mem;
use std::ptr;


fn main() {
    let mut table = Table::new();

    let mut line = String::new();
    loop {
        print!("? ");
        io::stdout().flush().expect("Failed to flush standard output");
        io::stdin().read_line(&mut line).expect("Failed to read line");

        let trimmed = line.as_str().trim();
        if trimmed.starts_with(".") {
            match do_meta_command(trimmed) {
                MetaCommandResult::Success => (),
                MetaCommandResult::Exit => break,
                MetaCommandResult::Unrecognized => {
                    println!("Error: unrecognized meta-command `{}`", trimmed);
                }
            }
        } else {
            match prepare_statement(trimmed) {
                Some(statement) => {
                    match execute_statement(&statement, &mut table) {
                        Ok(_) => (),
                        Err(e) => println!("Error: {}", e),
                    }
                }
                None => println!("Error: could not parse statement `{}`", trimmed),
            }
        }

        line.clear();
    }
}


#[derive(Debug)]
struct Statement<'a> {
    kind: StatementKind,
    row_to_insert: Option<Box<Row<'a>>>,
}


#[derive(Debug)]
enum StatementKind {
    Insert,
    Select,
}


const ROW_USERNAME_SIZE: usize = 32;
const ROW_EMAIL_SIZE: usize = 255;

#[derive(Debug)]
struct Row<'a> {
    id: u32,
    username: &'a str,
    email: &'a str,
}


fn prepare_statement(command: &str) -> Option<Statement> {
    if command.starts_with("insert") {
        let mut iter = command.split_ascii_whitespace();
        iter.next();

        let e1 = iter.next();
        let e2 = iter.next();
        let e3 = iter.next();
        let e4 = iter.next();

        match (e1, e2, e3, e4) {
            (Some(word1), Some(word2), Some(word3), None) => {
                match word1.parse::<u32>() {
                    Ok(n) => {
                        let row = Row { id: n, username: word2, email: word3 };
                        return Some(Statement {
                            kind: StatementKind::Insert,
                            row_to_insert: Some(Box::new(row)),
                        });
                    },
                    Err(_) => {
                        return None;
                    },
                }
            },
            _ => {
                return None;
            },
        }
    } else if command.starts_with("select") {
        Some(Statement { kind: StatementKind::Select, row_to_insert: None })
    } else {
        None
    }
}


const TABLE_MAX_PAGES: usize = 100;
const PAGE_SIZE: usize = 4096;
const ROW_SIZE: usize = 291;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;


struct Table {
    nrows: usize,
    pages: Vec<Vec<u8>>,
}


impl Table {
    fn new() -> Table {
        let mut tab = Table { nrows: 0, pages: Vec::with_capacity(TABLE_MAX_PAGES) };

        for _ in 0..TABLE_MAX_PAGES {
            tab.pages.push(Vec::new());
        }

        tab
    }
}


fn execute_statement(statement: &Statement, table: &mut Table) -> Result<(), &'static str> {
    if table.nrows >= TABLE_MAX_ROWS {
        return Err("table is full");
    }

    let (page_num, offset) = row_slot(table, table.nrows);
    match statement.row_to_insert {
        Some(ref p) => serialize_row(p, &mut table.pages[page_num], offset),
        None => return Err("no row to insert"),
    }
    Ok(())
}


fn serialize_row(row: &Row, destination: &mut Vec<u8>, offset: usize) {
    let id_bytes = row.id.to_be_bytes();
    destination[offset] = id_bytes[0];
    destination[offset+1] = id_bytes[1];
    destination[offset+2] = id_bytes[2];
    destination[offset+3] = id_bytes[3];

    let padding = iter::repeat(0).take(ROW_USERNAME_SIZE - row.username.len());
    for (i, c) in row.username.bytes().chain(padding).enumerate() {
        destination[offset+3+i] = c;
    }

    let padding = iter::repeat(0).take(ROW_EMAIL_SIZE - row.email.len());
    for (i, c) in row.email.bytes().chain(padding).enumerate() {
        destination[offset+3+ROW_USERNAME_SIZE+i] = c;
    }
}


fn row_slot<'a>(table: &'a mut Table, row_num: usize) -> (usize, usize) {
    let page_num = row_num / ROWS_PER_PAGE;

    if table.pages[page_num].len() == 0 {
        table.pages[page_num].reserve(PAGE_SIZE);
        for _ in 0..PAGE_SIZE {
            table.pages[page_num].push(0);
        }
    }

    let row_offset = row_num % ROWS_PER_PAGE;
    return (page_num, row_offset * ROW_SIZE);
}


enum MetaCommandResult {
    Success,
    Exit,
    Unrecognized,
}


fn do_meta_command(command: &str) -> MetaCommandResult {
    if command == ".exit" {
        return MetaCommandResult::Exit;
    } else {
        return MetaCommandResult::Unrecognized;
    }
}
