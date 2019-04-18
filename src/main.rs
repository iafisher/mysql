use std::io;
use std::io::prelude::*;


fn main() {
    let mut table = Table { nrows: 0, pages: [None; TABLE_MAX_PAGES]};

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


struct Statement<'a> {
    kind: StatementKind,
    row_to_insert: Option<Row<'a>>,
}


enum StatementKind {
    Insert,
    Select,
}


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
                        return Some(Statement {
                            kind: StatementKind::Insert,
                            row_to_insert: Some(Row {
                                id: n,
                                username: word2,
                                email: word3,
                            }),
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


type Page = [u8; PAGE_SIZE];
struct Table<'a> {
    nrows: usize,
    pages: [Option<&'a mut Page>; TABLE_MAX_PAGES],
}


fn execute_statement(statement: &Statement, table: &mut Table) -> Result<(), &'static str> {
    if table.nrows >= TABLE_MAX_ROWS {
        return Err("table is full");
    }

    let (page_num, offset) = row_slot(&mut table, table.nrows);
    serialize_row(
        &statement.row_to_insert.unwrap(),
        &mut table.pages[page_num].unwrap(),
        offset
    );
    Ok(())
}


fn serialize_row(row: &Row, destination: &mut Page, offset: usize) {

}


fn row_slot<'a>(table: &'a mut Table, row_num: usize) -> (usize, usize) {
    let page_num = row_num / ROWS_PER_PAGE;

    if table.pages[page_num].is_none() {
        table.pages[page_num] = Some(&mut [0; PAGE_SIZE]);
    }

    let mut page = table.pages[page_num].unwrap();

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
