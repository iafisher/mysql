/**
 * A basic SQLite clone in Rust.
 *
 * Based on "Let's Build a Simple Database" tutorial by cstack.
 * https://cstack.github.io/db_tutorial/
 *
 * The main data structure is a Table, which is array of fixed-size (4096 byte) pages of binary
 * data.
 *
 * Author:  Ian Fisher (iafisher@protonmail.com)
 * Version: May 2019
 */
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::iter;
use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};
use std::str;


fn main() {
    let mut table = db_open("db.mysql");

    let mut line = String::new();
    loop {
        print!("? ");
        io::stdout().flush().expect("Failed to flush standard output");
        io::stdin().read_line(&mut line).expect("Failed to read line");

        let trimmed = line.as_str().trim();
        if trimmed.starts_with(".") {
            // Handle meta-commands.
            match do_meta_command(trimmed, &table) {
                MetaCommandResult::Success => (),
                MetaCommandResult::Exit => break,
                MetaCommandResult::Unrecognized => {
                    println!("Error: unrecognized meta-command `{}`", trimmed);
                }
            }
        } else {
            // Handle SQL commands.
            if let Some(statement) = prepare_statement(trimmed) {
                let result = execute_statement(&statement, &mut table);
                if let Err(e) = result {
                    println!("Error: {}", e);
                }
            } else {
                println!("Error: could not parse statement `{}`", trimmed);
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


const ROW_ID_SIZE: usize = 4;
const ROW_USERNAME_SIZE: usize = 32;
const ROW_EMAIL_SIZE: usize = 255;
const ROW_USERNAME_START: usize = ROW_ID_SIZE;
const ROW_EMAIL_START: usize = ROW_USERNAME_START + ROW_USERNAME_SIZE;

#[derive(Debug)]
struct Row<'a> {
    id: u32,
    username: &'a str,
    email: &'a str,
}


/// Parse a string into a SQL statement.
fn prepare_statement(command: &str) -> Option<Statement> {
    if command.starts_with("insert ") {
        let words: Vec<&str> = command.split_ascii_whitespace().collect();

        if words.len() == 4 {
            let idstr = words[1];
            let username = words[2];
            let email = words[3];

            if username.len() > ROW_USERNAME_SIZE || email.len() > ROW_EMAIL_SIZE {
                return None;
            }

            match idstr.parse::<u32>() {
                Ok(n) => {
                        let row = Row { id: n, username, email };
                        return Some(Statement {
                            kind: StatementKind::Insert,
                            row_to_insert: Some(Box::new(row)),
                        });
                },
                _ => {
                    return None;
                }
            }
        } else {
            return None;
        }
    } else if command == "select" || command.starts_with("select ") {
        Some(Statement { kind: StatementKind::Select, row_to_insert: None })
    } else {
        None
    }
}


fn db_open(path: &str) -> Table {
    let pager = Pager::new(path);
    let nrows = pager.file_length / ROW_SIZE;

    Table { nrows, pager }
}


fn db_open_new(path: &str) -> Table {
    fs::remove_file(&path);
    db_open(&path)
}


const TABLE_MAX_PAGES: usize = 100;  // An arbitrary maximum.
const PAGE_SIZE: usize = 4096;  // Equivalent to virtual memory page size on many OSes.
const ROW_SIZE: usize = 291;  // Calculated from the Row struct.
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;


/// Represents the binary format of a database table.
struct Table {
    nrows: usize,
    pager: Pager,
}


impl Drop for Table {
    fn drop(&mut self) {
        let num_full_pages = self.nrows / ROWS_PER_PAGE;

        for i in 0..num_full_pages {
            if self.pager.pages[i].len() > 0 {
                self.pager.flush(i, PAGE_SIZE);
            }
        }

        // Could be some additional rows on a last, partial page.
        let num_additional_rows = self.nrows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            if self.pager.pages[num_full_pages].len() > 0 {
                self.pager.flush(num_full_pages, num_additional_rows * ROW_SIZE);
            }
        }

        // Automatically closed when it goes out of scope.
        let mut _file = unsafe { File::from_raw_fd(self.pager.fd) };
    }
}


/// Represents a location in a table.
struct Cursor<'a> {
    table: &'a mut Table,
    rowno: usize,
    end_of_table: bool,
}


impl<'a> Cursor<'a> {
    fn from_start(table: &mut Table) -> Cursor {
        // This line is necessary because `table` is moved into the Cursor object in the next
        // line, so we can't access table.nrows at that point.
        let nrows = table.nrows;
        Cursor { table, rowno: 0, end_of_table: (nrows == 0) }
    }

    fn from_end(table: &mut Table) -> Cursor {
        // This line is necessary because `table` is moved into the Cursor object in the next
        // line, so we can't access table.nrows at that point.
        let nrows = table.nrows;
        Cursor { table, rowno: nrows, end_of_table: true }
    }

    fn advance(&mut self) {
        self.rowno += 1;
        self.end_of_table = self.rowno == self.table.nrows;
    }
}


/// An abstraction for fetching pages.
struct Pager {
    fd: RawFd,
    file_length: usize,
    pages: Vec<Vec<u8>>,
}


impl Pager {
    fn new(path: &str) -> Self {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Failed to open file");

        let file_length = file.seek(SeekFrom::End(0)).expect("Seeking end of file failed");
        let fd = file.into_raw_fd();

        let mut pager = Self {
            fd, file_length: file_length as usize, pages: Vec::with_capacity(TABLE_MAX_PAGES)
        };
        for _ in 0..TABLE_MAX_PAGES {
            pager.pages.push(Vec::new());
        }

        pager
    }

    fn allocate_page(&mut self, page_num: usize) {
        if self.pages[page_num].len() == 0 {
            // Cache miss
            self.pages[page_num].reserve(PAGE_SIZE);

            // Zero out memory.
            for _ in 0..PAGE_SIZE {
                self.pages[page_num].push(0);
            }

            let mut npages = self.file_length / PAGE_SIZE;

            if self.file_length % PAGE_SIZE != 0 {
                npages += 1;
            }

            if page_num <= npages {
                let mut file = unsafe { File::from_raw_fd(self.fd) };
                file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .expect("File seek failed");

                file.read(&mut self.pages[page_num]).expect("Reading from file failed");

                self.fd = file.into_raw_fd();
            }
        }
    }

    fn flush(&mut self, page_num: usize, size: usize) {
        let mut file = unsafe { File::from_raw_fd(self.fd) };
        file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .expect("File seek failed");

        file.write(&self.pages[page_num][0..size]).expect("File write failed");

        self.fd = file.into_raw_fd();
    }
}


/// Execute a prepared statement on the database.
fn execute_statement(statement: &Statement, table: &mut Table) -> Result<(), &'static str> {
    match statement.kind {
        StatementKind::Insert => execute_insert(statement, table),
        StatementKind::Select => execute_select(statement, table),
    }
}


/// Execute an INSERT statement.
fn execute_insert(statement: &Statement, mut table: &mut Table) -> Result<(), &'static str> {
    if table.nrows >= TABLE_MAX_ROWS {
        return Err("table is full");
    }

    let mut cursor = Cursor::from_end(&mut table);
    let (page_num, offset) = cursor_value(&mut cursor);
    serialize_row(
        statement.row_to_insert.as_ref().unwrap(),
        &mut cursor.table.pager.pages[page_num],
        offset
    );
    table.nrows += 1;
    Ok(())
}


/// Execute a SELECT statement.
fn execute_select(statement: &Statement, mut table: &mut Table) -> Result<(), &'static str> {
    let mut cursor = Cursor::from_start(&mut table);
    while !cursor.end_of_table {
        let (page_num, offset) = cursor_value(&mut cursor);
        println!("{:?}", deserialize_row(&cursor.table.pager.pages[page_num], offset));
        cursor.advance();
    }
    Ok(())
}


/// Write a row to the destination buffer.
fn serialize_row(row: &Row, destination: &mut Vec<u8>, offset: usize) {
    let id_bytes = row.id.to_be_bytes();
    destination[offset] = id_bytes[0];
    destination[offset+1] = id_bytes[1];
    destination[offset+2] = id_bytes[2];
    destination[offset+3] = id_bytes[3];

    let padding = iter::repeat(0).take(ROW_USERNAME_SIZE - row.username.len());
    for (i, c) in row.username.bytes().chain(padding).enumerate() {
        destination[offset+4+i] = c;
    }

    let padding = iter::repeat(0).take(ROW_EMAIL_SIZE - row.email.len());
    for (i, c) in row.email.bytes().chain(padding).enumerate() {
        destination[offset+4+ROW_USERNAME_SIZE+i] = c;
    }
}


/// Read a row from the source buffer.
fn deserialize_row(source: &Vec<u8>, offset: usize) -> Row {
    let id: u32 =
        (u32::from(source[offset]) << 24) +
        (u32::from(source[offset+1]) << 16) +
        (u32::from(source[offset+2]) << 8) +
        u32::from(source[offset+3]);

    // Using unchecked UTF-8 conversion because lazy.
    unsafe {
        let username = str::from_utf8_unchecked(
            deserialize_string(&source, offset+ROW_USERNAME_START, ROW_USERNAME_SIZE)
        );
        let email = str::from_utf8_unchecked(
            deserialize_string(&source, offset+ROW_EMAIL_START, ROW_EMAIL_SIZE)
        );
        return Row { id, username, email };
    }
}


/// Helper function to read a slice of bytes of an expected length from a source buffer.
fn deserialize_string(source: &Vec<u8>, offset: usize, length: usize) -> &[u8] {
    let nullpos = source[offset..].iter().position(|&x| x == 0);
    match nullpos {
        Some(p) if p < length => &source[offset..(offset + p)],
        _ => &source[offset..offset+length],
    }
}


/// Return (page number, byte offset) for position indicated by the given cursor. Also allocates
/// a page if the row requested would be in an unallocated page (which is why Cursor is mutable).
fn cursor_value(cursor: &mut Cursor) -> (usize, usize) {
    let page_num = cursor.rowno / ROWS_PER_PAGE;
    cursor.table.pager.allocate_page(page_num);

    let row_offset = cursor.rowno % ROWS_PER_PAGE;
    return (page_num, row_offset * ROW_SIZE);
}


enum MetaCommandResult {
    Success,
    Exit,
    Unrecognized,
}


/// Execute a meta-command (i.e., a non-SQL statement in the shell).
fn do_meta_command(command: &str, table: &Table) -> MetaCommandResult {
    if command == ".exit" {
        return MetaCommandResult::Exit;
    } else if command == ".size" {
        println!("{} row(s)", table.nrows);
        return MetaCommandResult::Success;
    } else {
        return MetaCommandResult::Unrecognized;
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn insert_and_retrieve() {
        let mut table = db_open_new("testdb.mysql");

        let insert = Statement {
            kind: StatementKind::Insert,
            row_to_insert: Some(Box::new(Row {
                id: 1,
                username: "jdoe",
                email: "jdoe@example.com",
            })),
        };

        let mut result = execute_statement(&insert, &mut table);
        assert!(result.is_ok());

        let select = Statement {
            kind: StatementKind::Select,
            row_to_insert: None,
        };

        result = execute_statement(&select, &mut table);
        assert!(result.is_ok());
    }

    #[test]
    fn max_rows() {
        let mut table = db_open_new("testdb.mysql");

        for _ in 0..TABLE_MAX_ROWS {
            let insert = Statement {
                kind: StatementKind::Insert,
                row_to_insert: Some(Box::new(Row {
                    id: 1,
                    username: "jdoe",
                    email: "jdoe@example.com",
                })),
            };

            let result = execute_statement(&insert, &mut table);
            assert!(result.is_ok());
        }

        let insert = Statement {
            kind: StatementKind::Insert,
            row_to_insert: Some(Box::new(Row {
                id: 9999,
                username: "jdoe",
                email: "jdoe@example.com",
            })),
        };

        let result = execute_statement(&insert, &mut table);
        assert!(result.is_err());
    }

    #[test]
    fn username_too_long() {
        let result = prepare_statement(
            "insert 1 a-string-that-has-more-than-32-characters-in-it user@example.com"
        );
        assert!(result.is_none());
    }
}
