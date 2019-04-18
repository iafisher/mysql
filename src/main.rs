use std::io;
use std::io::prelude::*;


fn main() {
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
                Some(statement) => execute_statement(&statement),
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
        Some(Statement { kind: StatementKind::Insert, row_to_insert: None })
    } else if command.starts_with("select") {
        Some(Statement { kind: StatementKind::Select, row_to_insert: None })
    } else {
        None
    }
}


fn execute_statement(statement: &Statement) {
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
