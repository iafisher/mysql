use std::io;
use std::io::prelude::*;

fn main() {
    let mut line = String::new();
    loop {
        print!("? ");
        io::stdout().flush().expect("Failed to flush standard output");
        io::stdin().read_line(&mut line).expect("Failed to read line");

        let trimmed = line.as_str().trim();
        if trimmed == ".exit" {
            break;
        } else {
            println!("Error: unrecognized command `{}`", trimmed)
        }

        line.clear();
    }
}
