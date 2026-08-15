use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    match netbadb_codegen::run_cli(env::args_os().skip(1)) {
        Ok(message) => {
            if let Some(message) = message {
                println!("{message}");
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("netbadb-codegen: {error}");
            ExitCode::FAILURE
        }
    }
}
