use std::env;
use std::io::{self, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    match netbadb_lsp::run_cli(env::args_os().skip(1)) {
        Ok(Some(output)) => match io::stdout().lock().write_all(output.as_bytes()) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("netbadb-lsp: failed to write stdout: {error}");
                ExitCode::FAILURE
            }
        },
        Ok(None) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("netbadb-lsp: {error}");
            ExitCode::FAILURE
        }
    }
}
