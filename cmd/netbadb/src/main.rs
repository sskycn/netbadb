use std::env;
use std::io::{self, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    match netbadb_cli::run_cli(env::args_os().skip(1)) {
        Ok(output) => match io::stdout().lock().write_all(output.as_bytes()) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("netbadb: failed to write stdout: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("netbadb: {error}");
            ExitCode::from(error.exit_code())
        }
    }
}
