use std::ffi::{OsStr, OsString};
use std::process::Command;

use anyhow::{anyhow, Context, Result};

pub fn run_tools_ops<I, S>(subcommand: &[&str], args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let mut forwarded_args = subcommand
        .iter()
        .map(OsString::from)
        .collect::<Vec<OsString>>();
    forwarded_args.extend(args.into_iter().map(Into::into));

    let status = match run_binary("rustcta-tools-ops", &forwarded_args) {
        Ok(status) => status,
        Err(binary_err) => run_cargo(&forwarded_args).with_context(|| {
            format!(
                "failed to run rustcta-tools-ops binary directly ({binary_err}); cargo fallback also failed"
            )
        })?,
    };

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("rustcta-tools-ops exited with status {status}"))
    }
}

fn run_binary(program: &str, args: &[OsString]) -> std::io::Result<std::process::ExitStatus> {
    Command::new(program).args(args).status()
}

fn run_cargo(args: &[OsString]) -> std::io::Result<std::process::ExitStatus> {
    Command::new("cargo")
        .arg("run")
        .arg("-q")
        .arg("-p")
        .arg("rustcta-tools-ops")
        .arg("--")
        .args(args.iter().map(OsStr::new))
        .status()
}
