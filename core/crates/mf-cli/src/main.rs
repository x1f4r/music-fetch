//! `mf`: developer CLI for the Music Fetch engine.

use clap::Parser;

mod links;

#[derive(Parser)]
#[command(name = "mf", version, about = "Music Fetch engine CLI")]
enum Cli {
    /// Print the engine version.
    Version,
    #[command(flatten)]
    Links(links::LinksCommand),
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Version => println!("{}", mfcore::core_version()),
        Cli::Links(command) => links::run(command)?,
    }
    Ok(())
}
