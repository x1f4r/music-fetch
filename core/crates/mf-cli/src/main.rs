//! `mf`: developer CLI for the Music Fetch engine.

use clap::Parser;

mod shazam;

#[derive(Parser)]
#[command(name = "mf", version, about = "Music Fetch engine CLI")]
enum Cli {
    /// Print the engine version.
    Version,
    #[command(flatten)]
    Shazam(shazam::Command),
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Version => println!("{}", mfcore::core_version()),
        Cli::Shazam(command) => shazam::run(command)?,
    }
    Ok(())
}
