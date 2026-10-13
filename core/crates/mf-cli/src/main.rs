//! `mf`: developer CLI for the Music Fetch engine.

use clap::Parser;

mod library;
mod media;

#[derive(Parser)]
#[command(name = "mf", version, about = "Music Fetch engine CLI")]
enum Cli {
    /// Print the engine version.
    Version,
    #[command(flatten)]
    Library(library::Command),
    Media(media::Command),
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Version => println!("{}", mfcore::core_version()),
        Cli::Library(command) => library::run(command)?,
        Cli::Media(command) => media::run(command)?,
    }
    Ok(())
}
