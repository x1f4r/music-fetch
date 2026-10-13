//! `mf`: developer CLI for the Music Fetch engine.

mod lyrics;

use clap::Parser;

#[derive(Parser)]
#[command(name = "mf", version, about = "Music Fetch engine CLI")]
enum Cli {
    /// Print the engine version.
    Version,
    #[command(flatten)]
    Lyrics(lyrics::Command),
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Version => println!("{}", mfcore::core_version()),
        Cli::Lyrics(command) => lyrics::run(command)?,
    }
    Ok(())
}
