//! `mf`: developer CLI for the Music Fetch engine.

use clap::Parser;

mod library;
mod media;
mod shazam;
mod links;

#[derive(Parser)]
#[command(name = "mf", version, about = "Music Fetch engine CLI")]
enum Cli {
    /// Print the engine version.
    Version,
    #[command(flatten)]
    Library(library::Command),
    Media(media::Command),
    Shazam(shazam::Command),
    Links(links::LinksCommand),
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Version => println!("{}", mfcore::core_version()),
        Cli::Library(command) => library::run(command)?,
        Cli::Media(command) => media::run(command)?,
        Cli::Shazam(command) => shazam::run(command)?,
        Cli::Links(command) => links::run(command)?,
    }
    Ok(())
}
