//! `mf`: developer CLI for the Music Fetch engine.

use clap::Parser;

mod library;
mod links;
mod lyrics;
mod media;
mod shazam;

#[derive(Parser)]
#[command(name = "mf", version, about = "Music Fetch engine CLI")]
enum Cli {
    /// Print the engine version.
    Version,
    #[command(flatten)]
    Library(library::Command),
    #[command(flatten)]
    Media(media::Command),
    #[command(flatten)]
    Shazam(shazam::Command),
    #[command(flatten)]
    Links(links::LinksCommand),
    #[command(flatten)]
    Lyrics(lyrics::Command),
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Version => println!("{}", mfcore::core_version()),
        Cli::Library(command) => library::run(command)?,
        Cli::Media(command) => media::run(command)?,
        Cli::Shazam(command) => shazam::run(command)?,
        Cli::Links(command) => links::run(command)?,
        Cli::Lyrics(command) => lyrics::run(command)?,
    }
    Ok(())
}
