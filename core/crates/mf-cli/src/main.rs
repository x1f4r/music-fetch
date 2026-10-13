//! `mf`: developer CLI for the Music Fetch engine.

use clap::{Parser, Subcommand};

mod library;
mod links;
mod lyrics;
mod media;
mod melody;
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
    #[command(flatten)]
    Melody(melody::MelodyCommand),
    /// Measure recognition quality on evaluation sets.
    #[command(subcommand)]
    Eval(EvalCommand),
}

#[derive(Subcommand)]
enum EvalCommand {
    #[command(flatten)]
    Lyrics(lyrics::EvalCommand),
    #[command(flatten)]
    Melody(melody::EvalCommand),
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Version => println!("{}", mfcore::core_version()),
        Cli::Library(command) => library::run(command)?,
        Cli::Media(command) => media::run(command)?,
        Cli::Shazam(command) => shazam::run(command)?,
        Cli::Links(command) => links::run(command)?,
        Cli::Lyrics(command) => lyrics::run(command)?,
        Cli::Melody(command) => melody::run(command)?,
        Cli::Eval(EvalCommand::Lyrics(command)) => lyrics::run(lyrics::Command::Eval(command))?,
        Cli::Eval(EvalCommand::Melody(command)) => melody::run(melody::MelodyCommand::Eval(command))?,
    }
    Ok(())
}
