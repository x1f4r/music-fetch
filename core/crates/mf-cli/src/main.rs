//! `mf`: developer CLI for the Music Fetch engine.

use clap::{Parser, Subcommand};

mod encoder;
mod eval_mixes;
mod library;
mod links;
mod lyrics;
mod media;
mod melody;
mod session;
mod shazam;
mod stub;
mod youtube_record;

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
    #[command(flatten)]
    Session(session::Command),
    /// Build, inspect, install or remove the melody index.
    #[command(subcommand)]
    Index(IndexCommand),
    /// Measure recognition quality on evaluation sets.
    #[command(subcommand)]
    Eval(EvalCommand),
    /// Developer tools: the stub recognizer, the header policy.
    #[command(subcommand)]
    Dev(stub::Command),
}

#[derive(Subcommand)]
enum IndexCommand {
    #[command(flatten)]
    Melody(melody::IndexCommand),
    #[command(flatten)]
    Engine(session::IndexCommand),
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
        Cli::Session(command) => session::run(command)?,
        Cli::Index(IndexCommand::Melody(command)) => melody::run(melody::MelodyCommand::Index(command))?,
        Cli::Index(IndexCommand::Engine(command)) => session::run_index(command)?,
        Cli::Eval(EvalCommand::Lyrics(command)) => lyrics::run(lyrics::Command::Eval(command))?,
        Cli::Eval(EvalCommand::Melody(command)) => melody::run(melody::MelodyCommand::Eval(command))?,
        Cli::Dev(command) => stub::run(command)?,
    }
    Ok(())
}
