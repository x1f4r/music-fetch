//! `mf`: developer CLI for the Music Fetch engine.

mod melody;

use clap::Parser;

#[derive(Parser)]
#[command(name = "mf", version, about = "Music Fetch engine CLI")]
enum Cli {
    /// Print the engine version.
    Version,
    #[command(flatten)]
    Melody(melody::MelodyCommand),
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Version => println!("{}", mfcore::core_version()),
        Cli::Melody(cmd) => melody::run(cmd)?,
    }
    Ok(())
}
