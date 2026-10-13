//! `mf resolve`, `mf canon`, `mf enrich`: links and the catalog from the command line.

use anyhow::Context;
use clap::Subcommand;
use mfcore::links::{self, Resolution};
use mfcore::util::cancel::CancelToken;
use mfcore::{Track, catalog};

#[derive(Subcommand)]
pub enum LinksCommand {
    /// Resolve a link (or shared text containing one) and print the result as JSON.
    Resolve {
        /// Link or text containing a link.
        url: String,
        /// Storefront for services that need one.
        #[arg(long, default_value = "us")]
        country: String,
        /// Also enrich tracks and hints from Deezer and iTunes (slow for playlists).
        #[arg(long)]
        enrich: bool,
    },
    /// Print the canonical form of a link (offline).
    Canon { url: String },
    /// Look up "Artist - Title" in the catalogs and print the enriched track as JSON.
    Enrich {
        /// "Artist - Title", or just a title.
        query: String,
        #[arg(long, default_value = "us")]
        country: String,
    },
}

pub fn run(command: LinksCommand) -> anyhow::Result<()> {
    match command {
        LinksCommand::Canon { url } => {
            println!("{}", links::canonicalize(&url)?);
            Ok(())
        }
        LinksCommand::Resolve {
            url,
            country,
            enrich,
        } => runtime()?.block_on(resolve(&url, &country, enrich)),
        LinksCommand::Enrich { query, country } => {
            runtime()?.block_on(enrich_query(&query, &country))
        }
    }
}

fn runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting the async runtime")
}

async fn resolve(url: &str, country: &str, enrich: bool) -> anyhow::Result<()> {
    let http = links::http_client(&mfcore::core_version())?;
    let cancel = CancelToken::new();
    let mut resolution = links::resolve(&http, url, country, &cancel).await?;
    if enrich {
        resolution = match resolution {
            Resolution::Tracks { source, tracks } => {
                let mut enriched = Vec::with_capacity(tracks.len());
                for track in tracks {
                    enriched.push(catalog::enrich(&http, track, country, &cancel).await);
                }
                Resolution::Tracks {
                    source,
                    tracks: enriched,
                }
            }
            Resolution::Media {
                mut source,
                url,
                direct,
            } => {
                let mut hints = Vec::with_capacity(source.hints.len());
                for hint in std::mem::take(&mut source.hints) {
                    hints.push(catalog::enrich(&http, hint, country, &cancel).await);
                }
                source.hints = hints;
                Resolution::Media {
                    source,
                    url,
                    direct,
                }
            }
        };
    }
    println!("{}", serde_json::to_string_pretty(&resolution)?);
    Ok(())
}

async fn enrich_query(query: &str, country: &str) -> anyhow::Result<()> {
    let http = links::http_client(&mfcore::core_version())?;
    let cancel = CancelToken::new();
    let (artist, title) = match query.split_once(" - ") {
        Some((artist, title)) => (artist.trim(), title.trim()),
        None => ("", query.trim()),
    };
    anyhow::ensure!(!title.is_empty(), "enter \"Artist - Title\"");
    let track = Track {
        title: title.to_string(),
        artist: artist.to_string(),
        ..Track::default()
    };
    let enriched = catalog::enrich(&http, track, country, &cancel).await;
    println!("{}", serde_json::to_string_pretty(&enriched)?);
    Ok(())
}
