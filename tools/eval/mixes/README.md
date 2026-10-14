# Mix, single-song and playlist evaluation

`manifest.json` lists public YouTube cases with ground truth:

- **mix** — DJ sets, radio shows and compilations. `tracks` holds the published
  tracklist (start time, artist, title with its version). `id: true` marks
  unreleased or unknown entries and spoken intros (left out of recall; predictions
  there are neither right nor wrong); `speech: true` marks talk such as interviews
  (left out of recall; a predicted song there is a false positive).
- **single** — one song that must come back as exactly one segment (`expect`).
- **playlist** — entries in order (`null` for an entry that is not a song).

Only public URLs and published tracklists are stored. The tracklists were parsed
from chapters, descriptions or comments with `mf eval mix-draft <url>`, corrected
by hand where the published text was malformed, and cross-checked against the
audio by the hinted run (local preview matches and recognizer probes).

## Running

```sh
mf eval mixes tools/eval/mixes/manifest.json --mode both \
  --cache-db "$MF_EVAL_DIR/cache.sqlite3" --report-dir "$MF_EVAL_DIR/reports" \
  --yt-dlp yt-dlp --js-runtime deno
```

- Every recognizer answer is cached by signature in `--cache-db` and kept, so a
  second run replays without network calls. Probes that miss the cache are spaced
  `--spacing` seconds apart (default 10; never go below 9).
- `--cached-only` never calls the recognizer; uncached probes count as no match.
- Downloads are decoded once and kept in `media/` next to the cache DB; previews
  in `previews/`.
- `--mode blind` disables every hint (tracklists, titles, playlist metadata) and
  measures the audio path alone; `hinted` is the product default.
- Reports (Markdown, HTML and JSON with every predicted segment and the decision
  log) are written to `--report-dir`.

Run evaluation traffic from a machine other than the one you use the app on, so a
rate limit never hits your own listening. `run-remote.sh` syncs the source to
`$MF_EVAL_HOST`, builds the CLI there and runs the evaluation; set
`MF_EVAL_HOST` (an SSH destination) and optionally `MF_EVAL_DIR` (default
`mf-eval` in the remote home directory) and `MF_EVAL_TOOLS` (a directory with
`yt-dlp` and `deno`).

## Metrics

Per mix: recall and precision of track identity (version-agnostic, and with the
version required: a remix is not its original), start-time error (median and
p90), splits (a true track covered by more than one predicted segment of that
song), merges (a predicted segment covering most of two or more true tracks)
and recognizer calls per hour of audio. Per single: segments (must be one) and
whether the song and version are right. Per playlist: entries right and how many
needed audio.
