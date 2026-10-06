# Offline installed native acceptance

Run `pnpm validate` with Node 22 and Python 3.12 for the unit and synthetic runtime
checks. Run `pnpm acceptance` separately with `INGESTRON_ACCEPTANCE_CLI_PACKAGE`
pointing at an installed CLI package directory and `INGESTRON_ACCEPTANCE_OUTPUT`
pointing at an isolated output directory. Accepted CLI/core pairs are
0.17.8/0.12.14 and 0.17.9/0.12.15 for their older compatible snapshots;
private 3.11.0 uses 0.17.19/0.12.45. No sibling core source is imported.

The gate copies built `plugin` assets into a fresh local Git repository, commits
and tags that snapshot, then installs its exact version into synthetic projects.
It preserves the snapshot and writes `native-acceptance.tap` and
`native-acceptance.json`. Evidence records compiler versions, provider commit and
module hash. Set `INGESTRON_ACCEPTANCE_PROOF` to save another copy of the JSON.
No remote Git, package download, source read or workspace operation occurs.

Scenarios cover all seven declared standards, authoring, dependent materialised
views, managed/external/adopted pipeline ownership, production identity and
permissions, paused schedules, notifications, timeouts, freshness and rejection of
incomplete or corrupt snapshots. Installed project builds check supported quality,
required unsupported-rule rejection, preservation of the last accepted output and
explicit report policy without claiming execution.

The snapshot callback uses a synthetic Spark boundary. A passed local result proves
offline compiler integration and callback behaviour. Vendor SDK compatibility,
source access, cloud execution and real concurrent recovery remain separate gates.

Private Python scenarios compare SDK and CLI authoring proposals, preserve source
bytes and planned hashes, generate typed materialised views and committed-table
quality tasks, and refuse changed source, invalid syntax and ingestion language
options. Synthetic source contains a sentinel action that must never run during
generation. Separate Studio/editor acceptance exercises reviewed saves and
configuration-only preparation through their actual installed boundaries.

For private 3.8.0 snapshots, required supported rules must declare after-load
coverage and generate a quality task between ingestion and a dependent
transformation. Unsupported engine rules still block and preserve the last output.
Example installation references remain public versions; the harness substitutes
only the locally snapshotted provider reference when qualifying a private candidate.
