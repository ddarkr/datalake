# Contributing

Short rules for external contributions. The full workflow lives in
[docs/development.md](docs/development.md) — read it before opening a PR.

## Ground rules

- `compose.yaml` is generated. Never edit it by hand: change `compose/*.yaml`
  or `scripts/*`, then regenerate with the exact command order in
  `docs/development.md` (render → `check_env.py` → render tests → per-profile
  `config --quiet`).
- Keep `.env.example` and `tools/check_env.py` in sync: every Compose variable
  must have an example entry.
- Tests must not write to a production server or a real vehicle. Use
  `.env.example`-based checks and synthetic fixtures. Isolated loopback
  servers or throwaway containers are allowed; existing GreptimeDB, S3,
  SocketCAN, and Fleet endpoints are not test targets.
- Do not commit secrets, vehicle identifiers, calibration values tied to a
  real vehicle, or internal host/path details. Public examples must be
  synthetic.
- Keep external DBC files, VSS definitions, and NASA `.mat` data outside the
  public repository. Follow the input provenance rules in `docs/operations.md`
  and the separate terms in `THIRD_PARTY_NOTICES.md`.

## What makes a good PR

- One focused change with a clear scope (bug fix, docs, single-feature).
- Regenerated `compose.yaml` included when any fragment changed, with CI-green
  proof of the render-order check.
- Tests for behavior changes where they exist for that area
  (`tests/test_*.py`, `tests/test_plugin_*.mjs`, Hermes unittest suite).
- No scope creep: retries, telemetry, refactors, or abstractions beyond the
  stated fix need their own discussion first.

## License

By contributing, you agree your contribution is under this repository's MIT
license (`LICENSE`) with no additional terms. Upstream material you reference
(DBC definitions, datasets, price tables) keeps its own license — see
`THIRD_PARTY_NOTICES.md`.
