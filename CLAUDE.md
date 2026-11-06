# CLAUDE.md

Collects planned power outage schedules for Ukrainian oblasts with GitHub Actions and publishes them as JSON for the Battery ETA iPhone app. Schedules are read through `ua_outages` (ukraine-energy-monitor, MIT) and written in the YASNO day format.

## Gate

`uv run pytest`

## Constraints

- Never copy code from ha-svitlo-yeah (GPL-3.0).
- Never use data from yaroslav2901 (no licence).
- Any paid API key lives only in a GitHub Actions secret. Never commit one, never print one.
- The `outage-feed` branch is force-pushed as one orphan commit every run. Never commit to it by hand.
