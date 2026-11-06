# battery-eta-outages

Planned power outage schedules for Ukrainian oblasts, collected every 10 minutes by GitHub Actions for the [Battery ETA](https://lapin7771n.github.io/marstek_analytics/) iPhone app.

The `outage-feed` branch holds one JSON file per grid operator plus `index.json`. Each file uses the YASNO day format: per queue, `today` and `tomorrow` with outage slots in minutes from midnight, Kyiv time.

Schedules come from the operators' public pages, read through [ua_outages](https://github.com/fastiuk/ukraine-energy-monitor) (MIT, Copyright (c) 2026 Yevhen Fastiuk).

Oblasts without a free source are filled from [alerts.energy](https://alerts.energy) (paid API, key in the `ALERTS_ENERGY_KEY` secret). Their `index.json` entries carry `source: "alerts.energy"`.
