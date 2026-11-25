# battery-eta-outages

Planned power outage schedules for Ukrainian oblasts, collected every 10 minutes by GitHub Actions for the [Battery ETA](https://lapin7771n.github.io/marstek_analytics/) iPhone app.

The `outage-feed` branch holds one JSON file per grid operator plus `index.json`. Each file uses the YASNO day format: per queue, `today` and `tomorrow` with outage slots in minutes from midnight, Kyiv time.

Schedules come from the operators' public pages, read through [ua_outages](https://github.com/fastiuk/ukraine-energy-monitor) (MIT, Copyright (c) 2026 Yevhen Fastiuk).

[alerts.energy](https://alerts.energy) is the only source for Kharkiv, Poltava, Zaporizhzhia, Chernivtsi and Vinnytsia oblasts. For Kyiv oblast, Zhytomyr, Kirovohrad, Mykolaiv, Odesa, Sumy, Cherkasy and Chernihiv, the feed requests the area's paid schedule only when its free sources fail in that run. A Chernihiv response with no queue schedule also counts as a failure. The API key lives in the `ALERTS_ENERGY_KEY` secret. Schedules from alerts.energy carry `source: "alerts.energy"` in `index.json`.
