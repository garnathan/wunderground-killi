# wunderground-killi

Live readings from a **SensorPush HTP.xw** environmental sensor in the garden,
published every 5 minutes by a Raspberry Pi (`home-pi`) over Bluetooth LE.

- **Live trend chart:** https://garnathan.github.io/wunderground-killi/
- **Latest reading (JSON):** [`latest.json`](latest.json) — consumed by the Android home-screen widget
- **Rolling ~7-day window:** [`data/recent.json`](data/recent.json) — drives the chart
- **Full archive:** `data/YYYY-MM.csv` — every reading, one row each (open in Excel/pandas)

Also uploaded to Weather Underground as a Personal Weather Station.

## Reading schema

| field | unit | notes |
|---|---|---|
| `ts` | ISO-8601 | local time with offset |
| `epoch` | seconds | UTC unix time |
| `temperature_c` / `temperature_f` | °C / °F | |
| `humidity_pct` | % RH | |
| `pressure_sealevel_mbar` | mbar | corrected to sea level (50 m altitude) — matches the SensorPush app |
| `pressure_station_mbar` | mbar | raw sensor (station) pressure |
| `dew_point_c` | °C | computed (Magnus) |
| `heat_index_c` | °C | computed (NWS) |
| `wu_status` | string | Weather Underground upload result (`success` / `skipped` / error) |

Data is public — it's the same information already published to the public Weather Underground station.
