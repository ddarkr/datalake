# Third-party notices

This repository's own code and docs are under the MIT license in `LICENSE`
(copyright 2026 ddarkr and contributors).

That license covers only this repository's original work. It does not cover
separately obtained definitions, data, price tables, or the upstream projects
below — each keeps its own license and terms. Nothing here vendors
third-party datasets or code; external material is fetched at runtime or
supplied by the operator.

## License scope

- Covered by `LICENSE` (MIT): scripts, tools, tests, Compose fragments,
  dashboards, plugin adapters, and docs authored in this repository.
- Not covered by `LICENSE`: operator-supplied DBC files and VSS mapping
  content, NASA `.mat` files, LiteLLM price-table responses, and upstream
  container images / packages listed below.

## Runtime-fetched or operator-supplied inputs (not redistributed)

| Input | How it enters | Terms / source |
| --- | --- | --- |
| KUKSA `dbcfeederlib` (`__init__.py`, `dbcparser.py`, `dbc2vssmapper.py`) at pinned commit `d03dd7db364dd1ce9f7d0c614d80ebf6642ad167` | `redecode-deps` downloads from `eclipse-kuksa/kuksa-can-provider`, verifies SHA256, stores the upstream `LICENSE` alongside | Apache License 2.0, © Eclipse KUKSA contributors: https://github.com/eclipse-kuksa/kuksa-can-provider |
| Operator DBC files (`DBC_*_URL`) and VSS mapping | Supplied by the vehicle operator, hash-pinned per decode epoch | License unknown and operator-dependent. This project claims no license over external DBC definitions; redistribution rights must be confirmed with the DBC supplier before sharing those files. |
| VSS signal paths (e.g. `Vehicle.Speed`) | Names referenced by recorders; no VSS definition files are vendored here | VSS standard is Mozilla Public License 2.0: https://github.com/COVESA/vehicle_signal_specification — path-name references only, no MPL-covered files distributed. |
| LiteLLM `model_prices_and_context_window.json` | `aggregate` fetches from `BerriAI/litellm`, memory-only, refreshed at most daily | MIT License, © 2023 Berri AI: https://github.com/BerriAI/litellm — cached list-rate estimates only, not invoices. |
| NASA PCoE battery `.mat` files | Operator downloads separately; `battery_reference.py` reads them offline with scipy | Public dataset, not vendored. Required citation: B. Saha and K. Goebel (2007), "Battery Data Set", NASA Prognostics Data Repository, NASA Ames Research Center. Repository terms: publications using the data should acknowledge the repository and the donators; data is used at the user's own risk, NASA and donators assume no liability: https://www.nasa.gov/intelligent-systems-division/discovery-and-systems-health/pcoe/pcoe-data-set-repository/ |

## Container images (used unmodified, not redistributed as source)

| Image | License |
| --- | --- |
| `grafana/grafana:12.4.0` | GNU AGPLv3 (applies to Grafana itself; this repo modifies no Grafana code): https://github.com/grafana/grafana/blob/main/LICENSE |
| `grafana/alloy:v1.19.2` | Apache License 2.0: https://github.com/grafana/alloy/blob/main/LICENSE |
| `greptime/greptimedb:v1.2.1` | Apache License 2.0: https://github.com/GreptimeTeam/greptimedb/blob/main/LICENSE |
| `telegraf:1.40.0` | MIT License, © 2015–2025 InfluxData Inc.: https://github.com/influxdata/telegraf/blob/v1.40.0/LICENSE |
| `ghcr.io/eclipse-kuksa/kuksa-databroker:0.7.1`, `ghcr.io/eclipse-kuksa/kuksa-can-provider/can-provider:0.5.0` | Apache License 2.0 (same Eclipse KUKSA terms as above) |
| `python:3.12.8-slim-bookworm` | Python Software Foundation license + Debian base; see image docs at https://hub.docker.com/_/python |

## Python / Node packages (installed at deploy time, not vendored)

| Package @ pinned version | License |
| --- | --- |
| [`kuksa-client==0.6.0`](https://pypi.org/project/kuksa-client/0.6.0/) | Apache-2.0 (Eclipse KUKSA Project) |
| [`python-can==4.6.1`](https://pypi.org/project/python-can/4.6.1/) | LGPL-3.0-only — used unmodified via pip; no LGPL source is vendored or modified here |
| [`asammdf==8.8.27`](https://pypi.org/project/asammdf/8.8.27/) | LGPLv3+ — same unmodified-pip-use basis as `python-can` |
| [`cantools==40.7.1`](https://pypi.org/project/cantools/40.7.1/) | MIT |
| [`zstd==1.5.6.1`](https://pypi.org/project/zstd/1.5.6.1/) | BSD |
| [`py-expression-eval==0.3.14`](https://pypi.org/project/py-expression-eval/0.3.14/) | MIT (port of js-expression-eval by Matthew Crumley) |
| [`boto3==1.43.98`](https://pypi.org/project/boto3/1.43.98/) | Apache-2.0 (Amazon Web Services) |
| [`pyzmq==26.2.0`](https://pypi.org/project/pyzmq/26.2.0/) | BSD 3-Clause (includes bundled libzmq, which is MPL-2.0: https://github.com/zeromq/libzmq/blob/master/LICENSE) |
| [`PyYAML==6.0.2`](https://pypi.org/project/PyYAML/6.0.2/) (renderer/CI only) | MIT |
| [`jsonc-parser@3.3.1`](https://www.npmjs.com/package/jsonc-parser/v/3.3.1) (plugins) | MIT, © Microsoft Corporation |

License labels above were verified against the upstream registry or
repository pages named in each row (PyPI JSON API, npm registry API, and
the linked LICENSE files). They are a good-faith attribution record, not
a legal audit.

## What is deliberately not claimed

- No permission is claimed over operator DBC definitions, Tesla signal
  semantics, or any vehicle firmware/mapping content.
- No patent grant is claimed or implied for third-party methods (e.g. the
  CB-R supplier patents noted in `docs/tesla_fleet.md`); public patents do
  not imply implementation permission.
- NASA lab-cell models are lab-only references, never Tesla pack lifetime
  claims.
