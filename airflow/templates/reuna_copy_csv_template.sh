#!/usr/bin/env bash
set -euo pipefail


{% if {{params.virtualenv}} -%}
conda activate {{params.virtualenv}}
{% endif %}

python main.py psql-copy-csv --config_json={{params.vars}}

