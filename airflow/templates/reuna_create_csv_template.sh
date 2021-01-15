#!/usr/bin/env bash
set -euo pipefail


{% if {{params.virtualenv}} -%}
conda activate {{params.virtualenv}}
{% endif %}

python main.py create-csv --config_json={{params.vars}}
