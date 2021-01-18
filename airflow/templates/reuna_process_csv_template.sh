#!/usr/bin/env bash
set -euo pipefail


{% if {{params.virtualenv}} -%}
conda activate {{params.virtualenv}}
{% endif %}

python main.py process-csv --config_json={{params.vars}}


