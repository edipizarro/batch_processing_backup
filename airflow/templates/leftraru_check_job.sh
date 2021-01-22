#!/usr/bin/env bash
set -euo pipefail

cd /home/apps/astro/alercebroker/batch_processing/computing/scripts

bash check_finished.sh {% if xcom -%} {{xcom.strip().decode()}} {% endif %}
