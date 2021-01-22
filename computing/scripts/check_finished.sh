#!/usr/bin/env bash
set -euo pipefail

count=$(squeue | grep $1 | wc -l)
if [ $count -eq 0 ]
then
    exit 0
else
    exit 1
fi
