#!/usr/bin/env bash
set -euo pipefail

count=$(squeue | grep $1 | wc -l)
if [ $count -eq 0 ]
then
    echo "Finished"
    exit 0
else
    echo "Still processing"
    exit 1
fi
