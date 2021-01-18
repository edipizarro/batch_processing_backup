#!/usr/bin/env bash

{{params.rm}}

export AWS_ACCESS_KEY_ID={{params.aws_access_key}}
export AWS_SECRET_ACCESS_KEY={{params.aws_secret_access_key}}

cd /home/apps/astro/alercebroker/batch_processing/computing/

{{params.command}}
