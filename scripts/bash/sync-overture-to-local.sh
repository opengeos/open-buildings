#!/bin/bash
mkdir -p ~/data/overture-02-15
cd ~/data/overture-02-15
aws s3 sync --no-sign-request s3://overturemaps-us-west-2/release/2024-02-15-alpha.0/ .
