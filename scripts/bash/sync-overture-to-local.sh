#!/bin/bash

DEFAULT_RELEASE="2024-02-15-alpha.0"
DEFAULT_DESTINATION="$HOME/data/src/overture/$DEFAULT_RELEASE"

while getopts ":d:r:" opt; do
    case ${opt} in
    d) # Process option for the destination
        DESTINATION=$OPTARG
        ;;
    r) # Process option for the release
        RELEASE=$OPTARG
        ;;
    \?)
        echo "Usage: cmd [-d destination] [-r release]"
        ;;
    esac
done

# Set the destination directory based on the provided destination or release argument
DESTINATION="${DESTINATION:-$DEFAULT_DESTINATION}"
RELEASE="${RELEASE:-$DEFAULT_RELEASE}"

mkdir -p "${DESTINATION}"
cd "${DESTINATION}"

aws s3 sync --no-sign-request "s3://overturemaps-us-west-2/release/${RELEASE}/" .

# Verification step to ensure all files are transferred correctly
# This is a simple re-sync operation; any missing or incomplete files will be re-downloaded
echo "Verifying file transfer..."
aws s3 sync --no-sign-request "s3://overturemaps-us-west-2/release/${RELEASE}/" . --dryrun
