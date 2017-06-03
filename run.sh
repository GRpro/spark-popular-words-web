#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

rm -rf "$DIR/data/output"

docker run -v "$DIR/data":/opt -e INPUT_URI='/opt/input.txt' -e OUTPUT_URI='/opt/output' -it spark-popular-words-web