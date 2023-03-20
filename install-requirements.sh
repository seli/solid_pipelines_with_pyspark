#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'  # prevents many common mistakes

ls -lrt /usr/lib/jvm/
sudo python3 -m pip install -U holidays
