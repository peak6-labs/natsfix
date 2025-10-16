#!/bin/bash
# Convert pipe-delimited FIX message to SOH-delimited format
# Usage: ./fixfmt.sh "8=FIXT.1.1|9=65|35=A|..."

if [ $# -eq 0 ]; then
    echo "Usage: $0 <pipe-delimited-fix-message>" >&2
    exit 1
fi

echo -n "$1" | tr '|' '\001'
