#!/bin/bash
set -e

for i in $(seq 1 $1); do
    if [ -e dummy-$i ]; then
        rm dummy-$i
    else
        touch dummy-$i
    fi
    git add .
    git commit -m"Dummy commit"
done
