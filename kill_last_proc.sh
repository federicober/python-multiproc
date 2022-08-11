#!/bin/bash
pgrep python | tee /dev/stderr | tail -1 | xargs -r kill -9