#!/bin/bash

socat tcp-listen:9999,reuseaddr,fork tcp-connect:127.0.0.1:2242 &
exec "$@"