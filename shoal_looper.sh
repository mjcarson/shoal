#!/bin/bash
set -e

for i in {1..100}; do
  echo "\n\n\n\nSKIP-----$i----" >> shoal_logs_looped
  RUSTFLAGS="-C target-cpu=native" cargo run --example tmdb --release >> shoal_logs_looped
done
