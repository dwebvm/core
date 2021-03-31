#!/bin/bash
NAME=concatenator
cargo build --target wasm32-unknown-unknown --release && cp ./target/wasm32-unknown-unknown/release/$NAME.wasm ../../test/machines/
