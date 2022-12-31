#!/bin/bash
cargo build --release

./target/release/qpml mermaid examples/minimal.yaml > examples/minimal.md
./target/release/qpml dot examples/minimal.yaml > examples/minimal.dot
dot -Tpng examples/minimal.dot > examples/minimal.png

./target/release/qpml dot examples/nested-join.yaml > examples/nested-join.dot
dot -Tpng examples/nested-join.dot > examples/nested-join.png
