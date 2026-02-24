#!/bin/bash
# Regenerate gannt.png from gannt.mmd
# Run from any directory: bash docs/project/diagrams/render.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

mmdc \
  -i "$SCRIPT_DIR/gannt.mmd" \
  -o "$SCRIPT_DIR/gannt.png" \
  --cssFile "$SCRIPT_DIR/gantt.css" \
  --scale 4 \
  --width 2400
