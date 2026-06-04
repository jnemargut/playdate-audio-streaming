#!/usr/bin/env bash
# prepare-audio.sh — transcode any audio into a stream-ready MP3 for the engine.
#
# The engine wants CONSTANT bitrate, mono, 44.1 kHz, and crucially NO embedded ID3
# cover-art tag (a fat APIC frame at the front of the file stalls the prebuffer — the
# stream must begin near an MP3 frame sync, not hundreds of KB of artwork). This drops
# all metadata and forces CBR.
#
#   ./prepare-audio.sh input.(mp3|wav|m4a|…) out.mp3 [bitrate]
#   ./prepare-audio.sh show.m4a show.mp3 64k        # 64k mono ~ half the size, fine for speech
#
# Requires ffmpeg on your PATH.
set -euo pipefail

IN="${1:?usage: prepare-audio.sh <input> <out.mp3> [bitrate=64k]}"
OUT="${2:?usage: prepare-audio.sh <input> <out.mp3> [bitrate=64k]}"
BR="${3:-64k}"

ffmpeg -y -v error -i "$IN" \
  -map 0:a -map_metadata -1 \
  -ar 44100 -ac 1 -b:a "$BR" \
  -write_id3v1 0 -id3v2_version 0 \
  "$OUT"

# sanity: the file should start with an MP3 frame sync (0xFF 0xFB), not "ID3"
head -c 2 "$OUT" | xxd | grep -qi "fffb" \
  && echo "ok: $OUT starts at a frame sync (stream-ready)" \
  || echo "warn: $OUT does not start with 0xFFFB — check for a leftover tag"
