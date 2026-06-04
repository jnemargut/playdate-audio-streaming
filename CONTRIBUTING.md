# Contributing

Thanks for being here. This project only exists so that streaming audio on the
Playdate stops being something everyone has to solve from scratch — so improvements
that go back upstream are the whole idea. **Rough PRs and half-formed issues are
welcome**; a sample stream URL or `.mp3` and a description of what went wrong is
already a gift.

## Ways to help

- **Report bugs** — open an issue with your hardware/Simulator, the stream (or how you
  prepared the MP3), and what you heard vs. expected. Console logs help
  (`StreamAudio.debug = true`, or `livestream.statusTextC()` for live mode).
- **Send fixes or features** — seeking, gapless playback, AAC/Ogg support, an alternate
  transport, or tighter buffering would all be welcome.
- **Improve the docs** — if something was confusing to integrate, say so or fix it.

## Working on it

- The native engine is `src/streamaudio.c` (+ vendored `minimp3`). The Lua front-end for
  HTTP "episode" streaming is `lua/StreamAudio.lua`. Live (TCP) streaming is driven from C
  via the `livestream.startC` API. The audio-prep helper is `tools/prepare-audio.sh`.
- Keep it **C99 + Lua 5.4**, no dependencies beyond the Playdate C API and vendored minimp3.
- Build a host project with the SDK's `common.mk` (`make device` for hardware,
  `make simulator` for the Simulator). **Test both** — a Simulator-only build and a
  device-only build can pass independently while the packaged `.pdx` is broken.
- The companion video engine is at
  https://github.com/jnemargut/playdate-video-streaming — keep the two consistent where it
  makes sense (they share the feed/flow-control pattern).

## Conduct

Be kind and assume good faith. That's the whole policy.

## License

By contributing you agree your work is released under the project's [MIT license](LICENSE).
