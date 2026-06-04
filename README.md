# Playdate Audio Streaming

**Real-time MP3 streaming for the [Playdate](https://play.date), in pure C + Lua.**

The Playdate's `fileplayer` needs an audio file **fully on disk** before it plays. This
is a small native engine that plays MP3 audio **as it arrives over the network** —
decoding and playing after a ~3 second prebuffer instead of waiting for the whole file.
It does two things:

- **Episode mode** — stream an on-demand MP3 over HTTP(S) (Lua does the fetch and feeds
  the bytes to the engine). Great for podcasts, radio plays, long-form audio.
- **Live mode** — connect straight to an Icecast/Shoutcast-style server over TCP and play
  a continuous live stream.

It was built for the daily-gift app *Read Watch Listen Play* (and grew out of the live-radio
app *Pour Over*), then pulled out here so anyone can use it. **Contributions very welcome** —
see [Contributing](#contributing).

> 🎬 **Streaming video too?** There's a companion engine with the same design:
> **[playdate-video-streaming](https://github.com/jnemargut/playdate-video-streaming)**.
> See [Using both together](#using-both-together) — a `.pdx` may export only one `eventHandler`.

---

## What you get

- **Play while downloading** — starts after a short prebuffer, not after the full file.
- **In-order, flow-controlled feeding** — bytes are fed strictly in order and only while
  the engine has room (`roomC`), so a fast or long stream can't overrun the ring or scramble
  the MP3 (which makes playback skip forward). Memory stays bounded for any length.
- **Stream + Save** — optionally write the stream to disk as it plays, then replay it offline
  with the normal `fileplayer`.
- **Handles real-world streams** — waits for headers, follows 3xx redirects, treats
  "Connection closed" as a clean end, skips ID3 tags, and has a stall watchdog.
- **Resampling + downmix** — any input rate / channel count is converted to 44.1 kHz mono.

## How it works

The engine ([`src/streamaudio.c`](src/streamaudio.c)) keeps a byte ring fed with MP3 data,
decodes it with the vendored [minimp3](https://github.com/lieff/minimp3), resamples to
44.1 kHz, and plays through a Playdate sound source. Two ways to fill the ring:

- **Episode mode:** Lua calls `playdate.network.http` (which gives you TLS), reads the
  response, and hands bytes to C via `feedC` — in order, gated on `roomC`. The
  [`lua/StreamAudio.lua`](lua/StreamAudio.lua) front-end does all of this for you.
- **Live mode:** C opens a raw TCP socket itself (`livestream.startC(url, name)`), writes the
  HTTP GET, and pumps the response body straight into the ring. No Lua HTTP involved.

## Quick start (episode mode)

### 1. Add the engine to your build

Copy `src/streamaudio.c`, `src/minimp3.h`, and `src/minimp3_impl.c` into your project and add
the `.c` files to your `SRC`. `streamaudio.c` provides the `eventHandler` the runtime needs:

```make
HEAP_SIZE  = 8388208
STACK_SIZE = 61800
SRC = src/streamaudio.c src/minimp3_impl.c
include $(SDK)/C_API/buildsupport/common.mk
```

### 2. Drop in the Lua front-end

```lua
import "StreamAudio"

StreamAudio.configure("your-host.example.com", 443, true)   -- host, port, useSSL

function playdate.update()
    if not StreamAudio.isActive() then
        StreamAudio.play("audio/show.mp3")     -- streams https://your-host/audio/show.mp3
        -- or Stream+Save:  StreamAudio.play("audio/show.mp3", "/show.mp3", onCached)
    end
    StreamAudio.tick()
end
```

That's it — audio starts once it's buffered. A fuller example is in [`example/main.lua`](example/main.lua).

### 3. Prepare your MP3

Streams must be **constant-bitrate mono MP3 with no cover-art ID3 tag** (a fat tag at the
front stalls the prebuffer). The helper does this:

```sh
./tools/prepare-audio.sh input.m4a show.mp3 64k     # CBR, mono, 44.1 kHz, tags stripped
```

## Live radio mode

For a continuous live stream, skip the Lua HTTP and let C connect over TCP:

```lua
import "StreamAudio"            -- (the native `livestream` table is global once built)

-- url is plain HTTP (raw TCP — no TLS in live mode); name is for status text
livestream.startC("http://ice.example.com:8000/stream.mp3", "My Station")

function playdate.update()
    livestream.tickC()
    -- livestream.isPlayingC(), livestream.statusTextC(), livestream.stopC() …
end
```

Live mode auto-reconnects, has a stall watchdog, and reports status via `statusTextC()`.

## Lua API (episode mode)

| Call | Purpose |
|---|---|
| `StreamAudio.configure(host, port, ssl)` | Where to stream from. Call once. |
| `StreamAudio.available()` | `true` if the native engine is present and complete. |
| `StreamAudio.play(path[, cachePath[, onCached]])` | Stream `/path`. With `cachePath`, also save; `onCached(path)` fires when done. |
| `StreamAudio.tick()` | Call every `playdate.update()`. |
| `StreamAudio.detach()` | Leave the player but finish writing the cache in the background. |
| `StreamAudio.stop()` | Tear everything down (crash-safe). |
| `isPlaying()` / `isFinished()` / `isActive()` / `isCaching()` / `failed()` | State. |
| `progress()` | `(bytesDown, bytesTotal)`. |

The native functions (`livestream.startEpisodeC`, `feedC`, `roomC`, `finalizeEpisodeC`,
`tickC`, `isPlayingC`, `isFinishedC`, `startC`, `stopC`, `statusTextC`, …) are on the global
`livestream` table if you'd rather drive the engine directly.

## Using both together

The audio and [video](https://github.com/jnemargut/playdate-video-streaming) engines each
ship an `eventHandler`, but a `.pdx` may export only **one**. To use both, keep **this**
engine's handler and add the video engine's two hooks to it — the exact lines are commented
right where they go, near the bottom of [`src/streamaudio.c`](src/streamaudio.c) (`// + video
engine: …`). Then **don't** compile the video repo's `streamvideo_entry.c`.

## Caveats & limits

- **CBR MP3 only.** The clock and pacing assume constant bitrate; VBR drifts. `prepare-audio.sh`
  forces CBR — if you roll your own, do the same.
- **Strip embedded cover art.** A big ID3 `APIC` tag at the start stalls the prebuffer. The
  engine skips ID3 tags, but a multi-hundred-KB tag delays playback badly — strip it.
- **One connection at a time on hardware.** The device dislikes concurrent HTTP connections.
  If you also stream video or run a background sync, make sure only one is live.
- **Episode mode = TLS via Lua; live mode = plain TCP.** `playdate.network.http` provides HTTPS
  for episodes. The live TCP path is unencrypted `http://` only.
- **Memory.** A 128 KB byte ring + a PCM ring live in the engine; budget `HEAP_SIZE`.
- **~3s prebuffer** before playback starts.
- **Tested on Playdate hardware + Simulator.** Other content and networks will find edges —
  please report them.

## Contributing

This exists so that streaming audio on the Playdate stops being something everyone solves from
scratch. If you improve it, send it back. Issues and PRs — even rough ones — are genuinely
welcome. Helpful directions: seeking, gapless playback, AAC/Ogg support, an alternate transport,
and bug reports with sample streams. See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Credits & license

- Built by **[jnemargut](https://github.com/jnemargut)** for *Read Watch Listen Play* / *Pour Over*.
- MP3 decoding by [minimp3](https://github.com/lieff/minimp3) (CC0 / public domain), vendored in `src/`.
- **MIT licensed** — see [`LICENSE`](LICENSE). Use it, ship it, sell your game with it; keep the notice.
