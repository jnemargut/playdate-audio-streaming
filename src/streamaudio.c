// livestream_c.c — native MP3 streaming engine for Pour Over's Live mode.
//
// Adapted from fredley/minimp3-pd's example app. Major differences:
//   - Not a standalone app. We register Lua-callable functions instead
//     of setUpdateCallback, since Pour Over already owns the main loop
//     from Lua.
//   - URL is supplied at runtime via Lua (startC), not hardcoded.
//   - All on-screen logging is dropped; we route diagnostics through
//     pd->system->logToConsole only.
//   - Failure budget eventually transitions to a sticky "off air" state
//     so the Lua side can surface a retry prompt to the user.
//
// The audio path is unchanged from the example:
//   HTTP -> byte ring -> minimp3 decode -> mono PCM ring -> audio callback
//
// Lua-callable surface (registered as livestream.startC, etc.):
//   startC(url, name)    string, string         begin streaming
//   stopC()              -                       tear down cleanly
//   tickC()              -                       called once per frame
//                                                from playdate.update()
//   isPlayingC()         -> bool                 audio is flowing
//   isOffAirC()          -> bool                 sticky failure state
//   statusTextC()        -> string               UI status line
//   bytesTextC()         -> string               loading meter text

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <stdatomic.h>

#include "pd_api.h"
#include "minimp3.h"

// ──────────────────────────────────────────────────────────────────────
// Tunables
// ──────────────────────────────────────────────────────────────────────

#define STREAM_HTTP_READ_BUFFER 262144   // SDK-side HTTP buffer

#define RING_SIZE 131072                 // 128 KB ≈ 8s @ 128 kbps
#define RING_MASK (RING_SIZE - 1)

#define MP3_SCRATCH 4096                 // > one MP3 frame + ID3 slack

#define PCM_FRAMES 16384                 // ≈ 372 ms @ 44.1 kHz (mono)
#define PCM_MASK   (PCM_FRAMES - 1)

#define OUTPUT_HZ 44100                  // Playdate output rate, fixed
#define MAX_RESAMPLED_PER_FRAME 8192

#define PREBUFFER_BYTES         49152    // ≈ 3s of audio before play
#define PREBUFFER_TIMEOUT_MS    10000
#define STALL_TIMEOUT_MS        8000
#define RECONNECT_DELAY_MS      2000
#define MAX_CONSECUTIVE_FAILS   5        // before going sticky off-air

#define STATUS_LEN 96

// ──────────────────────────────────────────────────────────────────────
// SDK handles
// ──────────────────────────────────────────────────────────────────────
//
// We use the SDK's TCP API (not HTTP). The HTTP API is download-oriented:
// it buffers the entire response and only surfaces bytes when the request
// completes. For an Icecast/Shoutcast audio stream the request never
// completes (infinite body), so HTTP would give us nothing. TCP gives us
// per-byte access — we write a raw HTTP/1.1 GET ourselves and read body
// bytes as they arrive.

static PlaydateAPI*              pd  = NULL;
static const struct playdate_tcp* tcp = NULL;

// ──────────────────────────────────────────────────────────────────────
// Engine state
// ──────────────────────────────────────────────────────────────────────

typedef enum {
    kIdle,                // not playing, no connection
    kRequestingAccess,
    kWaitingForAccess,
    kStartingStream,
    kConnecting,          // tcp->open issued, waiting on onTcpOpen
    kSendingRequest,      // socket connected, about to write the GET
    kPrebuffering,
    kPlaying,
    kReconnectWait,
    kOffAir,              // sticky after MAX_CONSECUTIVE_FAILS
} State;

static State state = kIdle;

// Engine mode: live = TCP-fed, infinite stream; episode = Lua-fed,
// finite stream (Lua does the HTTPS download elsewhere and pushes bytes
// in via livestream.feedC).
static int is_episode        = 0;
static int episode_finalized = 0;  // Lua said "no more bytes coming"
static int episode_completed = 0;  // the whole episode played out to the end

// ID3v2 tag skipping (episode mode only). Podcast MP3s commonly start
// with a 10-byte ID3v2 header followed by a multi-KB metadata blob
// (cover art, tags) — far more than our 49 KB prebuffer can hold while
// also looking for MP3 sync. We strip the tag in feedC so it never
// reaches the audio ring buffer.
static uint8_t  id3_check_buf[10];
static unsigned id3_check_pos       = 0;
static int      id3_check_done      = 0;
static unsigned id3_skip_remaining  = 0;

static TCPConnection* stream_conn = NULL;
static SoundSource*   mp3_source  = NULL;

// HTTP response header parsing state. Bytes from TCP first go into
// header_buf until we find the "\r\n\r\n" terminator; then we flip to
// body mode and bytes go straight into the audio ring buffer.
#define HEADER_BUF_SIZE 2048
static char     header_buf[HEADER_BUF_SIZE];
static unsigned header_pos    = 0;
static int      headers_done  = 0;
static int      header_status = 0;

static mp3dec_t mp3dec;
static uint8_t  mp3_scratch[MP3_SCRATCH];
static int16_t  decode_buf[MINIMP3_MAX_SAMPLES_PER_FRAME];

// Mono PCM ring. Producer = main thread (decode). Consumer = audio thread.
// Mono because addCallbackSource crashes on hardware when stereo.
static int16_t     pcm[PCM_FRAMES];
static atomic_uint pcm_write = 0;
static atomic_uint pcm_read  = 0;

// Linear-interp resampler state (Q16.16 fixed point).
static int     rs_in_rate  = 0;
static int32_t rs_step_q16 = 0;
static int32_t rs_pos_q16  = 0;
static int16_t rs_prev     = 0;

// Byte ring between HTTP pump (main thread) and decoder (main thread).
static uint8_t     ring[RING_SIZE];
static atomic_uint ring_write = 0;
static atomic_uint ring_read  = 0;

// Parsed URL.
static char stream_host[128] = {0};
static int  stream_port      = 80;
static char stream_path[256] = {0};

// User-supplied station label (for status messages).
static char station_label[64] = {0};

// Last requested URL — preserved so reconnect can re-parse.
static char last_url[300] = {0};

// State machine timestamps.
static unsigned prebuffer_start_ms = 0;
static unsigned reconnect_at_ms    = 0;

// Failure counter for the sticky off-air transition.
static int consecutive_fails = 0;

// User-visible status string, set by state transitions and read by Lua.
static char status_text[STATUS_LEN] = "";

// Total bytes received since start (for the "12 KB" loading meter line).
static unsigned bytes_received_total = 0;

// Deferred-action plumbing — same pattern as the example: callbacks can't
// safely tear down a connection from inside that connection's own callback,
// so they set a pending action and the next tick performs the work.
typedef enum { kPendingNone, kPendingRetry } PendingAction;
static PendingAction pending = kPendingNone;

// ──────────────────────────────────────────────────────────────────────
// Forward decls
// ──────────────────────────────────────────────────────────────────────

static void ring_reset(void);
static void pcm_reset(void);
static int  parseHttpURL(const char* url, char* host, size_t hostlen,
                         int* port, char* path, size_t pathlen);
static void teardownPlayback(void);
static void requestRetry(const char* reason);
static int  startStreamPlayer(void);

// ──────────────────────────────────────────────────────────────────────
// Logging helpers
// ──────────────────────────────────────────────────────────────────────

static void ls_log(const char* fmt, ...)
{
    if (!pd) return;
    char buf[160];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    pd->system->logToConsole("livestream: %s", buf);
}

static void setStatus(const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(status_text, sizeof(status_text), fmt, ap);
    va_end(ap);
}

// ──────────────────────────────────────────────────────────────────────
// Byte ring
// ──────────────────────────────────────────────────────────────────────

static inline unsigned ring_used(void) {
    unsigned w = atomic_load_explicit(&ring_write, memory_order_acquire);
    unsigned r = atomic_load_explicit(&ring_read,  memory_order_relaxed);
    return (w - r) & RING_MASK;
}

static inline unsigned ring_free(void) {
    return RING_SIZE - 1 - ring_used();
}

static void ring_reset(void) {
    atomic_store_explicit(&ring_write, 0, memory_order_relaxed);
    atomic_store_explicit(&ring_read,  0, memory_order_relaxed);
}

// Reject reserved bitrate/samplerate/layer/version codes — keeps sync
// from latching onto random 0xFF bytes inside the audio body.
static int looksLikeFrameHeader(unsigned pos)
{
    unsigned p1 = pos       & RING_MASK;
    unsigned p2 = (pos + 1) & RING_MASK;
    unsigned p3 = (pos + 2) & RING_MASK;
    uint8_t b1 = ring[p1];
    uint8_t b2 = ring[p2];
    uint8_t b3 = ring[p3];
    if (b1 != 0xFF) return 0;
    if ((b2 & 0xE0) != 0xE0) return 0;
    if ((b2 & 0x18) == 0x08) return 0;
    if ((b2 & 0x06) == 0x00) return 0;
    if ((b3 & 0xF0) == 0xF0) return 0;
    if ((b3 & 0x0C) == 0x0C) return 0;
    return 1;
}

static int frameLengthAt(unsigned pos)
{
    uint8_t b2 = ring[(pos + 1) & RING_MASK];
    uint8_t b3 = ring[(pos + 2) & RING_MASK];
    int ver   = (b2 >> 3) & 0x03;
    int layer = (b2 >> 1) & 0x03;
    if (ver == 1 || layer != 1) return 0;

    static const int br_mpeg1[16] = {0,32,40,48,56,64,80,96,112,128,160,192,224,256,320,0};
    static const int br_mpeg2[16] = {0, 8,16,24,32,40,48,56, 64, 80, 96,112,128,144,160,0};
    static const int sr_mpeg1 [4] = {44100, 48000, 32000, 0};
    static const int sr_mpeg2 [4] = {22050, 24000, 16000, 0};
    static const int sr_mpeg25[4] = {11025, 12000,  8000, 0};

    int br = (ver == 3) ? br_mpeg1[(b3 >> 4) & 0x0F]
                        : br_mpeg2[(b3 >> 4) & 0x0F];
    int sr = (ver == 3) ? sr_mpeg1 [(b3 >> 2) & 0x03]
           : (ver == 2) ? sr_mpeg2 [(b3 >> 2) & 0x03]
                        : sr_mpeg25[(b3 >> 2) & 0x03];
    if (br == 0 || sr == 0) return 0;
    int padding = (b3 >> 1) & 0x01;
    int coeff = (ver == 3) ? 144 : 72;
    return (coeff * br * 1000) / sr + padding;
}

// Skip any preamble (ID3 tags, HTML error bodies, etc.) before the first
// pair of consecutive valid Layer III frame headers.
static int alignRingToFrameSync(void)
{
    unsigned r = atomic_load_explicit(&ring_read,  memory_order_relaxed);
    unsigned w = atomic_load_explicit(&ring_write, memory_order_acquire);
    unsigned avail = (w - r) & RING_MASK;
    if (avail < 4) return 0;

    for (unsigned i = 0; i + 3 < avail; i++) {
        if (!looksLikeFrameHeader(r + i)) continue;
        int flen = frameLengthAt(r + i);
        if (flen <= 0) continue;
        if (i + (unsigned)flen + 2 >= avail) continue;
        if (!looksLikeFrameHeader(r + i + flen)) continue;
        if (i > 0) {
            ls_log("aligned ring: skipped %u preamble bytes", i);
            atomic_store_explicit(&ring_read, (r + i) & RING_MASK,
                                  memory_order_release);
        }
        return 1;
    }
    ls_log("no MP3 sync in %u-byte prebuffer", avail);
    return 0;
}

// Push body bytes into the ring buffer. Returns 0 on retry-trigger.
static int pushBodyBytes(const uint8_t* src, unsigned len)
{
    if (len == 0) return 1;
    unsigned writable = ring_free();
    if (len > writable) len = writable;
    unsigned w = atomic_load_explicit(&ring_write, memory_order_relaxed);
    for (unsigned k = 0; k < len; k++) {
        ring[(w + k) & RING_MASK] = src[k];
    }
    atomic_store_explicit(&ring_write, (w + len) & RING_MASK,
                          memory_order_release);
    bytes_received_total += len;
    return 1;
}

// Pull bytes from the TCP socket. In header mode, accumulates into
// header_buf until "\r\n\r\n", parses the status line, then puts any
// post-header tail bytes into the ring and flips to body mode. In body
// mode, reads directly into the ring.
static void pumpTCPIntoRing(void)
{
    if (!stream_conn) return;

    // Headers first.
    if (!headers_done) {
        while (1) {
            size_t avail = tcp->getBytesAvailable(stream_conn);
            if (avail == 0) break;

            unsigned room = HEADER_BUF_SIZE - header_pos;
            if (room == 0) {
                requestRetry("headers too large");
                return;
            }
            unsigned chunk = avail < room ? (unsigned)avail : room;
            int got = tcp->read(stream_conn, header_buf + header_pos, chunk);
            if (got <= 0) break;
            unsigned old_pos = header_pos;
            header_pos += (unsigned)got;

            // Scan for "\r\n\r\n" — start a few bytes back to cover the
            // case where the terminator straddles this read and the prior.
            unsigned scan_start = old_pos >= 3 ? old_pos - 3 : 0;
            for (unsigned i = scan_start + 3; i < header_pos; i++) {
                if (header_buf[i-3] == '\r' && header_buf[i-2] == '\n' &&
                    header_buf[i-1] == '\r' && header_buf[i  ] == '\n') {
                    // Parse status line: "HTTP/1.x SSS Reason\r\n".
                    const char* sp = (const char*)memchr(header_buf, ' ',
                                                          header_pos);
                    if (sp) header_status = atoi(sp + 1);
                    ls_log("headers done: status=%d (consumed %u bytes)",
                           header_status, i + 1);

                    if (header_status != 200) {
                        char why[48];
                        snprintf(why, sizeof(why), "status %d",
                                 header_status);
                        requestRetry(why);
                        return;
                    }

                    // Tail bytes after the terminator go into the ring.
                    unsigned body_start = i + 1;
                    unsigned tail = header_pos - body_start;
                    if (tail > 0) {
                        pushBodyBytes((uint8_t*)header_buf + body_start,
                                      tail);
                    }
                    headers_done = 1;
                    break;
                }
            }
            if (headers_done) break;
        }
    }

    // Body mode — read straight from socket into the ring.
    if (headers_done) {
        while (1) {
            size_t avail = tcp->getBytesAvailable(stream_conn);
            if (avail == 0) break;
            unsigned writable = ring_free();
            if (writable == 0) break;
            unsigned w = atomic_load_explicit(&ring_write,
                                              memory_order_relaxed);
            unsigned contiguous = RING_SIZE - w;
            unsigned chunk = writable < contiguous ? writable : contiguous;
            if ((size_t)chunk > avail) chunk = (unsigned)avail;
            int got = tcp->read(stream_conn, ring + w, chunk);
            if (got <= 0) break;
            atomic_store_explicit(&ring_write, (w + got) & RING_MASK,
                                  memory_order_release);
            bytes_received_total += (unsigned)got;
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// PCM ring + decoder
// ──────────────────────────────────────────────────────────────────────

static unsigned ring_peek_contig(uint8_t* dst, unsigned want)
{
    unsigned r = atomic_load_explicit(&ring_read,  memory_order_relaxed);
    unsigned w = atomic_load_explicit(&ring_write, memory_order_acquire);
    unsigned avail = (w - r) & RING_MASK;
    unsigned n = want < avail ? want : avail;
    unsigned first = RING_SIZE - r;
    if (first >= n) {
        memcpy(dst, ring + r, n);
    } else {
        memcpy(dst, ring + r, first);
        memcpy(dst + first, ring, n - first);
    }
    return n;
}

static void ring_consume(unsigned n)
{
    unsigned r = atomic_load_explicit(&ring_read, memory_order_relaxed);
    atomic_store_explicit(&ring_read, (r + n) & RING_MASK, memory_order_release);
}

static inline unsigned pcm_used(void) {
    unsigned w = atomic_load_explicit(&pcm_write, memory_order_relaxed);
    unsigned r = atomic_load_explicit(&pcm_read,  memory_order_acquire);
    return (w - r) & PCM_MASK;
}

static inline unsigned pcm_free(void) {
    return PCM_FRAMES - 1 - pcm_used();
}

static void pcm_reset(void)
{
    atomic_store_explicit(&pcm_write, 0, memory_order_relaxed);
    atomic_store_explicit(&pcm_read,  0, memory_order_relaxed);
}

static void rs_reset(void)
{
    rs_in_rate  = 0;
    rs_step_q16 = 0;
    rs_pos_q16  = 0;
    rs_prev     = 0;
}

// Push one decoded frame's samples (downmixed to mono, resampled to 44.1
// kHz) into the PCM ring.
static void pcm_write_frame(const int16_t* src, int samples, int channels, int hz)
{
    if (hz != rs_in_rate) {
        ls_log("decoder: input=%d Hz, %d ch -> %d Hz", hz, channels, OUTPUT_HZ);
        rs_in_rate  = hz;
        rs_step_q16 = (int32_t)(((int64_t)hz << 16) / OUTPUT_HZ);
    }

    static int16_t mono[MINIMP3_MAX_SAMPLES_PER_FRAME / 2];
    if (channels == 2) {
        for (int i = 0; i < samples; i++) {
            int32_t l = src[i * 2];
            int32_t r = src[i * 2 + 1];
            mono[i] = (int16_t)((l + r) >> 1);
        }
    } else {
        for (int i = 0; i < samples; i++) mono[i] = src[i];
    }

    int32_t pos  = rs_pos_q16;
    int32_t step = rs_step_q16;
    int32_t end  = (int32_t)samples << 16;

    unsigned w = atomic_load_explicit(&pcm_write, memory_order_relaxed);
    int written = 0;
    while (pos < end) {
        int idx  = pos >> 16;
        int frac = pos & 0xFFFF;
        int16_t a = (idx == 0) ? rs_prev : mono[idx - 1];
        int16_t b = mono[idx];
        int32_t s = a + (int32_t)(((int64_t)(b - a) * frac) >> 16);
        pcm[(w + written) & PCM_MASK] = (int16_t)s;
        written++;
        pos += step;
    }
    atomic_store_explicit(&pcm_write, (w + written) & PCM_MASK,
                          memory_order_release);

    rs_prev    = mono[samples - 1];
    rs_pos_q16 = pos - end;
}

static void decodeIntoPCMRing(int max_frames)
{
    for (int n = 0; n < max_frames; n++) {
        if (pcm_free() < MAX_RESAMPLED_PER_FRAME) break;

        unsigned got = ring_peek_contig(mp3_scratch, MP3_SCRATCH);
        if (got < 4) break;

        mp3dec_frame_info_t info = {0};
        int samples = mp3dec_decode_frame(&mp3dec, mp3_scratch, (int)got,
                                          decode_buf, &info);
        if (info.frame_bytes > 0) {
            ring_consume((unsigned)info.frame_bytes);
        } else {
            break;
        }
        if (samples > 0) {
            pcm_write_frame(decode_buf, samples, info.channels, info.hz);
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// Audio callback (audio thread)
// ──────────────────────────────────────────────────────────────────────

static int audioCallback(void* ctx, int16_t* left, int16_t* right, int len)
{
    (void)ctx;
    (void)right;

    unsigned r = atomic_load_explicit(&pcm_read,  memory_order_relaxed);
    unsigned w = atomic_load_explicit(&pcm_write, memory_order_acquire);
    unsigned avail = (w - r) & PCM_MASK;
    unsigned want  = (unsigned)len < avail ? (unsigned)len : avail;

    unsigned first = PCM_FRAMES - r;
    if (first > want) first = want;
    memcpy(left, pcm + r, first * sizeof(int16_t));
    if (first < want) {
        memcpy(left + first, pcm, (want - first) * sizeof(int16_t));
    }
    for (unsigned i = want; i < (unsigned)len; i++) left[i] = 0;

    atomic_store_explicit(&pcm_read, (r + want) & PCM_MASK,
                          memory_order_release);
    return 1;
}

// ──────────────────────────────────────────────────────────────────────
// URL parsing  (http:// only — Decision 27)
// ──────────────────────────────────────────────────────────────────────

static int parseHttpURL(const char* url, char* host, size_t hostlen,
                        int* port, char* path, size_t pathlen)
{
    static const char prefix[] = "http://";
    if (strncmp(url, prefix, sizeof(prefix) - 1) != 0) return 0;
    const char* p = url + sizeof(prefix) - 1;

    const char* host_end = p;
    while (*host_end && *host_end != ':' && *host_end != '/') host_end++;
    size_t hl = host_end - p;
    if (hl == 0 || hl >= hostlen) return 0;
    memcpy(host, p, hl);
    host[hl] = 0;

    *port = 80;
    if (*host_end == ':') {
        host_end++;
        char* end;
        long parsed = strtol(host_end, &end, 10);
        if (end == host_end || parsed <= 0 || parsed > 65535) return 0;
        *port = (int)parsed;
        host_end = end;
    }

    if (*host_end == 0) {
        if (pathlen < 2) return 0;
        path[0] = '/'; path[1] = 0;
    } else {
        size_t pl = strlen(host_end);
        if (pl >= pathlen) return 0;
        memcpy(path, host_end, pl + 1);
    }
    return 1;
}

// ──────────────────────────────────────────────────────────────────────
// Lifecycle
// ──────────────────────────────────────────────────────────────────────

static void requestRetry(const char* reason)
{
    ls_log("retry: %s", reason);
    // Episode mode is Lua-driven (HTTPS download done in Lua); the C
    // engine has no idea how to retry a Lua download, so we just stop.
    // Lua decides what to do (re-attempt, surface an error to the UI,
    // fall back to cache, etc.).
    if (is_episode) {
        state = kIdle;
        return;
    }
    pending = kPendingRetry;
}

static void teardownPlayback(void)
{
    if (mp3_source) {
        pd->sound->removeSource(mp3_source);
        mp3_source = NULL;
    }
    if (stream_conn) {
        // Close but do NOT release. The simulator's network layer
        // queues callbacks on the runloop; releasing while those are
        // still in flight crashes the simulator. We accept the small
        // per-retry leak (the connection structure stays alive until
        // the app exits) in exchange for stable retries.
        tcp->close(stream_conn);
        stream_conn = NULL;
    }
    headers_done      = 0;
    header_pos        = 0;
    header_status     = 0;
    is_episode        = 0;
    episode_finalized = 0;
    episode_completed = 0;
    pcm_reset();
    ring_reset();
}

static void onAccessReply(bool allowed, void* userdata)
{
    (void)userdata;
    ls_log("access reply: allowed=%d", allowed);
    if (allowed) {
        state = kStartingStream;
        setStatus("Tuning in: %s", station_label);
    } else {
        state = kOffAir;
        setStatus("Network access denied");
    }
}

// TCP connection closed (by server, or by error). If we're still
// expecting data, this counts as a retry trigger.
static void onTcpConnectionClosed(TCPConnection* c, PDNetErr err)
{
    (void)c;
    ls_log("tcp closed (err=%d)", err);
    if (state == kPrebuffering || state == kPlaying) {
        requestRetry("connection closed");
    }
}

// Fires when tcp->open() finishes. Write the GET request straight
// from here — the SDK's own TCP example does this. tcp->write returns
// the number of bytes "synchronously" sent which can be 0 even on a
// successful write (the bytes get queued); we don't trust that return
// value and proceed to header-reading anyway.
static void onTcpOpen(TCPConnection* c, PDNetErr err, void* ud)
{
    (void)ud;
    if (err != NET_OK || !c) {
        char why[48];
        snprintf(why, sizeof(why), "tcp open err=%d", err);
        requestRetry(why);
        return;
    }
    ls_log("tcp open ok");

    // Send HTTP/1.0 not 1.1: HTTP/1.0 doesn't support chunked
    // transfer-encoding, so the server is guaranteed to send a flat
    // byte stream. With 1.1 some Icecast servers chunk the response
    // and we'd need to strip the per-chunk length prefixes (which
    // currently get fed to minimp3 as garbage, producing a small
    // glitch every chunk boundary, ~every 2 seconds).
    char req[512];
    int n = snprintf(req, sizeof(req),
        "GET %s HTTP/1.0\r\n"
        "Host: %s\r\n"
        "User-Agent: PourOver/0.9 (Playdate)\r\n"
        "Accept: audio/mpeg,*/*\r\n"
        "\r\n",
        stream_path, stream_host);
    int got = tcp->write(c, req, n);
    ls_log("tcp write returned %d (want %d, ignoring if <n)", got, n);

    headers_done       = 0;
    header_pos         = 0;
    header_status      = 0;
    prebuffer_start_ms = pd->system->getCurrentTimeMilliseconds();
    if (state == kConnecting) state = kPrebuffering;
}

static void startStreamRequest(void)
{
    bytes_received_total = 0;
    headers_done         = 0;
    header_pos           = 0;
    header_status        = 0;
    ls_log("connecting %s:%d", stream_host, stream_port);
    setStatus("Tuning in: %s", station_label);

    stream_conn = tcp->newConnection(stream_host, stream_port, false);
    if (!stream_conn) {
        requestRetry("newConnection failed");
        return;
    }

    tcp->setReadBufferSize(stream_conn, 16384);
    tcp->setReadTimeout(stream_conn, 15000);
    tcp->setConnectTimeout(stream_conn, 10000);
    tcp->setConnectionClosedCallback(stream_conn, onTcpConnectionClosed);

    {
        WifiStatus ws = pd->network->getStatus();
        ls_log("network status pre-open: wifi=%d (0=NotConn,1=Conn,2=NotAvail)", ws);
    }
    PDNetErr err = tcp->open(stream_conn, onTcpOpen, NULL);
    ls_log("tcp->open returned err=%d", err);
    if (err != NET_OK) {
        char why[48];
        snprintf(why, sizeof(why), "tcp->open err=%d", err);
        requestRetry(why);
        return;
    }

    // Wait in kConnecting until onTcpOpen advances state. The
    // prebuffer_start_ms watchdog still bounds the wait — if onTcpOpen
    // never fires (server unreachable) prebuffer-timeout retries us.
    prebuffer_start_ms = pd->system->getCurrentTimeMilliseconds();
    state = kConnecting;
}

// Write the HTTP/1.1 GET request to a connected socket. Returns 1 on
// success, 0 if we should retry. The simulator's tcp->write sometimes
// returns 0 transiently when called too soon after connect — we loop a
// few times to handle that without giving up.
static int sendHTTPRequest(void)
{
    if (!stream_conn) return 0;

    char req[512];
    int n = snprintf(req, sizeof(req),
        "GET %s HTTP/1.1\r\n"
        "Host: %s\r\n"
        "User-Agent: PourOver/0.9 (Playdate)\r\n"
        "Connection: close\r\n"
        "Accept: audio/mpeg,*/*\r\n"
        "\r\n",
        stream_path, stream_host);

    int sent = 0;
    for (int attempt = 0; attempt < 8 && sent < n; attempt++) {
        int got = tcp->write(stream_conn, req + sent, n - sent);
        if (got < 0) {
            ls_log("tcp write err=%d (sent=%d/%d)", got, sent, n);
            return 0;
        }
        if (got > 0) {
            sent += got;
        }
    }
    if (sent != n) {
        ls_log("tcp write incomplete: sent=%d want=%d", sent, n);
        return 0;
    }
    ls_log("wrote GET (%d bytes)", n);
    return 1;
}

static int startStreamPlayer(void)
{
    mp3dec_init(&mp3dec);
    pcm_reset();
    rs_reset();
    mp3_source = pd->sound->channel->addCallbackSource(
        pd->sound->getDefaultChannel(), audioCallback, NULL, /*stereo*/ 0);
    return mp3_source != NULL;
}

// ──────────────────────────────────────────────────────────────────────
// Per-tick state machine — driven from Lua's update loop
// ──────────────────────────────────────────────────────────────────────

static void tickOnce(void)
{
    // Deferred-action plumbing: a callback that needed to retry can't
    // close the connection from inside its own callback chain, so it
    // sets pending=kPendingRetry and we do the cleanup here.
    if (pending == kPendingRetry) {
        pending = kPendingNone;
        teardownPlayback();
        consecutive_fails++;
        if (consecutive_fails >= MAX_CONSECUTIVE_FAILS) {
            state = kOffAir;
            setStatus("Off air. Press A to try again.");
            return;
        }
        reconnect_at_ms = pd->system->getCurrentTimeMilliseconds()
                        + RECONNECT_DELAY_MS;
        state = kReconnectWait;
        setStatus("Reconnecting (%d/%d)...",
                  consecutive_fails, MAX_CONSECUTIVE_FAILS);
    }

    switch (state) {
        case kIdle:
        case kOffAir:
            return;

        case kRequestingAccess: {
            enum accessReply r = tcp->requestAccess(
                stream_host, stream_port, false,
                "stream NPR live audio", onAccessReply, NULL);
            if      (r == kAccessAllow) state = kStartingStream;
            else if (r == kAccessDeny)  { state = kOffAir; setStatus("Network denied"); }
            else                        state = kWaitingForAccess;
            break;
        }

        case kWaitingForAccess:
            // onAccessReply transitions state.
            break;

        case kStartingStream:
            startStreamRequest();
            break;

        case kConnecting: {
            // Waiting on onTcpOpen. If the prebuffer watchdog elapses
            // without a callback, treat that as a failed connect.
            unsigned now = pd->system->getCurrentTimeMilliseconds();
            if (now - prebuffer_start_ms > PREBUFFER_TIMEOUT_MS) {
                requestRetry("tcp connect timeout");
            }
            break;
        }

        case kSendingRequest: {
            if (!sendHTTPRequest()) {
                requestRetry("tcp write failed");
                break;
            }
            headers_done       = 0;
            header_pos         = 0;
            header_status      = 0;
            prebuffer_start_ms = pd->system->getCurrentTimeMilliseconds();
            state              = kPrebuffering;
            break;
        }

        case kPrebuffering: {
            pumpTCPIntoRing();
            unsigned used = ring_used();
            // Heartbeat: every ~2s during prebuffer, log connection state.
            {
                static unsigned last_heartbeat_ms = 0;
                unsigned now = pd->system->getCurrentTimeMilliseconds();
                if (now - last_heartbeat_ms > 2000 && stream_conn) {
                    last_heartbeat_ms = now;
                    PDNetErr err = tcp->getError(stream_conn);
                    size_t   avail = tcp->getBytesAvailable(stream_conn);
                    ls_log("prebuf hb: used=%u hdrs=%d hpos=%u err=%d avail=%zu",
                           used, headers_done, header_pos, err, avail);
                }
            }
            if (used >= PREBUFFER_BYTES) {
                if (!alignRingToFrameSync()) {
                    requestRetry("no MP3 frame sync");
                    break;
                }
                ls_log("prebuffered %u bytes, playing", used);
                if (!startStreamPlayer()) {
                    requestRetry("addCallbackSource failed");
                    break;
                }
                consecutive_fails = 0;
                state = kPlaying;
                setStatus("Live: %s", station_label);
            } else if (!is_episode) {
                // Live mode: prebuffer should fill quickly from a live
                // stream; if it doesn't in 10s, the connection is broken.
                // Episode mode is Lua-fed and can legitimately take
                // longer (TLS handshake + slow link); we let Lua decide
                // when to give up there.
                unsigned now = pd->system->getCurrentTimeMilliseconds();
                if (now - prebuffer_start_ms > PREBUFFER_TIMEOUT_MS) {
                    char why[48];
                    snprintf(why, sizeof(why), "prebuffer timeout (%u B)", used);
                    requestRetry(why);
                }
            }
            break;
        }

        case kPlaying: {
            pumpTCPIntoRing();
            decodeIntoPCMRing(16);

            // Episode end-of-stream: Lua signaled "no more bytes" AND
            // the byte ring is drained AND the PCM ring's write cursor
            // has caught up to the read cursor. At that point the audio
            // thread has played everything we have, and the episode is
            // genuinely over.
            if (is_episode && episode_finalized) {
                unsigned pr = atomic_load_explicit(&pcm_read,
                                                   memory_order_acquire);
                unsigned pw = atomic_load_explicit(&pcm_write,
                                                   memory_order_acquire);
                if (ring_used() == 0 && pr == pw) {
                    ls_log("episode: end-of-stream, draining done");
                    episode_completed = 1;   // played all the way through
                    teardownPlayback();
                    state = kIdle;
                    break;
                }
            }

            // Watchdog over the audio thread's read cursor. Disabled in
            // episode mode — a paused-by-network gap is normal there and
            // doesn't mean the connection failed (Lua manages that).
            if (!is_episode) {
                static unsigned last_pcm_read_seen  = 0;
                static unsigned last_pcm_advance_ms = 0;
                unsigned now = pd->system->getCurrentTimeMilliseconds();
                unsigned r   = atomic_load_explicit(&pcm_read,
                                                    memory_order_acquire);
                if (r != last_pcm_read_seen) {
                    last_pcm_read_seen  = r;
                    last_pcm_advance_ms = now;
                } else if (last_pcm_advance_ms == 0) {
                    last_pcm_advance_ms = now;
                }

                if (now - last_pcm_advance_ms > STALL_TIMEOUT_MS) {
                    ls_log("audio stalled %u ms", now - last_pcm_advance_ms);
                    last_pcm_advance_ms = 0;
                    last_pcm_read_seen  = 0;
                    requestRetry("stalled");
                }
            }
            break;
        }

        case kReconnectWait: {
            unsigned now = pd->system->getCurrentTimeMilliseconds();
            if ((int)(reconnect_at_ms - now) <= 0) {
                state = kStartingStream;
            }
            break;
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// Lua-callable surface
// ──────────────────────────────────────────────────────────────────────

static int l_start(lua_State* L)
{
    (void)L;
    const char* url  = pd->lua->getArgString(1);
    const char* name = pd->lua->getArgString(2);
    if (!url || !*url) {
        pd->lua->pushBool(0);
        return 1;
    }

    // Teardown any prior session cleanly before starting a new one.
    teardownPlayback();
    pending           = kPendingNone;
    consecutive_fails = 0;

    if (!parseHttpURL(url, stream_host, sizeof(stream_host), &stream_port,
                      stream_path, sizeof(stream_path))) {
        ls_log("bad URL: %s", url);
        state = kOffAir;
        setStatus("Bad URL");
        pd->lua->pushBool(0);
        return 1;
    }

    strncpy(last_url,      url,           sizeof(last_url)      - 1);
    strncpy(station_label, name ? name : "Live", sizeof(station_label) - 1);
    last_url[sizeof(last_url)           - 1] = 0;
    station_label[sizeof(station_label) - 1] = 0;

    ls_log("starting %s (%s)", station_label, last_url);
    setStatus("Tuning in: %s", station_label);
    state = kRequestingAccess;
    pd->lua->pushBool(1);
    return 1;
}

static int l_stop(lua_State* L)
{
    (void)L;
    ls_log("stop");
    teardownPlayback();
    pending           = kPendingNone;
    state             = kIdle;
    consecutive_fails = 0;
    status_text[0]    = 0;
    bytes_received_total = 0;
    return 0;
}

// ── Episode mode: Lua does the HTTPS download (TLS + redirects) and
// pushes raw MP3 bytes in. The C engine just decodes and plays. No TCP
// involvement; the existing kPrebuffering → kPlaying path drives audio.

static int l_startEpisode(lua_State* L)
{
    (void)L;
    const char* name = pd->lua->getArgString(1);

    teardownPlayback();
    pending              = kPendingNone;
    consecutive_fails    = 0;
    bytes_received_total = 0;
    headers_done         = 1;   // Lua already stripped HTTP headers.
    header_pos           = 0;
    header_status        = 200;
    is_episode           = 1;
    episode_finalized    = 0;
    episode_completed    = 0;
    id3_check_pos        = 0;
    id3_check_done       = 0;
    id3_skip_remaining   = 0;

    strncpy(station_label, name ? name : "Episode",
            sizeof(station_label) - 1);
    station_label[sizeof(station_label) - 1] = 0;
    last_url[0] = 0;

    ls_log("episode: starting %s", station_label);
    setStatus("Buffering: %s", station_label);
    prebuffer_start_ms = pd->system->getCurrentTimeMilliseconds();
    state              = kPrebuffering;
    pd->lua->pushBool(1);
    return 1;
}

// Push N bytes from a Lua string into the ring, skipping any leading
// ID3v2 tag (very common in podcast MP3s; can be 50+ KB of metadata
// and cover art). Returns bytes ACCEPTED FROM THE INPUT (skipped + ring
// pushes). Skipped ID3 bytes count as accepted so Lua doesn't re-queue
// them.
static int l_feedC(lua_State* L)
{
    (void)L;
    size_t len = 0;
    const char* in = pd->lua->getArgBytes(1, &len);
    if (!in || len == 0 || !is_episode) {
        pd->lua->pushInt(0);
        return 1;
    }
    unsigned consumed = 0;

    // Step 1: peek the first 10 bytes for an ID3v2 header.
    while (consumed < (unsigned)len && !id3_check_done) {
        id3_check_buf[id3_check_pos++] = (uint8_t)in[consumed++];
        if (id3_check_pos == 10) {
            if (id3_check_buf[0] == 'I' && id3_check_buf[1] == 'D' &&
                id3_check_buf[2] == '3') {
                // Synchsafe 28-bit size (each byte uses only low 7 bits).
                unsigned sz =
                    ((unsigned)(id3_check_buf[6] & 0x7F) << 21) |
                    ((unsigned)(id3_check_buf[7] & 0x7F) << 14) |
                    ((unsigned)(id3_check_buf[8] & 0x7F) <<  7) |
                    ((unsigned)(id3_check_buf[9] & 0x7F));
                id3_skip_remaining = sz;
                ls_log("ID3v2 tag detected, skipping %u bytes", 10 + sz);
            } else {
                // No tag — push the 10 buffered bytes to the ring.
                unsigned w = atomic_load_explicit(&ring_write,
                                                  memory_order_relaxed);
                unsigned writable = ring_free();
                unsigned take = 10 < writable ? 10 : writable;
                for (unsigned k = 0; k < take; k++) {
                    ring[(w + k) & RING_MASK] = id3_check_buf[k];
                }
                atomic_store_explicit(&ring_write, (w + take) & RING_MASK,
                                      memory_order_release);
                bytes_received_total += take;
            }
            id3_check_done = 1;
        }
    }

    // Step 2: skip remaining ID3 bytes (if any).
    if (id3_skip_remaining > 0) {
        unsigned avail = (unsigned)len - consumed;
        unsigned drop  = avail < id3_skip_remaining ? avail : id3_skip_remaining;
        consumed           += drop;
        id3_skip_remaining -= drop;
    }

    // Step 3: push the rest into the audio ring.
    unsigned remaining = (unsigned)len - consumed;
    if (remaining > 0) {
        unsigned writable = ring_free();
        unsigned take = remaining < writable ? remaining : writable;
        if (take > 0) {
            unsigned w = atomic_load_explicit(&ring_write,
                                              memory_order_relaxed);
            for (unsigned k = 0; k < take; k++) {
                ring[(w + k) & RING_MASK] = (uint8_t)in[consumed + k];
            }
            atomic_store_explicit(&ring_write, (w + take) & RING_MASK,
                                  memory_order_release);
            bytes_received_total += take;
            consumed += take;
        }
    }

    pd->lua->pushInt((int)consumed);
    return 1;
}

// Lua signals that no more bytes are coming (download finished). The
// engine plays the remaining buffered MP3 + PCM, then transitions to
// idle once both drain.
static int l_finalizeEpisode(lua_State* L)
{
    (void)L;
    if (!is_episode) return 0;
    ls_log("episode: finalized (%u bytes total)", bytes_received_total);
    episode_finalized = 1;
    return 0;
}

static int l_tick(lua_State* L)
{
    (void)L;
    tickOnce();
    return 0;
}

static int l_isPlaying(lua_State* L)
{
    (void)L;
    pd->lua->pushBool(state == kPlaying);
    return 1;
}

static int l_isOffAir(lua_State* L)
{
    (void)L;
    pd->lua->pushBool(state == kOffAir);
    return 1;
}

static int l_isFinished(lua_State* L)
{
    (void)L;
    pd->lua->pushBool(episode_completed);   // played the whole episode to the end
    return 1;
}

static int l_isActive(lua_State* L)
{
    (void)L;
    pd->lua->pushBool(state != kIdle && state != kOffAir);
    return 1;
}

// Flow control for episode feeding: true only while the byte ring has comfortable
// room. Lua reads from the socket strictly in order and stops when this goes false,
// so a fast network can't overrun the ring (which would scramble the MP3 stream and
// make playback jump). TCP backpressure holds the rest until the ring drains.
static int l_room(lua_State* L)
{
    (void)L;
    pd->lua->pushBool(ring_free() > 16384);
    return 1;
}

static int l_statusText(lua_State* L)
{
    (void)L;
    pd->lua->pushString(status_text);
    return 1;
}

static int l_bytesText(lua_State* L)
{
    (void)L;
    static char buf[32];
    if (bytes_received_total < 1024) {
        snprintf(buf, sizeof(buf), "%u B", bytes_received_total);
    } else if (bytes_received_total < 1024 * 1024) {
        snprintf(buf, sizeof(buf), "%u KB", bytes_received_total / 1024);
    } else {
        snprintf(buf, sizeof(buf), "%.1f MB",
                 (double)bytes_received_total / (1024.0 * 1024.0));
    }
    pd->lua->pushString(buf);
    return 1;
}

// ──────────────────────────────────────────────────────────────────────
// Entry
// ──────────────────────────────────────────────────────────────────────

// Pairing this with the companion VIDEO engine?
//   https://github.com/jnemargut/playdate-video-streaming
// A .pdx may export only ONE eventHandler. To run both, keep this one and, at the two
// spots marked "+ video engine" below, add the video engine's hooks:
//     extern void streamvideo_setPD(PlaydateAPI* p);                               // declare up here
//     extern int  eventHandler_streamvideo(PlaydateAPI*, PDSystemEvent, uint32_t); // declare up here
//     streamvideo_setPD(playdate);                     // in kEventInit
//     eventHandler_streamvideo(playdate, event, arg);  // in kEventInitLua
// ...and do NOT compile that repo's streamvideo_entry.c (it defines a second handler).

#ifdef _WINDLL
__declspec(dllexport)
#endif
int eventHandler(PlaydateAPI* playdate, PDSystemEvent event, uint32_t arg)
{
    (void)arg;

    if (event == kEventInit) {
        pd  = playdate;
        tcp = pd->network->tcp;
        pd->system->logToConsole("livestream: native extension loaded");
        // + video engine: streamvideo_setPD(playdate);
        // Don't call pd->network->setEnabled at boot. Hardware hard-faults
        // when we pass a NULL callback (it dereferences as a function
        // pointer). The network turns on lazily on the first tcp->open
        // anyway — we don't need to pre-kick it. (We previously added the
        // call while debugging the HTTP-vs-TCP issue; once we switched to
        // TCP, this call became unnecessary.)
    } else if (event == kEventInitLua) {
        // Register Lua-callable functions. main.lua will route through
        // Source/livestream.lua which wraps these.
        const char* err = NULL;
        #define REG(fn, name) \
            if (!pd->lua->addFunction(fn, name, &err)) \
                pd->system->logToConsole("livestream: addFunction %s failed: %s", name, err ? err : "(null)");
        REG(l_start,           "livestream.startC");
        REG(l_stop,            "livestream.stopC");
        REG(l_tick,            "livestream.tickC");
        REG(l_isPlaying,       "livestream.isPlayingC");
        REG(l_isOffAir,        "livestream.isOffAirC");
        REG(l_isFinished,      "livestream.isFinishedC");
        REG(l_room,            "livestream.roomC");
        REG(l_isActive,        "livestream.isActiveC");
        REG(l_statusText,      "livestream.statusTextC");
        REG(l_bytesText,       "livestream.bytesTextC");
        REG(l_startEpisode,    "livestream.startEpisodeC");
        REG(l_feedC,           "livestream.feedC");
        REG(l_finalizeEpisode, "livestream.finalizeEpisodeC");
        #undef REG
        // + video engine: eventHandler_streamvideo(playdate, event, arg);
    } else if (event == kEventTerminate) {
        teardownPlayback();
    }

    return 0;
}
