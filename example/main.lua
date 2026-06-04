-- example/main.lua
-- Minimal reference for streaming an MP3. Copy StreamAudio.lua (from ../lua) into your
-- source, add the C from ../src to your build, host a prepared .mp3, and set HOST/CLIP.
-- This isn't turn-key (it needs your own content + build) — it's the smallest honest
-- example of the integration.

import "CoreLibs/graphics"
import "StreamAudio"

local gfx = playdate.graphics

local HOST = "your-host.example.com"   -- where your .mp3 is served from (HTTPS)
local CLIP = "audio/show.mp3"          -- path under that host
local CACHE = "/show.mp3"              -- local Data path to Stream+Save into (optional)

StreamAudio.configure(HOST, 443, true)
-- StreamAudio.debug = true            -- uncomment for console logs

local started = false

function playdate.update()
	if not StreamAudio.available() then
		gfx.clear(); gfx.drawTextAligned("native engine missing", 200, 116, kTextAlignment.center)
		return
	end

	if not started then
		started = true
		StreamAudio.play(CLIP, CACHE, function(p) print("cached -> " .. p) end)
	end

	StreamAudio.tick()

	gfx.clear()
	local line
	if StreamAudio.failed() then line = "couldn't connect"
	elseif StreamAudio.isPlaying() then line = "playing"
	elseif StreamAudio.finished() then line = "done"
	else line = "buffering…" end
	gfx.drawTextAligned(line, 200, 116, kTextAlignment.center)
end

-- Tidy up if the player leaves this scene / the app loses focus.
function playdate.gameWillTerminate() StreamAudio.stop() end
function playdate.deviceWillSleep()   StreamAudio.stop() end
