-- StreamAudio.lua
-- A small Lua front-end for the native MP3 streaming engine (src/streamaudio.c),
-- "episode" mode: Lua does the HTTP(S) GET and feeds the bytes to C as they arrive,
-- which decodes them with minimp3 and plays as it goes. Plays after a ~3s prebuffer,
-- not after the whole file.
--
-- The bytes are fed STRICTLY in order and only while the engine has room (roomC): the
-- unaccepted tail of a chunk is held in `leftover` and fed first next time, so a fast
-- or long stream can't reorder the MP3 byte stream (which would make playback jump).
-- When the ring is full we stop reading; TCP backpressure holds the rest.
--
-- This file is dependency-free: call configure() once, then play()/tick(). For LIVE
-- radio (direct TCP, no Lua HTTP) use the C `livestream.startC(url, name)` instead —
-- see the README.
--
--   Stream:        StreamAudio.play("path/show.mp3")
--   Stream + save: StreamAudio.play("path/show.mp3", "/Data/show.mp3")
--
-- Companion video engine: https://github.com/jnemargut/unofficial-playdate-video-streaming

StreamAudio = {}
StreamAudio.debug = false

local C = (livestream ~= nil and livestream.startEpisodeC ~= nil) and livestream or nil
local HOST, PORT, SSL = nil, 443, true

local conn, leftover, active = nil, nil, false
local headersOk, finalizePending = false, false
local bytesDown, bytesTotal = 0, 0
local outFile, cacheDest, doCache, cacheCb = nil, nil, false, nil
local cacheOnly, failed = false, false
local lastDataMs = 0
local STALL_MS = 20000
local READ_CAP = 16384

local function log(s) if StreamAudio.debug then print("SA: " .. s) end end

-- Require the full episode + flow-control surface. If a build ever ships a stale
-- native binary (missing roomC/finalize), treat streaming as unavailable so the app
-- can fall back to downloading instead of crashing on a missing C function.
function StreamAudio.available()
	return C ~= nil and C.roomC ~= nil and C.feedC ~= nil
		and C.finalizeEpisodeC ~= nil and C.startEpisodeC ~= nil
end

-- Point the streamer at your server. Call once at startup.
function StreamAudio.configure(host, port, useSSL)
	HOST, PORT, SSL = host, port or 443, useSSL ~= false
end

function StreamAudio.isCaching() return active and outFile ~= nil and doCache end
function StreamAudio.failed() return failed end
function StreamAudio.progress() return bytesDown, bytesTotal end

local function parseUrl(url)
	local proto, rest = url:match("^(https?)://(.+)$")
	if not rest then return nil end
	local host, path = rest:match("^([^/]+)(/.*)$")
	if not host then host = rest; path = "/" end
	return host, (proto == "https") and 443 or 80, path, (proto == "https")
end

local function closeFile(keep)
	if not outFile then return end
	pcall(function() outFile:close() end); outFile = nil
	if keep and doCache and cacheDest then
		playdate.file.delete(cacheDest); playdate.file.rename(cacheDest .. ".part", cacheDest)
		if cacheCb then cacheCb(cacheDest) end      -- tell the app a cached file is ready
	elseif cacheDest then
		playdate.file.delete(cacheDest .. ".part")
	end
end

local function feed(buf)
	local accepted = C.feedC(buf)
	if accepted < #buf then leftover = string.sub(buf, accepted + 1) end
end

-- Read from the socket and hand bytes to C, in order, only while there's room.
local function pump(c)
	if not C or not c then return end
	if cacheOnly then                          -- detached: drain to cache fast, no decode
		while true do
			local avail = c:getBytesAvailable() or 0
			if avail <= 0 then break end
			local buf = c:read(avail < 32768 and avail or 32768)
			if not buf or #buf == 0 then break end
			if outFile then outFile:write(buf) end
			bytesDown = bytesDown + #buf
			lastDataMs = playdate.getCurrentTimeMilliseconds()
		end
		return
	end
	if not headersOk then return end           -- wait for a 2xx (redirects handled on complete)
	if leftover then local p = leftover; leftover = nil; feed(p); if leftover then return end end
	while C.roomC() do
		local avail = c:getBytesAvailable() or 0
		if avail <= 0 then break end
		local buf = c:read(avail < READ_CAP and avail or READ_CAP)
		if not buf or #buf == 0 then break end
		if outFile then outFile:write(buf) end
		feed(buf)
		bytesDown = bytesDown + #buf
		lastDataMs = playdate.getCurrentTimeMilliseconds()
		if bytesTotal == 0 then local _, t = c:getProgress(); if t and t > 0 then bytesTotal = t end end
		if leftover then break end             -- ring full mid-chunk: stop, resume next tick
	end
end

local function getToStream(host, port, useSSL, path, redirectsLeft)
	redirectsLeft = redirectsLeft or 3
	local c = playdate.network.http.new(host, port, useSSL, "stream audio")
	conn = c
	if not c then failed = true; return end
	local isRedirect, fired = false, false
	headersOk = false
	lastDataMs = playdate.getCurrentTimeMilliseconds()

	c:setHeadersReadCallback(function()
		if fired or not active then return end
		local status = c:getResponseStatus() or 0
		log("headers status=" .. status)
		if status >= 300 and status < 400 then isRedirect = true; return end
		isRedirect = false
		if status >= 200 and status < 300 then
			headersOk = true
			if doCache and cacheDest then outFile = playdate.file.open(cacheDest .. ".part", playdate.file.kFileWrite) end
		end
	end)

	c:setRequestCallback(function()
		if fired or not active then return end
		pump(c)
	end)

	c:setRequestCompleteCallback(function()
		if fired or not active then return end
		local err = c:getError()
		if err and err ~= "Connection closed" then
			fired = true; log("error " .. tostring(err)); closeFile(false); failed = true; return
		end
		if isRedirect and redirectsLeft > 0 then
			local headers = c:getResponseHeaders() or {}
			local loc = headers["Location"] or headers["location"]
			if loc then
				fired = true
				pcall(function() c:close() end)
				if loc:sub(1, 1) == "/" then getToStream(host, port, useSSL, loc, redirectsLeft - 1)
				else local h, p, pa, s = parseUrl(loc); if h then getToStream(h, p, s, pa, redirectsLeft - 1) else failed = true end end
				return
			end
			fired = true; failed = true; return
		end
		local status = c:getResponseStatus() or 0
		if status >= 200 and status < 300 and bytesDown > 0 then
			fired = true
			pump(c)                            -- drain any final readable bytes, in order
			closeFile(true)
			if cacheOnly then conn = nil; active = false; cacheOnly = false
			else finalizePending = true end    -- tick finalizes once leftover is fully fed
		else
			fired = true; log("bad status " .. status .. " bytes=" .. bytesDown); closeFile(false); failed = true
		end
	end)

	c:setConnectionClosedCallback(function() end)
	c:setConnectTimeout(10)
	local ok = c:get(path)
	log("get('" .. path .. "') ok=" .. tostring(ok))
	if not ok then failed = true end
end

-- Stream /path over HTTP(S). With cachePath, the bytes are also written to disk
-- (Stream+Save); onCached(path) fires when the save completes.
function StreamAudio.play(path, cachePath, onCached)
	if not StreamAudio.available() or not HOST then return false end
	StreamAudio.stop()
	leftover, active, cacheOnly, failed, finalizePending = nil, true, false, false, false
	headersOk = false
	bytesDown, bytesTotal = 0, 0
	cacheDest, doCache, cacheCb, outFile = cachePath, (cachePath ~= nil), onCached, nil
	C.startEpisodeC(path)
	getToStream(HOST, PORT, SSL, "/" .. path, 3)
	return true
end

-- Call once per frame from playdate.update().
function StreamAudio.tick()
	if not C or not active then return end
	-- stall watchdog (the byte ring filling on a fast network is normal, not a stall)
	if not failed and conn and (playdate.getCurrentTimeMilliseconds() - lastDataMs) > STALL_MS then
		log("stalled"); failed = true
	end
	if cacheOnly then pump(conn); return end
	pump(conn)
	if finalizePending and not leftover then finalizePending = false; C.finalizeEpisodeC() end
	C.tickC()
end

-- Leave the player but finish writing the cache in the background.
function StreamAudio.detach()
	if not active or not (outFile and doCache) then StreamAudio.stop(); return end
	cacheOnly, leftover = true, nil
	if C then pcall(function() C.stopC() end) end
end

function StreamAudio.finished()  return (C and C.isFinishedC and C.isFinishedC()) == true end
function StreamAudio.isPlaying() return (C and active and not cacheOnly and C.isPlayingC()) == true end
function StreamAudio.isActive()  return active end

function StreamAudio.stop()
	local c = conn; conn = nil
	active, cacheOnly, failed, headersOk, finalizePending = false, false, false, false, false
	leftover = nil
	if c then pcall(function() c:close() end) end
	closeFile(false)
	if C then pcall(function() C.stopC() end) end
end

return StreamAudio
