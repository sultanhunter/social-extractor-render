const { execFile } = require("node:child_process");
const { randomUUID } = require("node:crypto");
const { existsSync, writeFileSync } = require("node:fs");
const { resolve } = require("node:path");
const { promisify } = require("node:util");

const cors = require("cors");
const dotenv = require("dotenv");
const express = require("express");

dotenv.config();

const execFileAsync = promisify(execFile);
const app = express();
const port = Number(process.env.PORT) || 3000;
let generatedCookiesPath = null;

const DEFAULT_FRAME_COUNT = 6;
const MIN_FRAME_COUNT = 2;
const MAX_FRAME_COUNT = 12;
const DEFAULT_FRAME_WIDTH = 960;
const MIN_FRAME_WIDTH = 480;
const MAX_FRAME_WIDTH = 1440;
const DEFAULT_TRANSCRIPT_ENABLED = true;
const DEFAULT_TRANSCRIPT_MAX_SECONDS = 90;
const MIN_TRANSCRIPT_MAX_SECONDS = 20;
const MAX_TRANSCRIPT_MAX_SECONDS = 180;

app.use(cors());
app.use(express.json({ limit: "1mb" }));

function isHttpUrl(value) {
  return typeof value === "string" && /^https?:\/\//i.test(value);
}

function dedupe(urls) {
  return Array.from(new Set(urls.filter(Boolean)));
}

function normalizeEscapedUrl(url) {
  if (typeof url !== "string") return "";
  return url
    .trim()
    .replace(/\\u0026/g, "&")
    .replace(/\\\//g, "/")
    .replace(/\\u002F/g, "/")
    .replace(/\\u003D/g, "=")
    .replace(/\\u0025/g, "%")
    .replace(/\\"/g, '"');
}

function hasImageExtension(pathnameWithQuery) {
  return /\.(jpe?g|png|webp|gif|bmp)(\?|$)/i.test(pathnameWithQuery);
}

function isLikelyDirectMediaUrl(url, platform) {
  if (!isHttpUrl(url)) return false;

  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    const pathWithQuery = `${parsed.pathname}${parsed.search}`.toLowerCase();

    if (platform !== "tiktok") {
      return true;
    }

    if (host === "www.tiktok.com" || host === "m.tiktok.com" || host === "tiktok.com") {
      return false;
    }

    if (pathWithQuery.includes("/notfound") || pathWithQuery.startsWith("/t/")) {
      return false;
    }

    if (pathWithQuery.includes("mime_type=audio") || pathWithQuery.includes("mime_type=video")) {
      return false;
    }

    if (pathWithQuery.includes("/video/")) {
      return false;
    }

    if (hasImageExtension(pathWithQuery)) {
      return true;
    }

    const likelyCdnHost =
      host.includes("tiktokcdn") ||
      host.includes("byteoversea") ||
      host.includes("bytedance") ||
      host.includes("ibytedtos") ||
      host.includes("muscdn") ||
      host.includes("snssdk") ||
      host.includes("akamaized") ||
      host.includes("cloudfront");

    const likelyMediaPath =
      pathWithQuery.includes("/obj/") ||
      pathWithQuery.includes("/tos-") ||
      pathWithQuery.includes("/image/") ||
      pathWithQuery.includes("photomode") ||
      pathWithQuery.includes("mime_type=image");

    return likelyCdnHost && likelyMediaPath;
  } catch {
    return false;
  }
}

function isLikelyVideoUrl(url) {
  if (!isHttpUrl(url)) return false;

  const lower = String(url).toLowerCase();

  if (isLikelyAudioOnlyUrl(lower)) {
    return false;
  }

  return (
    /\.(mp4|mov|webm|m4v|m3u8)(\?|$)/i.test(lower) ||
    lower.includes("mime_type=video") ||
    lower.includes("/video/") ||
    lower.includes("/play/") ||
    lower.includes("/aweme/v1/play")
  );
}

function isLikelyAudioOnlyUrl(url) {
  const lower = String(url).toLowerCase();

  if (lower.includes("mime_type=audio") || lower.includes("/audio/")) {
    return true;
  }

  const hasAudioMarker =
    lower.includes("_audio") ||
    lower.includes("audio_") ||
    lower.includes("heaac") ||
    lower.includes("aac") ||
    lower.includes("opus");

  const hasVideoMarker =
    lower.includes("mime_type=video") ||
    lower.includes("_video") ||
    lower.includes("video_") ||
    lower.includes("vcodec") ||
    lower.includes("/video/");

  return hasAudioMarker && !hasVideoMarker;
}

function scoreVideoUrl(url) {
  const lower = String(url).toLowerCase();
  let score = 0;

  if (lower.includes(".mp4")) score += 50;
  if (lower.includes("mime_type=video")) score += 35;
  if (lower.includes("/play/") || lower.includes("/aweme/v1/play")) score += 20;
  if (lower.includes(".m3u8")) score -= 20;
  if (lower.includes("audio")) score -= 60;

  return score;
}

function prioritizeVideoUrls(urls) {
  return dedupe(urls)
    .filter((url) => isLikelyVideoUrl(url))
    .sort((a, b) => scoreVideoUrl(b) - scoreVideoUrl(a));
}

function toBoundedInteger(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  if (parsed < min) return min;
  if (parsed > max) return max;
  return parsed;
}

function toBoolean(value, fallback) {
  if (typeof value === "boolean") return value;
  if (typeof value !== "string") return fallback;

  const normalized = value.trim().toLowerCase();
  if (normalized === "true") return true;
  if (normalized === "false") return false;
  return fallback;
}

function normalizeAndFilterMediaUrls(urls, platform) {
  return dedupe(
    (Array.isArray(urls) ? urls : [])
      .map((item) => normalizeEscapedUrl(item))
      .filter((item) => isLikelyDirectMediaUrl(item, platform))
  );
}

function toSingleLine(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function truncate(value, max = 500) {
  if (value.length <= max) return value;
  return `${value.slice(0, max)}...`;
}

function getLogPrefix(requestId) {
  return `[social-extract] req=${requestId}`;
}

function formatExecError(tool, error) {
  if (!(error instanceof Error)) {
    return `${tool} failed with unknown error`;
  }

  const parts = [truncate(toSingleLine(error.message))];
  if (typeof error.code !== "undefined") parts.push(`code=${String(error.code)}`);
  if (error.signal) parts.push(`signal=${error.signal}`);
  if (error.stderr) parts.push(`stderr=${truncate(toSingleLine(error.stderr))}`);
  if (error.stdout) parts.push(`stdout=${truncate(toSingleLine(error.stdout))}`);
  return parts.join(" | ");
}

function getBearerToken(authHeader) {
  if (!authHeader) return null;
  const [scheme, token] = authHeader.split(" ");
  if (!scheme || !token || scheme.toLowerCase() !== "bearer") return null;
  return token;
}

function authorizeRequest(req, res, requestId) {
  const requiredToken = process.env.SOCIAL_EXTRACTOR_API_TOKEN || process.env.EXTRACTOR_API_TOKEN;
  const providedToken = getBearerToken(req.headers.authorization);

  if (requiredToken && providedToken !== requiredToken) {
    const logPrefix = getLogPrefix(requestId);
    console.warn(`${logPrefix} unauthorized token mismatch`);
    res.status(401).json({
      error: "Unauthorized",
      details: "Missing or invalid extractor API token",
      requestId,
    });
    return false;
  }

  return true;
}

function extractPlatform(url) {
  const normalized = String(url).toLowerCase();
  if (normalized.includes("instagram.com")) return "instagram";
  if (normalized.includes("tiktok.com")) return "tiktok";
  if (normalized.includes("youtube.com") || normalized.includes("youtu.be")) return "youtube";
  if (normalized.includes("twitter.com") || normalized.includes("x.com")) return "twitter";
  return "unknown";
}

function withOptionalSession(username, sessionId) {
  if (!sessionId) return username;
  if (process.env.DECODO_PROXY_USE_SESSION !== "true") return username;
  if (username.includes("session-")) return username;
  return `${username}-session-${sessionId}`;
}

function getProxyUrl(sessionId) {
  const directUrl = process.env.DECODO_PROXY_URL || process.env.PROXY_URL;
  if (directUrl) return directUrl;

  const host = process.env.DECODO_PROXY_HOST || process.env.PROXY_HOST;
  const proxyPort = process.env.DECODO_PROXY_PORT || process.env.PROXY_PORT;
  const username = process.env.DECODO_PROXY_USERNAME || process.env.PROXY_USERNAME;
  const password = process.env.DECODO_PROXY_PASSWORD || process.env.PROXY_PASSWORD;

  if (!host || !proxyPort || !username || !password) return null;

  const finalUsername = withOptionalSession(username, sessionId);
  return `http://${encodeURIComponent(finalUsername)}:${encodeURIComponent(password)}@${host}:${proxyPort}`;
}

function resolveInstagramCookiesFile() {
  if (generatedCookiesPath && existsSync(generatedCookiesPath)) {
    return generatedCookiesPath;
  }

  const rootCookiesFile = resolve(process.cwd(), "instagram_cookies.txt");
  if (existsSync(rootCookiesFile)) {
    return rootCookiesFile;
  }

  const inlineCookies = process.env.INSTAGRAM_COOKIES_CONTENT;
  if (inlineCookies && inlineCookies.trim()) {
    const tempCookiesFile = "/tmp/instagram_cookies.txt";
    writeFileSync(tempCookiesFile, inlineCookies, { encoding: "utf8", mode: 0o600 });
    generatedCookiesPath = tempCookiesFile;
    return tempCookiesFile;
  }

  return null;
}

function getThumbnailUrls(entry) {
  if (!Array.isArray(entry.thumbnails)) return [];

  return dedupe(
    entry.thumbnails
      .map((item) => (item && typeof item === "object" ? item : null))
      .filter(Boolean)
      .map((thumb) => (isHttpUrl(thumb.url) ? thumb.url : null))
      .filter(Boolean)
  );
}

function extractUrlsFromEntry(entry) {
  const urls = [];
  if (!entry || typeof entry !== "object") return [];

  if (isHttpUrl(entry.url)) urls.push(entry.url);
  if (isHttpUrl(entry.thumbnail)) urls.push(entry.thumbnail);

  urls.push(...getThumbnailUrls(entry));

  if (Array.isArray(entry.formats)) {
    for (const format of entry.formats) {
      if (!format || typeof format !== "object") continue;
      if (isHttpUrl(format.url)) urls.push(format.url);
    }
  }

  return dedupe(urls);
}

function extractVideoUrlsFromEntry(entry) {
  if (!entry || typeof entry !== "object") return [];

  const urls = [];

  const entryVcodec = typeof entry.vcodec === "string" ? entry.vcodec.toLowerCase() : "";
  const entryExt = typeof entry.ext === "string" ? entry.ext.toLowerCase() : "";
  const entryIsVideoTrack = entryVcodec && entryVcodec !== "none";
  const entryLooksVideoExt =
    entryExt === "mp4" || entryExt === "mov" || entryExt === "webm" || entryExt === "m3u8";

  if (
    isHttpUrl(entry.url) &&
    !isLikelyAudioOnlyUrl(entry.url) &&
    (entryIsVideoTrack || entryLooksVideoExt || isLikelyVideoUrl(entry.url))
  ) {
    urls.push(entry.url);
  }

  const candidates = [];
  if (Array.isArray(entry.formats)) candidates.push(...entry.formats);
  if (Array.isArray(entry.requested_formats)) candidates.push(...entry.requested_formats);

  for (const format of candidates) {
    if (!format || typeof format !== "object") continue;

    const formatUrl = isHttpUrl(format.url) ? format.url : null;
    if (!formatUrl) continue;

    const vcodec = typeof format.vcodec === "string" ? format.vcodec.toLowerCase() : "";
    const ext = typeof format.ext === "string" ? format.ext.toLowerCase() : "";
    const isVideoTrack = vcodec && vcodec !== "none";
    const looksVideoExt = ext === "mp4" || ext === "mov" || ext === "webm" || ext === "m3u8";

    if (!isLikelyAudioOnlyUrl(formatUrl) && (isVideoTrack || looksVideoExt || isLikelyVideoUrl(formatUrl))) {
      urls.push(formatUrl);
    }
  }

  return prioritizeVideoUrls(urls);
}

async function runYtDlpForVideoStream(url, platform, requestId, sessionId) {
  const binary = process.env.YT_DLP_PATH || "yt-dlp";
  const proxy = getProxyUrl(sessionId);
  const cookiesFile = resolveInstagramCookiesFile();
  const startedAt = Date.now();

  const args = ["--dump-single-json", "--skip-download", "--no-warnings", "--no-call-home", "--ignore-errors"];
  if (proxy) args.push("--proxy", proxy);
  if (cookiesFile && platform === "instagram") args.push("--cookies", cookiesFile);

  const instagramSessionId = process.env.INSTAGRAM_SESSIONID;
  if (instagramSessionId && platform === "instagram") {
    args.push("--add-header", `Cookie: sessionid=${instagramSessionId}`);
  }

  args.push(url);

  console.log(
    `${getLogPrefix(requestId)} yt-dlp-video start binary=${binary} proxy=${proxy ? "yes" : "no"} cookies=${cookiesFile ? "yes" : "no"} session=${sessionId || "none"}`
  );

  try {
    const { stdout } = await execFileAsync(binary, args, {
      timeout: 120000,
      maxBuffer: 15 * 1024 * 1024,
    });

    const payload = parseYtDlpStdout(stdout);
    const entries = Array.isArray(payload.entries)
      ? payload.entries.filter((item) => item && typeof item === "object")
      : [];

    const candidateVideoUrls =
      entries.length > 0
        ? prioritizeVideoUrls(entries.flatMap((entry) => extractVideoUrlsFromEntry(entry)))
        : extractVideoUrlsFromEntry(payload);

    console.log(
      `${getLogPrefix(requestId)} yt-dlp-video success elapsedMs=${Date.now() - startedAt} candidates=${candidateVideoUrls.length}`
    );

    return {
      title: typeof payload.title === "string" ? payload.title : null,
      description: typeof payload.description === "string" ? payload.description : null,
      candidateVideoUrls,
    };
  } catch (error) {
    const formatted = formatExecError("yt-dlp-video", error);
    console.error(
      `${getLogPrefix(requestId)} yt-dlp-video failed elapsedMs=${Date.now() - startedAt} ${formatted}`
    );
    throw new Error(formatted);
  }
}

async function runGalleryDlForVideoStream(url, platform, requestId, sessionId) {
  const binary = process.env.GALLERY_DL_PATH || "gallery-dl";
  const proxy = getProxyUrl(sessionId);
  const cookiesFile = resolveInstagramCookiesFile();
  const startedAt = Date.now();

  const args = [];
  if (proxy) args.push("--proxy", proxy);
  if (cookiesFile && platform === "instagram") args.push("--cookies", cookiesFile);
  args.push("-g", url);

  console.log(
    `${getLogPrefix(requestId)} gallery-dl-video start binary=${binary} proxy=${proxy ? "yes" : "no"} cookies=${cookiesFile ? "yes" : "no"} session=${sessionId || "none"}`
  );

  try {
    const { stdout } = await execFileAsync(binary, args, {
      timeout: 120000,
      maxBuffer: 15 * 1024 * 1024,
    });

    const rawUrls = dedupe(
      String(stdout)
        .split("\n")
        .map((line) => normalizeEscapedUrl(line))
        .filter((line) => isHttpUrl(line))
    );

    const candidateVideoUrls = prioritizeVideoUrls(rawUrls);

    console.log(
      `${getLogPrefix(requestId)} gallery-dl-video success elapsedMs=${Date.now() - startedAt} candidates=${candidateVideoUrls.length}`
    );

    return {
      candidateVideoUrls,
    };
  } catch (error) {
    const formatted = formatExecError("gallery-dl-video", error);
    console.error(
      `${getLogPrefix(requestId)} gallery-dl-video failed elapsedMs=${Date.now() - startedAt} ${formatted}`
    );
    throw new Error(formatted);
  }
}

async function probeVideoDuration(videoUrl) {
  const args = [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    videoUrl,
  ];

  try {
    const { stdout } = await execFileAsync("ffprobe", args, {
      timeout: 30000,
      maxBuffer: 1024 * 1024,
    });

    const duration = Number.parseFloat(String(stdout || "").trim());
    if (!Number.isFinite(duration) || duration <= 0) return null;
    return duration;
  } catch {
    return null;
  }
}

async function hasVideoStream(videoUrl) {
  const args = [
    "-v",
    "error",
    "-select_streams",
    "v:0",
    "-show_entries",
    "stream=codec_type",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    videoUrl,
  ];

  try {
    const { stdout } = await execFileAsync("ffprobe", args, {
      timeout: 30000,
      maxBuffer: 1024 * 1024,
    });

    return String(stdout || "").toLowerCase().includes("video");
  } catch {
    return false;
  }
}

function buildFrameTimestamps(durationSeconds, frameCount) {
  if (!durationSeconds || durationSeconds <= 0.8) {
    const defaults = [0.2, 0.7, 1.2, 1.7, 2.2, 2.7, 3.2, 3.7];
    return defaults.slice(0, frameCount);
  }

  const clipLength = Math.max(0.8, durationSeconds - 0.2);
  const timestamps = [];

  for (let i = 0; i < frameCount; i += 1) {
    const ratio = frameCount === 1 ? 0.5 : i / (frameCount - 1);
    const second = Math.max(0.1, Math.min(clipLength, clipLength * ratio));
    timestamps.push(Number.parseFloat(second.toFixed(2)));
  }

  return dedupe(timestamps.map((item) => item.toFixed(2))).map((item) => Number.parseFloat(item));
}

async function extractFramesFromVideoUrl(videoUrl, frameCount, frameWidth, requestId) {
  const hasVideo = await hasVideoStream(videoUrl);
  if (!hasVideo) {
    throw new Error("resolved stream has no video track");
  }

  const durationSeconds = await probeVideoDuration(videoUrl);
  const timestamps = buildFrameTimestamps(durationSeconds, frameCount);
  const frames = [];

  for (let index = 0; index < timestamps.length; index += 1) {
    const timestamp = timestamps[index];
    const args = [
      "-v",
      "error",
      "-ss",
      String(timestamp),
      "-i",
      videoUrl,
      "-frames:v",
      "1",
      "-vf",
      `scale=${frameWidth}:-2`,
      "-q:v",
      "4",
      "-f",
      "image2pipe",
      "-vcodec",
      "mjpeg",
      "pipe:1",
    ];

    try {
      const { stdout } = await execFileAsync("ffmpeg", args, {
        timeout: 60000,
        maxBuffer: 8 * 1024 * 1024,
        encoding: "buffer",
      });

      if (!stdout || !stdout.length) continue;

      frames.push({
        index: index + 1,
        timestamp,
        mimeType: "image/jpeg",
        data: stdout.toString("base64"),
      });
    } catch (error) {
      console.warn(
        `${getLogPrefix(requestId)} frame_extract_failed timestamp=${timestamp} ${formatExecError("ffmpeg", error)}`
      );
    }
  }

  return {
    durationSeconds,
    requestedFrameCount: frameCount,
    extractedFrameCount: frames.length,
    frames,
  };
}

async function extractFramesFromCandidates(candidateUrls, frameCount, frameWidth, requestId) {
  const errors = [];

  for (const candidateUrl of candidateUrls) {
    if (!isHttpUrl(candidateUrl)) continue;
    if (isLikelyAudioOnlyUrl(candidateUrl)) {
      errors.push(`skip audio-only candidate: ${truncate(candidateUrl, 120)}`);
      continue;
    }

    try {
      const frameResult = await extractFramesFromVideoUrl(
        candidateUrl,
        frameCount,
        frameWidth,
        requestId
      );

      if (frameResult.extractedFrameCount > 0) {
        return {
          videoUrl: candidateUrl,
          frameResult,
          errors,
        };
      }

      errors.push(`candidate produced zero frames: ${truncate(candidateUrl, 120)}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown candidate extraction error";
      errors.push(`candidate failed: ${truncate(candidateUrl, 120)} | ${message}`);
      console.warn(`${getLogPrefix(requestId)} candidate_failed url=${truncate(candidateUrl, 180)} message=${message}`);
    }
  }

  return {
    videoUrl: null,
    frameResult: null,
    errors,
  };
}

function parseJsonObject(text) {
  if (typeof text !== "string") return null;

  const cleaned = text
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "")
    .trim();

  try {
    const parsed = JSON.parse(cleaned);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    const match = cleaned.match(/\{[\s\S]*\}/);
    if (!match) return null;
    try {
      const parsed = JSON.parse(match[0]);
      return parsed && typeof parsed === "object" ? parsed : null;
    } catch {
      return null;
    }
  }
}

async function extractAudioBufferForTranscript(videoUrl, maxSeconds) {
  const args = [
    "-v",
    "error",
    "-i",
    videoUrl,
    "-vn",
    "-ac",
    "1",
    "-ar",
    "16000",
    "-t",
    String(maxSeconds),
    "-f",
    "mp3",
    "-b:a",
    "64k",
    "pipe:1",
  ];

  const { stdout } = await execFileAsync("ffmpeg", args, {
    timeout: 90000,
    maxBuffer: 20 * 1024 * 1024,
    encoding: "buffer",
  });

  return Buffer.isBuffer(stdout) ? stdout : Buffer.from(stdout || "");
}

async function transcribeAudioWithGemini(audioBuffer, requestId) {
  const apiKey =
    process.env.GOOGLE_GEMINI_API_KEY || process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;

  if (!apiKey) {
    throw new Error("Missing GEMINI API key for transcript extraction on extractor service.");
  }

  const model = process.env.GEMINI_TRANSCRIBE_MODEL || "gemini-3.1-flash-lite-preview";
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const prompt = `Transcribe this short-form social video audio.

Rules:
- Return strict JSON only.
- Preserve original language.
- Keep punctuation natural.
- If speech is unclear, still provide best-effort text.

JSON shape:
{
  "language": "string",
  "summary": "1-2 sentence summary",
  "fullText": "full transcript text",
  "segments": [
    { "startSec": 0.0, "endSec": 2.4, "text": "..." }
  ]
}`;

  const payload = {
    contents: [
      {
        parts: [
          { text: prompt },
          {
            inlineData: {
              mimeType: "audio/mpeg",
              data: audioBuffer.toString("base64"),
            },
          },
        ],
      },
    ],
    generationConfig: {
      temperature: 0.1,
      responseMimeType: "application/json",
    },
  };

  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const raw = await response.text();
    throw new Error(`Gemini transcript request failed (${response.status}): ${truncate(toSingleLine(raw), 300)}`);
  }

  const data = await response.json();
  const candidateText =
    data?.candidates?.[0]?.content?.parts
      ?.map((part) => (typeof part?.text === "string" ? part.text : ""))
      .filter(Boolean)
      .join("\n") || "";

  const parsed = parseJsonObject(candidateText);
  if (!parsed) {
    throw new Error("Gemini transcript response was not valid JSON");
  }

  const segmentsRaw = Array.isArray(parsed.segments) ? parsed.segments : [];
  const segments = segmentsRaw
    .map((item) => {
      if (!item || typeof item !== "object") return null;
      const row = item;
      const text = typeof row.text === "string" ? row.text.trim() : "";
      if (!text) return null;

      return {
        startSec: typeof row.startSec === "number" ? row.startSec : 0,
        endSec: typeof row.endSec === "number" ? row.endSec : 0,
        text,
      };
    })
    .filter(Boolean)
    .slice(0, 80);

  const fullText =
    typeof parsed.fullText === "string" && parsed.fullText.trim()
      ? parsed.fullText.trim()
      : segments.map((item) => item.text).join(" ").trim();

  return {
    provider: "gemini",
    model,
    language: typeof parsed.language === "string" ? parsed.language.trim() : "unknown",
    summary: typeof parsed.summary === "string" ? parsed.summary.trim() : "",
    fullText,
    segments,
    generatedAt: new Date().toISOString(),
  };
}

async function buildVideoTranscript(videoUrl, requestId, maxSeconds) {
  try {
    const audioBuffer = await extractAudioBufferForTranscript(videoUrl, maxSeconds);

    if (!audioBuffer || audioBuffer.length < 1024) {
      return {
        available: false,
        error: "audio extraction produced an empty payload",
      };
    }

    const transcript = await transcribeAudioWithGemini(audioBuffer, requestId);

    if (!transcript.fullText) {
      return {
        available: false,
        error: "transcript extraction returned empty text",
      };
    }

    return {
      available: true,
      ...transcript,
      maxSeconds,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown transcript extraction error";
    console.warn(`${getLogPrefix(requestId)} transcript_failed message=${message}`);

    return {
      available: false,
      error: message,
    };
  }
}

function parseYtDlpStdout(stdout) {
  const trimmed = String(stdout || "").trim();
  if (!trimmed) throw new Error("yt-dlp returned empty output");

  try {
    return JSON.parse(trimmed);
  } catch {
    const lines = trimmed.split("\n").reverse();
    for (const line of lines) {
      try {
        return JSON.parse(line);
      } catch {
        continue;
      }
    }
  }

  throw new Error("Failed to parse yt-dlp JSON output");
}

async function resolveTikTokUrl(url, requestId, sessionId) {
  const trimmed = String(url || "").trim();
  const isShortUrl = /tiktok\.com\/(t\/|@[^/]+\/video\/|vm\.tiktok\.com)/i.test(trimmed);
  if (!isShortUrl) return trimmed;

  const proxy = getProxyUrl(sessionId);
  const args = [
    "-Ls",
    "-o",
    "/dev/null",
    "-A",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "-w",
    "%{url_effective}",
  ];

  if (proxy) {
    args.push("--proxy", proxy);
  }

  args.push(trimmed);

  try {
    const { stdout } = await execFileAsync("curl", args, {
      timeout: 30000,
      maxBuffer: 1024 * 1024,
    });

    const resolved = String(stdout || "").trim();
    if (isHttpUrl(resolved) && !resolved.includes("/notfound")) {
      if (resolved !== trimmed) {
        console.log(`${getLogPrefix(requestId)} tiktok_url_resolved from=${trimmed} to=${resolved}`);
      }
      return resolved;
    }
  } catch (error) {
    console.warn(
      `${getLogPrefix(requestId)} tiktok_url_resolve_failed ${formatExecError("curl", error)}`
    );
  }

  // Fallback: resolve canonical webpage URL via yt-dlp (works without curl).
  try {
    const binary = process.env.YT_DLP_PATH || "yt-dlp";
    const args = ["--print", "webpage_url", "--skip-download", "--no-warnings"];
    if (proxy) args.push("--proxy", proxy);
    args.push(trimmed);

    const { stdout } = await execFileAsync(binary, args, {
      timeout: 30000,
      maxBuffer: 1024 * 1024,
    });

    const resolved = String(stdout || "")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => isHttpUrl(line))
      .find((line) => !line.includes("/notfound"));

    if (resolved) {
      if (resolved !== trimmed) {
        console.log(
          `${getLogPrefix(requestId)} tiktok_url_resolved extractor=yt-dlp from=${trimmed} to=${resolved}`
        );
      }
      return resolved;
    }
  } catch (error) {
    console.warn(
      `${getLogPrefix(requestId)} tiktok_url_resolve_failed ${formatExecError("yt-dlp", error)}`
    );
  }

  return trimmed;
}

async function runGalleryDl(url, platform, requestId, sessionId) {
  const binary = process.env.GALLERY_DL_PATH || "gallery-dl";
  const proxy = getProxyUrl(sessionId);
  const cookiesFile = resolveInstagramCookiesFile();
  const startedAt = Date.now();

  const args = [];
  if (proxy) args.push("--proxy", proxy);
  if (cookiesFile && platform === "instagram") args.push("--cookies", cookiesFile);
  args.push("-g", url);

  console.log(
    `${getLogPrefix(requestId)} gallery-dl start binary=${binary} proxy=${proxy ? "yes" : "no"} cookies=${cookiesFile ? "yes" : "no"} session=${sessionId || "none"}`
  );

  try {
    const { stdout } = await execFileAsync(binary, args, {
      timeout: 120000,
      maxBuffer: 10 * 1024 * 1024,
    });

    const rawUrls = dedupe(
      String(stdout)
        .split("\n")
        .map((line) => normalizeEscapedUrl(line))
        .filter((line) => isHttpUrl(line))
    );

    const mediaUrls = normalizeAndFilterMediaUrls(rawUrls, platform);

    console.log(
      `${getLogPrefix(requestId)} gallery-dl success elapsedMs=${Date.now() - startedAt} media=${mediaUrls.length} raw=${rawUrls.length}`
    );

    return mediaUrls;
  } catch (error) {
    const formatted = formatExecError("gallery-dl", error);
    console.error(
      `${getLogPrefix(requestId)} gallery-dl failed elapsedMs=${Date.now() - startedAt} ${formatted}`
    );
    throw new Error(formatted);
  }
}

async function runYtDlp(url, platform, requestId, sessionId) {
  const binary = process.env.YT_DLP_PATH || "yt-dlp";
  const proxy = getProxyUrl(sessionId);
  const cookiesFile = resolveInstagramCookiesFile();
  const startedAt = Date.now();

  const args = ["--dump-single-json", "--skip-download", "--no-warnings", "--no-call-home", "--ignore-errors"];
  if (proxy) args.push("--proxy", proxy);
  if (cookiesFile && platform === "instagram") args.push("--cookies", cookiesFile);

  const instagramSessionId = process.env.INSTAGRAM_SESSIONID;
  if (instagramSessionId && platform === "instagram") {
    args.push("--add-header", `Cookie: sessionid=${instagramSessionId}`);
  }

  args.push(url);

  console.log(
    `${getLogPrefix(requestId)} yt-dlp start binary=${binary} proxy=${proxy ? "yes" : "no"} cookies=${cookiesFile ? "yes" : "no"} session=${sessionId || "none"}`
  );

  try {
    const { stdout } = await execFileAsync(binary, args, {
      timeout: 120000,
      maxBuffer: 10 * 1024 * 1024,
    });

    const payload = parseYtDlpStdout(stdout);
    const entries = Array.isArray(payload.entries)
      ? payload.entries.filter((item) => item && typeof item === "object")
      : [];

    const rawUrls =
      entries.length > 0
        ? dedupe(entries.flatMap((entry) => extractUrlsFromEntry(entry)))
        : extractUrlsFromEntry(payload);

    const mediaUrls = normalizeAndFilterMediaUrls(rawUrls, platform);

    console.log(
      `${getLogPrefix(requestId)} yt-dlp success elapsedMs=${Date.now() - startedAt} media=${mediaUrls.length} raw=${rawUrls.length}`
    );

    return {
      title: typeof payload.title === "string" ? payload.title : null,
      description: typeof payload.description === "string" ? payload.description : null,
      mediaUrls,
      extractor: "yt-dlp",
      attempts: 1,
    };
  } catch (error) {
    const formatted = formatExecError("yt-dlp", error);
    console.error(
      `${getLogPrefix(requestId)} yt-dlp failed elapsedMs=${Date.now() - startedAt} ${formatted}`
    );
    throw new Error(formatted);
  }
}

app.get("/", (req, res) => {
  res.json({
    status: "ok",
    service: "social-extractor",
    timestamp: new Date().toISOString(),
  });
});

app.get("/health", (req, res) => {
  const cookiesPath = resolveInstagramCookiesFile();
  res.json({
    status: "ok",
    hasCookiesFile: Boolean(cookiesPath),
    hasProxyConfig: Boolean(getProxyUrl()),
    timestamp: new Date().toISOString(),
  });
});

app.post("/api/extract-social-post", async (req, res) => {
  const requestId = randomUUID().slice(0, 8);
  const logPrefix = getLogPrefix(requestId);
  if (!authorizeRequest(req, res, requestId)) return;

  const { url, platform, sessionId } = req.body || {};
  if (typeof url !== "string" || !url.trim()) {
    console.warn(`${logPrefix} validation_failed missing url`);
    return res.status(400).json({ error: "url is required", requestId });
  }

  const resolvedPlatform =
    typeof platform === "string" ? platform : extractPlatform(url);

  if (resolvedPlatform !== "instagram" && resolvedPlatform !== "tiktok") {
    console.warn(`${logPrefix} unsupported_platform resolved=${resolvedPlatform}`);
    return res.status(400).json({
      error: "Unsupported platform for extractor endpoint",
      details: `resolved platform: ${resolvedPlatform}`,
      requestId,
    });
  }

  const safeSessionId = typeof sessionId === "string" && sessionId.trim() ? sessionId : undefined;
  const extractionUrl =
    resolvedPlatform === "tiktok"
      ? await resolveTikTokUrl(url, requestId, safeSessionId)
      : String(url).trim();

  console.log(
    `${logPrefix} start platform=${resolvedPlatform} session=${safeSessionId || "none"} proxy=${getProxyUrl(safeSessionId) ? "yes" : "no"} cookies=${resolveInstagramCookiesFile() ? "yes" : "no"} url=${extractionUrl}`
  );

  const errors = [];

  try {
    const galleryUrls = await runGalleryDl(extractionUrl, resolvedPlatform, requestId, safeSessionId);
    if (galleryUrls.length > 0) {
      console.log(`${logPrefix} success extractor=gallery-dl media=${galleryUrls.length}`);
      return res.json({
        title: null,
        description: null,
        mediaUrls: galleryUrls,
        extractor: "gallery-dl",
        attempts: 1,
        requestId,
      });
    }
    errors.push("gallery-dl returned no media");
  } catch (error) {
    const message = error instanceof Error ? error.message : "gallery-dl extraction failed";
    errors.push(`gallery-dl: ${message}`);
    console.error(`${logPrefix} gallery-dl error message=${message}`);
  }

  try {
    const ytData = await runYtDlp(extractionUrl, resolvedPlatform, requestId, safeSessionId);
    if (ytData.mediaUrls.length > 0) {
      console.log(`${logPrefix} success extractor=yt-dlp media=${ytData.mediaUrls.length}`);
      return res.json({ ...ytData, requestId });
    }
    errors.push("yt-dlp returned no media");
  } catch (error) {
    const message = error instanceof Error ? error.message : "yt-dlp extraction failed";
    errors.push(`yt-dlp: ${message}`);
    console.error(`${logPrefix} yt-dlp error message=${message}`);
  }

  console.error(`${logPrefix} failed all_extractors errors=${errors.length}`);
  return res.status(422).json({
    error: "Failed to extract media from social post",
    details: errors,
    requestId,
  });
});

app.post("/api/extract-video-frames", async (req, res) => {
  const requestId = randomUUID().slice(0, 8);
  const logPrefix = getLogPrefix(requestId);
  if (!authorizeRequest(req, res, requestId)) return;

  const { url, platform, sessionId, frameCount, frameWidth, includeTranscript, transcriptMaxSeconds } = req.body || {};

  if (typeof url !== "string" || !url.trim()) {
    console.warn(`${logPrefix} validation_failed missing url`);
    return res.status(400).json({ error: "url is required", requestId });
  }

  const resolvedPlatform =
    typeof platform === "string" && platform.trim() ? platform.trim().toLowerCase() : extractPlatform(url);

  if (resolvedPlatform !== "instagram" && resolvedPlatform !== "tiktok") {
    console.warn(`${logPrefix} unsupported_platform resolved=${resolvedPlatform}`);
    return res.status(400).json({
      error: "Unsupported platform for video frame endpoint",
      details: `resolved platform: ${resolvedPlatform}`,
      requestId,
    });
  }

  const safeSessionId = typeof sessionId === "string" && sessionId.trim() ? sessionId : undefined;
  const extractionUrl =
    resolvedPlatform === "tiktok"
      ? await resolveTikTokUrl(url, requestId, safeSessionId)
      : String(url).trim();

  const normalizedFrameCount = toBoundedInteger(
    frameCount,
    DEFAULT_FRAME_COUNT,
    MIN_FRAME_COUNT,
    MAX_FRAME_COUNT
  );
  const normalizedFrameWidth = toBoundedInteger(
    frameWidth,
    DEFAULT_FRAME_WIDTH,
    MIN_FRAME_WIDTH,
    MAX_FRAME_WIDTH
  );
  const shouldIncludeTranscript = toBoolean(includeTranscript, DEFAULT_TRANSCRIPT_ENABLED);
  const normalizedTranscriptMaxSeconds = toBoundedInteger(
    transcriptMaxSeconds,
    DEFAULT_TRANSCRIPT_MAX_SECONDS,
    MIN_TRANSCRIPT_MAX_SECONDS,
    MAX_TRANSCRIPT_MAX_SECONDS
  );

  console.log(
    `${logPrefix} frame_extract_start platform=${resolvedPlatform} session=${safeSessionId || "none"} frameCount=${normalizedFrameCount} frameWidth=${normalizedFrameWidth} url=${extractionUrl}`
  );

  const errors = [];
  let title = null;
  let description = null;
  let sourceExtractor = null;
  const ytCandidateUrls = [];
  const galleryCandidateUrls = [];

  try {
    const ytResult = await runYtDlpForVideoStream(
      extractionUrl,
      resolvedPlatform,
      requestId,
      safeSessionId
    );

    if (ytResult.candidateVideoUrls.length > 0) {
      ytCandidateUrls.push(...ytResult.candidateVideoUrls);
      title = ytResult.title;
      description = ytResult.description;
    } else {
      errors.push("yt-dlp returned no candidate video stream URLs");
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "yt-dlp video extraction failed";
    errors.push(`yt-dlp-video: ${message}`);
    console.error(`${logPrefix} yt-dlp-video error message=${message}`);
  }

  if (ytCandidateUrls.length > 0) {
    const ytAttempt = await extractFramesFromCandidates(
      ytCandidateUrls,
      normalizedFrameCount,
      normalizedFrameWidth,
      requestId
    );

    errors.push(...ytAttempt.errors.map((item) => `yt-dlp-video: ${item}`));

    if (ytAttempt.videoUrl && ytAttempt.frameResult) {
      sourceExtractor = "yt-dlp";

      console.log(
        `${logPrefix} frame_extract_success extractor=${sourceExtractor} extracted=${ytAttempt.frameResult.extractedFrameCount}/${ytAttempt.frameResult.requestedFrameCount}`
      );

      const transcript = shouldIncludeTranscript
        ? await buildVideoTranscript(ytAttempt.videoUrl, requestId, normalizedTranscriptMaxSeconds)
        : {
            available: false,
            error: "transcript disabled by request",
          };

      return res.json({
        requestId,
        extractor: sourceExtractor,
        platform: resolvedPlatform,
        sourceUrl: extractionUrl,
        videoUrl: ytAttempt.videoUrl,
        title,
        description,
        durationSeconds: ytAttempt.frameResult.durationSeconds,
        requestedFrameCount: ytAttempt.frameResult.requestedFrameCount,
        extractedFrameCount: ytAttempt.frameResult.extractedFrameCount,
        frameWidth: normalizedFrameWidth,
        frames: ytAttempt.frameResult.frames,
        transcript,
      });
    }
  }

  try {
    const galleryResult = await runGalleryDlForVideoStream(
      extractionUrl,
      resolvedPlatform,
      requestId,
      safeSessionId
    );

    if (galleryResult.candidateVideoUrls.length > 0) {
      galleryCandidateUrls.push(...galleryResult.candidateVideoUrls);
    } else {
      errors.push("gallery-dl returned no candidate video stream URLs");
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "gallery-dl video extraction failed";
    errors.push(`gallery-dl-video: ${message}`);
    console.error(`${logPrefix} gallery-dl-video error message=${message}`);
  }

  if (galleryCandidateUrls.length > 0) {
    const galleryAttempt = await extractFramesFromCandidates(
      galleryCandidateUrls,
      normalizedFrameCount,
      normalizedFrameWidth,
      requestId
    );

    errors.push(...galleryAttempt.errors.map((item) => `gallery-dl-video: ${item}`));

    if (galleryAttempt.videoUrl && galleryAttempt.frameResult) {
      sourceExtractor = "gallery-dl";

      console.log(
        `${logPrefix} frame_extract_success extractor=${sourceExtractor} extracted=${galleryAttempt.frameResult.extractedFrameCount}/${galleryAttempt.frameResult.requestedFrameCount}`
      );

      const transcript = shouldIncludeTranscript
        ? await buildVideoTranscript(galleryAttempt.videoUrl, requestId, normalizedTranscriptMaxSeconds)
        : {
            available: false,
            error: "transcript disabled by request",
          };

      return res.json({
        requestId,
        extractor: sourceExtractor,
        platform: resolvedPlatform,
        sourceUrl: extractionUrl,
        videoUrl: galleryAttempt.videoUrl,
        title,
        description,
        durationSeconds: galleryAttempt.frameResult.durationSeconds,
        requestedFrameCount: galleryAttempt.frameResult.requestedFrameCount,
        extractedFrameCount: galleryAttempt.frameResult.extractedFrameCount,
        frameWidth: normalizedFrameWidth,
        frames: galleryAttempt.frameResult.frames,
        transcript,
      });
    }
  }

  console.error(`${logPrefix} frame_extract_failed no_working_video_stream errors=${errors.length}`);
  return res.status(422).json({
    error: "Frame extraction failed",
    details: errors,
    requestId,
  });
});

async function logRuntimeDiagnostics() {
  const checks = [
    { label: "yt-dlp", cmd: process.env.YT_DLP_PATH || "yt-dlp", args: ["--version"] },
    { label: "gallery-dl", cmd: process.env.GALLERY_DL_PATH || "gallery-dl", args: ["--version"] },
    { label: "python3", cmd: "python3", args: ["--version"] },
    { label: "ffmpeg", cmd: "ffmpeg", args: ["-version"] },
    { label: "ffprobe", cmd: "ffprobe", args: ["-version"] },
  ];

  console.log(
    `[extractor-runtime] cwd=${process.cwd()} cookies=${resolveInstagramCookiesFile() ? "yes" : "no"} proxy=${getProxyUrl() ? "yes" : "no"}`
  );

  for (const check of checks) {
    try {
      const { stdout, stderr } = await execFileAsync(check.cmd, check.args, {
        timeout: 15000,
        maxBuffer: 1024 * 1024,
      });
      console.log(`[extractor-runtime] ${check.label}: ok ${toSingleLine(stdout || stderr || "ok")}`);
    } catch (error) {
      console.warn(`[extractor-runtime] ${check.label}: missing ${formatExecError(check.label, error)}`);
    }
  }
}

app.listen(port, "0.0.0.0", () => {
  console.log(`[extractor-runtime] service listening port=${port}`);
  void logRuntimeDiagnostics();
});
