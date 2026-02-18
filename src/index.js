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

app.use(cors());
app.use(express.json({ limit: "1mb" }));

function isHttpUrl(value) {
  return typeof value === "string" && /^https?:\/\//i.test(value);
}

function dedupe(urls) {
  return Array.from(new Set(urls.filter(Boolean)));
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

function getLargestThumbnail(entry) {
  if (!Array.isArray(entry.thumbnails)) return null;

  const candidates = entry.thumbnails
    .map((item) => (item && typeof item === "object" ? item : null))
    .filter(Boolean)
    .map((thumb) => ({
      url: isHttpUrl(thumb.url) ? thumb.url : null,
      area:
        typeof thumb.width === "number" && typeof thumb.height === "number"
          ? thumb.width * thumb.height
          : 0,
    }))
    .filter((thumb) => thumb.url)
    .sort((a, b) => b.area - a.area);

  return candidates[0] ? candidates[0].url : null;
}

function extractUrlsFromEntry(entry) {
  const urls = [];
  if (isHttpUrl(entry.url)) urls.push(entry.url);
  if (isHttpUrl(entry.thumbnail)) urls.push(entry.thumbnail);

  const largestThumb = getLargestThumbnail(entry);
  if (largestThumb) urls.push(largestThumb);

  if (Array.isArray(entry.formats)) {
    for (const format of entry.formats) {
      if (!format || typeof format !== "object") continue;
      if (isHttpUrl(format.url)) urls.push(format.url);
    }
  }

  return dedupe(urls);
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

    const mediaUrls = dedupe(
      String(stdout)
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => isHttpUrl(line))
    );

    console.log(
      `${getLogPrefix(requestId)} gallery-dl success elapsedMs=${Date.now() - startedAt} media=${mediaUrls.length}`
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

    const mediaUrls =
      entries.length > 0
        ? dedupe(entries.flatMap((entry) => extractUrlsFromEntry(entry)))
        : extractUrlsFromEntry(payload);

    console.log(
      `${getLogPrefix(requestId)} yt-dlp success elapsedMs=${Date.now() - startedAt} media=${mediaUrls.length}`
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
  const requiredToken = process.env.SOCIAL_EXTRACTOR_API_TOKEN || process.env.EXTRACTOR_API_TOKEN;
  const providedToken = getBearerToken(req.headers.authorization);

  if (requiredToken && providedToken !== requiredToken) {
    console.warn(`${logPrefix} unauthorized token mismatch`);
    return res.status(401).json({
      error: "Unauthorized",
      details: "Missing or invalid extractor API token",
      requestId,
    });
  }

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

  console.log(
    `${logPrefix} start platform=${resolvedPlatform} session=${safeSessionId || "none"} proxy=${getProxyUrl(safeSessionId) ? "yes" : "no"} cookies=${resolveInstagramCookiesFile() ? "yes" : "no"}`
  );

  const errors = [];

  try {
    const galleryUrls = await runGalleryDl(url, resolvedPlatform, requestId, safeSessionId);
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
    const ytData = await runYtDlp(url, resolvedPlatform, requestId, safeSessionId);
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

async function logRuntimeDiagnostics() {
  const checks = [
    { label: "yt-dlp", cmd: process.env.YT_DLP_PATH || "yt-dlp", args: ["--version"] },
    { label: "gallery-dl", cmd: process.env.GALLERY_DL_PATH || "gallery-dl", args: ["--version"] },
    { label: "python3", cmd: "python3", args: ["--version"] },
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
