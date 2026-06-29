const { randomUUID } = require("node:crypto");
const { promises: fs } = require("node:fs");
const path = require("node:path");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

const OPENAI_BASE_URL = "https://api.openai.com/v1";
const SCRIPT_MODEL = "gpt-5.5";
const IMAGE_MODEL = "gpt-image-2";
const IMAGE_SIZE = "1024x1536";
const IMAGE_QUALITY = "medium";

const DEFAULT_REFERENCE_IMAGE_PATHS = [
  path.resolve(process.cwd(), "assets/muslimah-carousel/hook-reference.png"),
  path.resolve(process.cwd(), "assets/muslimah-carousel/chat-reference.png"),
];

const HOOK_BACKGROUNDS = [
  "Quran",
  "pink satin",
  "flowers",
  "prayer mat",
  "hijab",
  "tea",
  "coffee",
  "iPhone",
  "journal",
  "window light",
  "desk",
  "bed",
  "morning sunlight",
];

const ROTATABLE_FEATURES = [
  "Prayer",
  "Ghusl",
  "Hayd",
  "Istihada",
  "Spotting",
  "Fasting",
  "Nutrition tracking",
  "Workout tracking",
  "Skincare tracking",
  "Hydration",
  "Sleep",
  "Mood",
  "Energy",
  "Symptom logging",
  "Pregnancy insights",
  "Personalized cycle insights",
];

const SCRIPT_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: [
    "brand",
    "hook",
    "subtitle",
    "hookBackground",
    "freshTalkingPoints",
    "selectedFeatures",
    "slideOrder",
    "caption",
    "slides",
  ],
  properties: {
    brand: { type: "string", enum: ["muslimah.health"] },
    hook: { type: "string" },
    subtitle: { type: "string" },
    hookBackground: { type: "string" },
    freshTalkingPoints: { type: "array", minItems: 2, maxItems: 4, items: { type: "string" } },
    selectedFeatures: { type: "array", minItems: 6, maxItems: 10, items: { type: "string" } },
    slideOrder: { type: "array", minItems: 10, maxItems: 10, items: { type: "integer", minimum: 1, maximum: 10 } },
    caption: { type: "string" },
    slides: {
      type: "array",
      minItems: 10,
      maxItems: 10,
      items: {
        type: "object",
        additionalProperties: false,
        required: ["slideNumber", "slideType", "visualNotes", "messages", "hookText", "subtitle", "appScreenState"],
        properties: {
          slideNumber: { type: "integer", minimum: 1, maximum: 10 },
          slideType: { type: "string", enum: ["hook", "chat", "app_reveal", "cta"] },
          visualNotes: { type: "string" },
          messages: {
            type: "array",
            maxItems: 5,
            items: {
              type: "object",
              additionalProperties: false,
              required: ["speaker", "text", "timestamp"],
              properties: {
                speaker: { type: "string", enum: ["older_sister", "user"] },
                text: { type: "string" },
                timestamp: { type: "string" },
              },
            },
          },
          hookText: { type: "string" },
          subtitle: { type: "string" },
          appScreenState: { type: "string" },
        },
      },
    },
  },
};

function asNonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function asBoolean(value, fallback) {
  return typeof value === "boolean" ? value : fallback;
}

function asStringArray(value) {
  if (!Array.isArray(value)) return [];
  return value.filter((item) => typeof item === "string").map((item) => item.trim()).filter(Boolean);
}

function safeUrlLabel(value) {
  const url = asNonEmptyString(value);
  if (!url) return "missing";
  try {
    const parsed = new URL(url);
    return `${parsed.origin}${parsed.pathname}`;
  } catch {
    return "invalid-url";
  }
}

function logDebug(requestId, message, fields = {}) {
  const meta = Object.entries(fields)
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .map(([key, value]) => `${key}=${String(value)}`)
    .join(" ");
  console.log(`[${requestId || "no-request"}] muslimah-carousel ${message}${meta ? ` ${meta}` : ""}`);
}

function logWarn(requestId, message, fields = {}) {
  const meta = Object.entries(fields)
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .map(([key, value]) => `${key}=${String(value)}`)
    .join(" ");
  console.warn(`[${requestId || "no-request"}] muslimah-carousel ${message}${meta ? ` ${meta}` : ""}`);
}

function logError(requestId, message, fields = {}) {
  const meta = Object.entries(fields)
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .map(([key, value]) => `${key}=${String(value)}`)
    .join(" ");
  console.error(`[${requestId || "no-request"}] muslimah-carousel ${message}${meta ? ` ${meta}` : ""}`);
}

function isScript(value) {
  return Boolean(value && typeof value === "object" && value.brand === "muslimah.health" && Array.isArray(value.slides));
}

function compactText(value, fallback) {
  if (typeof value !== "string") return fallback;
  const cleaned = value.replace(/\s+/g, " ").trim();
  return cleaned || fallback;
}

function cleanArray(value, fallback, maxItems) {
  if (!Array.isArray(value)) return fallback;
  const seen = new Set();
  const items = value
    .filter((item) => typeof item === "string")
    .map((item) => item.replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .filter((item) => {
      const key = item.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .slice(0, maxItems);
  return items.length > 0 ? items : fallback;
}

function pickHookBackground(previousHookBackground) {
  const options = HOOK_BACKGROUNDS.filter(
    (background) => background.toLowerCase() !== String(previousHookBackground || "").toLowerCase()
  );
  return options[Math.floor(Math.random() * options.length)] || "pink satin";
}

function pickFeatureSeed(previousFeatures = []) {
  const overused = new Set(previousFeatures.map((feature) => feature.toLowerCase()));
  const fresh = ROTATABLE_FEATURES.filter((feature) => !overused.has(feature.toLowerCase()));
  const source = fresh.length >= 8 ? fresh : ROTATABLE_FEATURES;
  return [...source].sort(() => Math.random() - 0.5).slice(0, 8);
}

function normalizeScript(raw, fallbackBackground) {
  const slides = Array.isArray(raw?.slides) ? raw.slides.slice(0, 10) : [];
  const normalizedSlides = Array.from({ length: 10 }, (_, index) => {
    const slide = slides[index] || {};
    const slideNumber = index + 1;
    const slideType = slideNumber === 1 ? "hook" : slideNumber === 10 ? "cta" : slideNumber === 9 ? "app_reveal" : "chat";

    return {
      slideNumber,
      slideType,
      visualNotes: compactText(slide.visualNotes, "Match the provided reference carousel style exactly."),
      messages: Array.isArray(slide.messages)
        ? slide.messages
          .slice(0, 5)
          .map((message) => ({
            speaker: message?.speaker === "older_sister" ? "older_sister" : "user",
            text: compactText(message?.text, ""),
            timestamp: compactText(message?.timestamp, `10:${42 + slideNumber} AM`),
          }))
          .filter((message) => message.text.length > 0)
        : [],
      hookText: slideNumber === 1 ? "Why I stopped using Flo as a Muslim woman." : compactText(slide.hookText, ""),
      subtitle: slideNumber === 1 ? "It tracked my cycle... not my worship." : compactText(slide.subtitle, ""),
      appScreenState: compactText(slide.appScreenState, ""),
    };
  });

  return {
    brand: "muslimah.health",
    hook: "Why I stopped using Flo as a Muslim woman.",
    subtitle: "It tracked my cycle... not my worship.",
    hookBackground: compactText(raw?.hookBackground, fallbackBackground),
    freshTalkingPoints: cleanArray(raw?.freshTalkingPoints, ["Ghusl clarity", "Skincare tracking"], 4),
    selectedFeatures: cleanArray(raw?.selectedFeatures, ["Prayer", "Ghusl", "Nutrition tracking", "Workout tracking", "Skincare tracking"], 10),
    slideOrder: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    caption: compactText(
      raw?.caption,
      "Basic period apps can track a cycle. muslimah.health helps Muslim women connect cycle, worship, nutrition, workouts, skincare, and personal insights in one place."
    ),
    slides: normalizedSlides,
  };
}

async function openAIRequest(endpoint, init, context = {}) {
  const apiKey = asNonEmptyString(process.env.OPENAI_API_KEY);
  if (!apiKey) throw new Error("Missing OPENAI_API_KEY.");

  const startedAt = Date.now();
  let heartbeatCount = 0;
  let heartbeatInFlight = false;
  const heartbeatIntervalMs = Number(process.env.MUSLIMAH_CAROUSEL_OPENAI_HEARTBEAT_MS || 30000);
  const heartbeatTimer =
    typeof context.onHeartbeat === "function" && heartbeatIntervalMs > 0
      ? setInterval(() => {
          if (heartbeatInFlight) return;
          heartbeatInFlight = true;
          heartbeatCount += 1;
          const elapsedMs = Date.now() - startedAt;
          logDebug(context.requestId, "openai_request_waiting", {
            job: context.jobId,
            endpoint,
            stage: context.stage,
            model: context.model,
            elapsedMs,
            heartbeat: heartbeatCount,
          });
          Promise.resolve(context.onHeartbeat({ elapsedMs, heartbeatCount }))
            .catch((error) => {
              logWarn(context.requestId, "openai_heartbeat_failed", {
                job: context.jobId,
                endpoint,
                stage: context.stage,
                error: error instanceof Error ? error.message : "unknown",
              });
            })
            .finally(() => {
              heartbeatInFlight = false;
            });
        }, heartbeatIntervalMs)
      : null;
  logDebug(context.requestId, "openai_request_start", {
    job: context.jobId,
    endpoint,
    stage: context.stage,
    model: context.model,
  });

  try {
    const response = await fetch(`${OPENAI_BASE_URL}${endpoint}`, {
      ...init,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        ...(init.headers || {}),
      },
    });
    const raw = await response.text();
    let payload;
    try {
      payload = raw ? JSON.parse(raw) : {};
    } catch {
      payload = {};
    }
    const elapsedMs = Date.now() - startedAt;
    logDebug(context.requestId, "openai_request_end", {
      job: context.jobId,
      endpoint,
      stage: context.stage,
      model: context.model,
      status: response.status,
      elapsedMs,
    });

    if (!response.ok || payload?.error) {
      logError(context.requestId, "openai_request_failed", {
        job: context.jobId,
        endpoint,
        stage: context.stage,
        model: context.model,
        status: response.status,
        elapsedMs,
        error: payload?.error?.message || raw.slice(0, 180),
      });
      throw new Error(payload?.error?.message || `OpenAI request failed with status ${response.status}.`);
    }

    return payload;
  } finally {
    if (heartbeatTimer) clearInterval(heartbeatTimer);
  }
}

function extractOutputText(payload) {
  if (typeof payload?.output_text === "string" && payload.output_text.trim()) return payload.output_text.trim();
  const text = payload?.output
    ?.flatMap((item) => item.content || [])
    .map((content) => content.text || "")
    .join("")
    .trim();
  if (!text) throw new Error("OpenAI response did not include output text.");
  return text;
}

function buildScriptPrompt({ previousHookBackground, previousFeatures, focus }) {
  const hookBackground = pickHookBackground(previousHookBackground);
  const featureSeed = pickFeatureSeed(previousFeatures);

  return `Create today's 10-slide Instagram carousel JSON for muslimah.health.

Mandatory hook:
Why I stopped using Flo as a Muslim woman.

Mandatory subtitle:
It tracked my cycle... not my worship.

Chosen hook background for today:
${hookBackground}

Fresh feature seed for today:
${featureSeed.join(", ")}

Optional content focus from operator:
${focus?.trim() || "None"}

Conversation sequence, in this exact order:
1. Hook.
2. Older sister: "I thought you liked Flo?"
3. User: "It tracked my cycle..."
4. User or older sister: "But something was missing."
5. First reveal worship pain: prayer, ghusl, spotting, hayd, istihada, purity.
6. Then reveal health tracking: nutrition, workout, skincare, sleep, hydration, mood, energy.
7. Reveal muslimah.health.
8. Explain why it solves health and worship needs together.
9. Show realistic iPhone mockup with muslimah.health app UI.
10. Soft emotional CTA ending.

Positioning:
- Do not attack Flo.
- Say basic period apps mainly track the cycle.
- muslimah.health helps Muslim women manage cycle, worship, nutrition, workouts, skincare, and personalized insights together.

Writing style:
- Write like a real Muslim woman texting her older sister.
- Emotional, natural, short chat bubbles.
- No corporate tone and no obvious advertising.
- Never generate images in this step.
- Use at least 2 fresh talking points from the feature seed that are not generic.

Slide content rules:
- Exactly 10 slides.
- Slide 1 type hook, slides 2-8 type chat, slide 9 type app_reveal, slide 10 type cta.
- Chat slides should contain 2-5 messages each, short enough to render inside a phone screenshot.
- Include visualNotes for each slide for the image step.
- Keep all visible text in English.
- Return JSON only.`;
}

async function generateScript({ scriptModel, previousHookBackground, previousFeatures, focus, requestId, jobId, onProgress }) {
  const fallbackBackground = pickHookBackground(previousHookBackground);
  const payload = await openAIRequest("/responses", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: scriptModel || SCRIPT_MODEL,
      input: [
        {
          role: "system",
          content: [{ type: "input_text", text: "You generate structured Instagram carousel scripts for muslimah.health. You never generate images. You preserve the requested JSON schema exactly." }],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: buildScriptPrompt({ previousHookBackground, previousFeatures, focus }) }],
        },
      ],
      reasoning: { effort: "medium" },
      store: false,
      text: {
        verbosity: "low",
        format: {
          type: "json_schema",
          name: "muslimah_health_carousel_script",
          strict: true,
          schema: SCRIPT_SCHEMA,
        },
      },
    }),
  }, {
    requestId,
    jobId,
    stage: "script_generation",
    model: scriptModel || SCRIPT_MODEL,
    onHeartbeat: ({ elapsedMs }) =>
      onProgress?.({
        stage: "script_generation_waiting",
        message: `Still waiting for ${scriptModel || SCRIPT_MODEL} script response (${Math.round(elapsedMs / 1000)}s).`,
        progress: 10,
        elapsedMs,
      }),
  });

  return normalizeScript(JSON.parse(extractOutputText(payload)), fallbackBackground);
}

function renderChatMessages(messages) {
  if (!messages.length) return "No chat messages on this slide.";
  return messages
    .map((message) => `${message.speaker === "older_sister" ? "Incoming white bubble" : "Outgoing light green bubble"}: "${message.text}" (${message.timestamp})`)
    .join("\n");
}

function buildSlidePrompt(script, slide) {
  if (slide.slideType === "hook") {
    return `Generate slide 1 of a 10-slide 9:16 Instagram carousel for muslimah.health.

Use the attached hook-slide reference as the primary style reference.

Render this exact text:
"Why I stopped using
Flo as a Muslim woman"

Render this exact subtitle:
"it tracked my cycle... not my worship"

Typography:
- Match the reference hook typography as closely as possible: large bold rounded white sans-serif letters, medium black outline/stroke, centered, strong drop shadow only if needed for readability.
- Subtitle is smaller white rounded sans-serif with black outline.
- Add a bright pink underline only under "not my worship".
- No username, no slide number, no watermark.

Background:
- Use ${script.hookBackground}.
- Premium warm pink Muslim lifestyle flat-lay, soft sunlight, Quran/prayer-safe modest styling when relevant.
- Keep exact 9:16 portrait composition with text centered in the middle-lower third like the reference.
- No extra words.`;
  }

  if (slide.slideType === "app_reveal") {
    return `Generate slide ${slide.slideNumber} of a 10-slide 9:16 Instagram carousel for muslimah.health.

Use the attached chat-slide reference for header, wallpaper, colors, rounded bubbles, input bar, footer, and Apple-style typography.

Create a premium realistic iPhone mockup inside the phone screenshot. Show the muslimah.health app UI in a soft pink Apple-quality interface.
App screen state to show: ${slide.appScreenState || "cycle dashboard with worship, nutrition, workout, skincare, and personalized insights modules"}.

Chat screenshot details:
- Header text: Big Sis 💗
- Status: online
- Pink back/video/phone/menu icons
- Soft cream wallpaper with tiny floral Islamic pattern.
- Bottom input bar.
- Footer text exactly: Muslimah.health 💗
- Preserve the same spacing, bubble style, timestamps, checkmarks, and proportions as the reference.

Messages:
${renderChatMessages(slide.messages)}

Do not invent random app UI. Keep the UI realistic: cycle day card, prayer/purity status, ghusl reminder, nutrition, workout, skincare, and insights tiles only.`;
  }

  return `Generate slide ${slide.slideNumber} of a 10-slide 9:16 Instagram carousel for muslimah.health.

Use the attached chat-slide reference as the primary visual style reference.

This must look like a real premium WhatsApp/iMessage-style phone screenshot, consistent with every other chat slide:
- Header: Big Sis 💗
- Status: online
- Pink back/video/phone/menu icons
- Wallpaper: soft cream with tiny floral Islamic pattern.
- Incoming bubbles: white.
- Outgoing bubbles: light green.
- Apple-style typography.
- Include timestamps and double checkmarks on outgoing messages.
- Include bottom input bar.
- Footer text exactly: Muslimah.health 💗
- Same colors, fonts, spacing, header, icons, wallpaper, bubble style, footer, and proportions as the reference.

Messages to render exactly:
${renderChatMessages(slide.messages)}

Slide visual notes:
${slide.visualNotes}

No username other than Big Sis 💗. No slide number. No extra marketing copy.`;
}

function inferMimeType(filePath) {
  const extension = path.extname(filePath).toLowerCase();
  if (extension === ".png") return "image/png";
  if (extension === ".webp") return "image/webp";
  return "image/jpeg";
}

async function appendImageFile(form, filePath) {
  const buffer = await fs.readFile(filePath);
  const blob = new Blob([buffer], { type: inferMimeType(filePath) });
  form.append("image[]", blob, path.basename(filePath));
}

function getReferenceImagePaths(inputPaths) {
  const envPaths = process.env.MUSLIMAH_CAROUSEL_REFERENCE_IMAGE_PATHS
    ?.split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  return inputPaths && inputPaths.length > 0 ? inputPaths : envPaths && envPaths.length > 0 ? envPaths : DEFAULT_REFERENCE_IMAGE_PATHS;
}

async function createImage({ model, prompt, referenceImagePaths, requestId, jobId, slideNumber, progressBase, onProgress }) {
  const references = getReferenceImagePaths(referenceImagePaths);
  const heartbeat = ({ elapsedMs }) =>
    onProgress?.({
      stage: "image_generation_waiting",
      message: `Still waiting for ${model} slide ${slideNumber} image response (${Math.round(elapsedMs / 1000)}s).`,
      slideNumber,
      progress: progressBase,
      elapsedMs,
    });
  logDebug(requestId, "image_request_prepare", {
    job: jobId,
    slide: slideNumber,
    model,
    referenceCount: references.length,
    mode: references.length === 0 ? "generation" : "edit",
    promptChars: prompt.length,
  });

  if (references.length === 0) {
    const payload = await openAIRequest("/images/generations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model,
        prompt,
        size: IMAGE_SIZE,
        quality: IMAGE_QUALITY,
        output_format: "png",
        n: 1,
      }),
    }, { requestId, jobId, stage: `image_generation_slide_${slideNumber}`, model, onHeartbeat: heartbeat });
    const base64 = payload?.data?.[0]?.b64_json;
    if (!base64) throw new Error("OpenAI image generation returned no image bytes.");
    return Buffer.from(base64, "base64");
  }

  const form = new FormData();
  form.append("model", model);
  form.append("prompt", prompt);
  form.append("size", IMAGE_SIZE);
  form.append("quality", IMAGE_QUALITY);
  form.append("output_format", "png");
  form.append("n", "1");

  for (const filePath of references) {
    logDebug(requestId, "image_reference_attach", {
      job: jobId,
      slide: slideNumber,
      file: path.basename(filePath),
    });
    await appendImageFile(form, filePath);
  }

  const payload = await openAIRequest(
    "/images/edits",
    { method: "POST", body: form },
    { requestId, jobId, stage: `image_edit_slide_${slideNumber}`, model, onHeartbeat: heartbeat }
  );
  const base64 = payload?.data?.[0]?.b64_json;
  if (!base64) throw new Error("OpenAI image edit returned no image bytes.");
  return Buffer.from(base64, "base64");
}

function createR2Client() {
  const accountId = process.env.CLOUDFLARE_R2_ACCOUNT_ID || "";
  const accessKeyId = process.env.CLOUDFLARE_R2_ACCESS_KEY_ID || "";
  const secretAccessKey = process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY || "";
  const bucket = process.env.CLOUDFLARE_R2_BUCKET_NAME || "";
  const publicUrl = (process.env.CLOUDFLARE_R2_PUBLIC_URL || "").replace(/\/+$/, "");

  if (!accountId || !accessKeyId || !secretAccessKey || !bucket || !publicUrl) {
    throw new Error("Missing Cloudflare R2 environment variables for carousel image upload.");
  }

  return {
    bucket,
    publicUrl,
    client: new S3Client({
      region: "auto",
      endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
      credentials: { accessKeyId, secretAccessKey },
    }),
  };
}

function encodeR2Key(key) {
  return key.split("/").filter(Boolean).map(encodeURIComponent).join("/");
}

function slugify(input) {
  return String(input || "flo-muslim-woman")
    .toLowerCase()
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 80) || "flo-muslim-woman";
}

async function uploadImageToR2(buffer, collectionId, hook, slideNumber, context = {}) {
  const r2 = createR2Client();
  const key = `muslimah-health-carousels/${collectionId}/${slugify(hook)}/${Date.now()}-${randomUUID()}-slide-${slideNumber}.png`;
  const startedAt = Date.now();
  logDebug(context.requestId, "r2_upload_start", {
    job: context.jobId,
    slide: slideNumber,
    bucket: r2.bucket,
    bytes: buffer.byteLength,
  });
  await r2.client.send(
    new PutObjectCommand({
      Bucket: r2.bucket,
      Key: key,
      Body: buffer,
      ContentType: "image/png",
    })
  );
  logDebug(context.requestId, "r2_upload_end", {
    job: context.jobId,
    slide: slideNumber,
    elapsedMs: Date.now() - startedAt,
    key,
  });
  return `${r2.publicUrl}/${encodeR2Key(key)}`;
}

async function generateImages({ script, imageModel, referenceImagePaths, collectionId, onProgress, requestId, jobId }) {
  const images = [];
  for (const slide of script.slides) {
    const prompt = buildSlidePrompt(script, slide);
    logDebug(requestId, "slide_start", {
      job: jobId,
      slide: slide.slideNumber,
      type: slide.slideType,
      totalSlides: script.slides.length,
    });
    await onProgress?.({
      stage: "image_generation_started",
      message: `Generating slide ${slide.slideNumber} ${slide.slideType} image with ${imageModel || IMAGE_MODEL}.`,
      slideNumber: slide.slideNumber,
      progress: 20 + ((slide.slideNumber - 1) / script.slides.length) * 65,
    });
    const imageStartedAt = Date.now();
    const imageBuffer = await createImage({
      model: imageModel || IMAGE_MODEL,
      prompt,
      referenceImagePaths,
      requestId,
      jobId,
      slideNumber: slide.slideNumber,
      progressBase: 20 + ((slide.slideNumber - 1) / script.slides.length) * 65,
      onProgress,
    });
    await onProgress?.({
      stage: "image_generated",
      message: `Slide ${slide.slideNumber} image generated. Uploading to R2.`,
      slideNumber: slide.slideNumber,
      progress: 22 + ((slide.slideNumber - 1) / script.slides.length) * 65,
      elapsedMs: Date.now() - imageStartedAt,
    });
    const uploadStartedAt = Date.now();
    const imageUrl = await uploadImageToR2(imageBuffer, collectionId, script.hook, slide.slideNumber, { requestId, jobId });
    images.push({
      slideNumber: slide.slideNumber,
      slideType: slide.slideType,
      imageUrl,
      prompt,
    });
    await onProgress?.({
      stage: "image_uploaded",
      message: `Slide ${slide.slideNumber} uploaded to R2.`,
      slideNumber: slide.slideNumber,
      progress: 20 + (slide.slideNumber / script.slides.length) * 65,
      elapsedMs: Date.now() - uploadStartedAt,
      details: {
        imageUrl,
        slideType: slide.slideType,
        prompt,
      },
    });
    logDebug(requestId, "slide_done", {
      job: jobId,
      slide: slide.slideNumber,
      imageBytes: imageBuffer.byteLength,
      imageUrlHost: safeUrlLabel(imageUrl),
    });
  }
  return images;
}

async function publishInstagram({ generation, publish, requestId, jobId }) {
  if (!publish) {
    logDebug(requestId, "instagram_publish_skipped", { job: jobId });
    return null;
  }

  const accessToken = asNonEmptyString(process.env.INSTAGRAM_GRAPH_ACCESS_TOKEN);
  const igUserId = asNonEmptyString(process.env.INSTAGRAM_GRAPH_USER_ID);
  if (!accessToken || !igUserId) {
    throw new Error("Instagram publishing is not configured on Render worker.");
  }

  const apiVersion = process.env.INSTAGRAM_GRAPH_API_VERSION || "v22.0";
  const imageUrls = generation.images.map((image) => image.imageUrl).slice(0, 10);
  logDebug(requestId, "instagram_publish_start", {
    job: jobId,
    apiVersion,
    imageCount: imageUrls.length,
  });

  async function graphPost(pathname, params) {
    const startedAt = Date.now();
    logDebug(requestId, "instagram_graph_start", { job: jobId, pathname });
    const body = new URLSearchParams({ ...params, access_token: accessToken });
    const response = await fetch(`https://graph.facebook.com/${apiVersion}/${pathname}`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
    });
    const payload = await response.json();
    logDebug(requestId, "instagram_graph_end", {
      job: jobId,
      pathname,
      status: response.status,
      elapsedMs: Date.now() - startedAt,
    });
    if (!response.ok || payload?.error) {
      throw new Error(payload?.error?.message || `Instagram Graph request failed (${response.status})`);
    }
    return payload;
  }

  const children = [];
  for (const imageUrl of imageUrls) {
    const child = await graphPost(`${igUserId}/media`, {
      image_url: imageUrl,
      is_carousel_item: "true",
    });
    if (!child.id) throw new Error("Instagram did not return carousel child container id.");
    children.push(child.id);
  }

  const carousel = await graphPost(`${igUserId}/media`, {
    media_type: "CAROUSEL",
    children: children.join(","),
    caption: generation.script.caption.slice(0, 2200),
  });
  if (!carousel.id) throw new Error("Instagram did not return carousel container id.");

  const published = await graphPost(`${igUserId}/media_publish`, {
    creation_id: carousel.id,
  });
  if (!published.id) throw new Error("Instagram did not return published media id.");

  return {
    mediaId: published.id,
    containerId: carousel.id,
    childrenContainerIds: children,
    imageCount: imageUrls.length,
    usedCarousel: true,
    permalink: null,
  };
}

async function postCallback({ callbackUrl, callbackToken, payload, requestId, jobId }) {
  if (!callbackUrl || !callbackToken) {
    throw new Error("Missing callback URL or callback token.");
  }

  const stage = payload?.event?.stage || payload?.status || "unknown";
  const startedAt = Date.now();
  logDebug(requestId, "callback_start", {
    job: jobId,
    stage,
    status: payload?.status,
    callback: safeUrlLabel(callbackUrl),
  });

  const response = await fetch(callbackUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${callbackToken}`,
    },
    body: JSON.stringify(payload),
  });
  const elapsedMs = Date.now() - startedAt;
  logDebug(requestId, "callback_end", {
    job: jobId,
    stage,
    status: payload?.status,
    httpStatus: response.status,
    elapsedMs,
  });

  if (!response.ok) {
    const raw = await response.text();
    logError(requestId, "callback_failed_response", {
      job: jobId,
      stage,
      httpStatus: response.status,
      body: raw.slice(0, 180),
    });
    throw new Error(`Callback request failed (${response.status}): ${raw.slice(0, 300)}`);
  }
}

async function postProgress({ callbackUrl, callbackToken, requestId, jobId, startedAt, stage, message, progress, slideNumber, elapsedMs, level = "info", details = null }) {
  if (!callbackUrl || !callbackToken) {
    logWarn(requestId, "progress_callback_skipped_missing_config", { job: jobId, stage });
    return;
  }

  try {
    await postCallback({
      callbackUrl,
      callbackToken,
      requestId,
      jobId,
      payload: {
        status: "generating",
        event: {
          id: `${Date.now()}-${stage}-${slideNumber || "job"}`,
          at: new Date().toISOString(),
          stage,
          message,
          level,
          slideNumber: typeof slideNumber === "number" ? slideNumber : null,
          progress: typeof progress === "number" ? Math.max(0, Math.min(100, progress)) : null,
          elapsedMs: typeof elapsedMs === "number" ? elapsedMs : Date.now() - startedAt,
          details: {
            requestId,
            jobId,
            ...(details && typeof details === "object" ? details : {}),
          },
        },
      },
    });
    logDebug(requestId, "progress_callback_ok", {
      job: jobId,
      stage,
      slide: slideNumber,
      progress,
    });
  } catch (error) {
    logWarn(requestId, "progress_callback_failed", {
      job: jobId,
      stage,
      slide: slideNumber,
      error: error instanceof Error ? error.message : "unknown",
    });
  }
}

async function runMuslimahCarouselJob({ body, requestId, getLogPrefix }) {
  const jobId = asNonEmptyString(body.jobId);
  const callbackUrl = asNonEmptyString(body.callbackUrl);
  const callbackToken = asNonEmptyString(body.callbackToken);
  const collectionId = asNonEmptyString(body.collectionId) || "muslimah-health";
  const scriptModel = asNonEmptyString(body.scriptModel) || SCRIPT_MODEL;
  const imageModel = asNonEmptyString(body.imageModel) || IMAGE_MODEL;
  const publish = asBoolean(body.publish, false);
  const startedAt = Date.now();
  const progress = (event) => postProgress({ callbackUrl, callbackToken, requestId, jobId, startedAt, ...event });

  try {
    logDebug(requestId, "background_start", {
      job: jobId,
      collection: collectionId,
      publish: publish ? "yes" : "no",
      scriptModel,
      imageModel,
      callback: safeUrlLabel(callbackUrl),
      suppliedScript: isScript(body.script) ? "yes" : "no",
      previousFeatures: asStringArray(body.previousFeatures).length,
      referencePaths: asStringArray(body.referenceImagePaths).length,
    });
    await progress({
      stage: "worker_started",
      message: "Render worker started the muslimah carousel job.",
      progress: 5,
      details: { jobId, publish, scriptModel, imageModel },
    });

    let script;
    if (isScript(body.script)) {
      logDebug(requestId, "script_reuse_start", { job: jobId });
      script = normalizeScript(body.script, pickHookBackground(body.previousHookBackground));
      logDebug(requestId, "script_reuse_done", {
        job: jobId,
        hookBackground: script.hookBackground,
        selectedFeatures: script.selectedFeatures.length,
      });
      await progress({
        stage: "script_reused",
        message: "Using script JSON supplied by the caller.",
        progress: 18,
        details: { hookBackground: script.hookBackground, selectedFeatures: script.selectedFeatures, script },
      });
    } else {
      await progress({
        stage: "script_generation_started",
        message: `Generating carousel script with ${scriptModel}.`,
        progress: 8,
      });
      const scriptStartedAt = Date.now();
      logDebug(requestId, "script_generation_start", { job: jobId, model: scriptModel });
      script = await generateScript({
        scriptModel,
        focus: asNonEmptyString(body.focus) || undefined,
        previousHookBackground: asNonEmptyString(body.previousHookBackground) || undefined,
        previousFeatures: asStringArray(body.previousFeatures),
        requestId,
        jobId,
        onProgress: progress,
      });
      logDebug(requestId, "script_generation_done", {
        job: jobId,
        elapsedMs: Date.now() - scriptStartedAt,
        hookBackground: script.hookBackground,
        selectedFeatures: script.selectedFeatures.length,
        freshTalkingPoints: script.freshTalkingPoints.length,
      });
      await progress({
        stage: "script_generated",
        message: `Script generated. Hook background: ${script.hookBackground}.`,
        progress: 18,
        elapsedMs: Date.now() - scriptStartedAt,
        details: {
          hookBackground: script.hookBackground,
          selectedFeatures: script.selectedFeatures,
          freshTalkingPoints: script.freshTalkingPoints,
          script,
        },
      });
    }

    logDebug(requestId, "image_batch_start", {
      job: jobId,
      slides: script.slides.length,
      model: imageModel,
    });
    const images = await generateImages({
      script,
      imageModel,
      collectionId,
      referenceImagePaths: asStringArray(body.referenceImagePaths),
      onProgress: progress,
      requestId,
      jobId,
    });
    logDebug(requestId, "image_batch_done", { job: jobId, images: images.length });
    await progress({
      stage: "images_completed",
      message: `Generated and uploaded ${images.length} carousel images.`,
      progress: 90,
      details: { imageCount: images.length },
    });
    const generation = {
      scriptModel,
      imageModel,
      imageQuality: IMAGE_QUALITY,
      imageSize: IMAGE_SIZE,
      script,
      images,
    };
    if (publish) {
      logDebug(requestId, "publish_requested", { job: jobId });
      await progress({
        stage: "publish_started",
        message: "Publishing carousel to Instagram.",
        progress: 94,
      });
    }
    const publishResult = await publishInstagram({ generation, publish, requestId, jobId });
    if (publishResult) {
      await progress({
        stage: "publish_completed",
        message: "Instagram publish completed.",
        progress: 98,
        details: publishResult,
      });
    }

    logDebug(requestId, "final_callback_start", { job: jobId, images: images.length });
    await postCallback({
      callbackUrl,
      callbackToken,
      requestId,
      jobId,
      payload: {
        status: "completed",
        event: {
          id: `${Date.now()}-completed`,
          at: new Date().toISOString(),
          stage: "completed",
          message: `Carousel completed with ${images.length} generated images.`,
          level: "info",
          slideNumber: null,
          progress: 100,
          elapsedMs: Date.now() - startedAt,
          details: { requestId, published: Boolean(publishResult) },
        },
        result: {
          ...generation,
          generatedImages: true,
          published: Boolean(publishResult),
          publishResult,
        },
      },
    });

    logDebug(requestId, "background_completed", {
      job: jobId,
      images: images.length,
      elapsedMs: Date.now() - startedAt,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to run muslimah carousel worker job.";
    logError(requestId, "background_failed", {
      job: jobId || "none",
      elapsedMs: Date.now() - startedAt,
      error: message,
      stack: error instanceof Error ? String(error.stack || "").split("\n").slice(0, 3).join(" / ") : undefined,
    });

    try {
      logDebug(requestId, "failure_callback_start", { job: jobId });
      await postCallback({
        callbackUrl,
        callbackToken,
        requestId,
        jobId,
        payload: {
          status: "failed",
          error: message,
          event: {
            id: `${Date.now()}-failed`,
            at: new Date().toISOString(),
            stage: "failed",
            message,
            level: "error",
            slideNumber: null,
            progress: null,
            elapsedMs: Date.now() - startedAt,
            details: { requestId },
          },
        },
      });
    } catch (callbackError) {
      logError(requestId, "failure_callback_failed", {
        job: jobId,
        error: callbackError instanceof Error ? callbackError.message : "unknown",
      });
    }
  }
}

function installMuslimahCarouselRoutes(app, { authorizeRequest, getLogPrefix }) {
  app.post("/api/muslimah-carousel/worker", async (req, res) => {
    const requestId = randomUUID();
    logDebug(requestId, "worker_request_received", {
      path: req.originalUrl || req.url,
      method: req.method,
      contentLength: req.headers["content-length"] || "unknown",
    });
    if (!authorizeRequest(req, res, requestId)) return;

    try {
      const body = req.body || {};
      const jobId = asNonEmptyString(body.jobId);
      if (!jobId) {
        logWarn(requestId, "accept_rejected_missing_job_id");
        return res.status(400).json({ error: "Job ID is required.", requestId });
      }

      if (!asNonEmptyString(body.callbackUrl) || !asNonEmptyString(body.callbackToken)) {
        logWarn(requestId, "accept_rejected_missing_callback", { job: jobId });
        return res.status(400).json({ error: "Callback URL and token are required.", jobId, requestId });
      }

      logDebug(requestId, "accepted", {
        job: jobId,
        collection: asNonEmptyString(body.collectionId) || "muslimah-health",
        callback: safeUrlLabel(body.callbackUrl),
        publish: asBoolean(body.publish, false) ? "yes" : "no",
        suppliedScript: isScript(body.script) ? "yes" : "no",
        referencePaths: asStringArray(body.referenceImagePaths).length,
      });
      res.status(202).json({
        jobId,
        status: "accepted",
        requestId,
        progressCallbacks: true,
        progressStages: [
          "worker_started",
          "script_generation_started",
          "script_generated",
          "image_generation_started",
          "image_generated",
          "image_uploaded",
          "images_completed",
          "publish_started",
          "publish_completed",
          "completed",
          "failed",
        ],
      });

      logDebug(requestId, "background_schedule", { job: jobId });
      setImmediate(() => {
        logDebug(requestId, "background_invoked", { job: jobId });
        runMuslimahCarouselJob({ body, requestId, getLogPrefix }).catch((error) => {
          logError(requestId, "unhandled_background_error", {
            job: jobId,
            error: error instanceof Error ? error.message : "unknown",
          });
        });
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to run muslimah carousel worker job.";
      logError(requestId, "accept_failed", { error: message });
      res.status(500).json({ error: message, requestId });
    }
  });
}

module.exports = { installMuslimahCarouselRoutes };
