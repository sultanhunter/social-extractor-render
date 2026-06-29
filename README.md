# Social Extractor (Render)

Standalone extractor service for Instagram/TikTok media URLs using `gallery-dl` and `yt-dlp`.

## Endpoints

- `GET /` - basic status
- `GET /health` - health + cookies/proxy status
- `POST /api/extract-social-post` - media extraction
- `POST /api/extract-video-frames` - direct video stream resolution + frame extraction
  - optional transcript extraction (Gemini) from the resolved video audio
- `POST /api/muslimah-carousel/worker` - long-running muslimah.health carousel image generation worker

## Request

```json
{
  "url": "https://www.instagram.com/p/xxxx/",
  "platform": "instagram",
  "sessionId": "abcd1234"
}
```

`platform` is optional. It is auto-detected from the URL.

### Frame Extraction Request

```json
{
  "url": "https://www.instagram.com/reel/xxxx/",
  "platform": "instagram",
  "sessionId": "abcd1234",
  "frameCount": 6,
  "frameWidth": 960,
  "includeTranscript": true,
  "transcriptMaxSeconds": 90
}
```

- `frameCount` optional (`>=2`). If omitted, extractor auto-selects frame count based on full video duration for richer visual coverage.
- `frameWidth` optional (`480-1440`, default `960`)
- `includeTranscript` optional (`true/false`, default `true`)
- `transcriptMaxSeconds` optional. If omitted, full audio duration is transcribed.

Response includes:

- resolved `videoUrl`
- extraction metadata (`extractor`, `durationSeconds`, counts)
- `frames`: array of `{ index, timestamp, mimeType, data }` where `data` is base64 jpeg
- `transcript`: object with `available`, `fullText`, `summary`, `segments` (when enabled and successful)

## Auth

If `SOCIAL_EXTRACTOR_API_TOKEN` is set, send:

`Authorization: Bearer <token>`

The muslimah carousel worker uses the same bearer token.

## muslimah.health Carousel Worker

The Vercel app starts the job and writes a `muslimah_carousel_jobs` row in Supabase. This Render service accepts the job, immediately returns `202`, runs the long OpenAI image generation in the background, then calls back to the Vercel app to complete or fail the job.

Required Render env:

- `OPENAI_API_KEY`
- `CLOUDFLARE_R2_ACCOUNT_ID`
- `CLOUDFLARE_R2_ACCESS_KEY_ID`
- `CLOUDFLARE_R2_SECRET_ACCESS_KEY`
- `CLOUDFLARE_R2_BUCKET_NAME`
- `CLOUDFLARE_R2_PUBLIC_URL`

Optional Render env:

- `MUSLIMAH_CAROUSEL_REFERENCE_IMAGE_PATHS` comma-separated absolute paths. If unset, bundled assets in `assets/muslimah-carousel/` are used.
- `INSTAGRAM_GRAPH_ACCESS_TOKEN`, `INSTAGRAM_GRAPH_USER_ID`, `INSTAGRAM_GRAPH_API_VERSION` for `publish: true`.

The worker request includes `callbackUrl` and `callbackToken`; Render does not need Supabase credentials.

## Render Deployment

1. Create a new Render service from this folder/repo.
2. Render will detect `Dockerfile` (or use `render.yaml`).
3. Set environment variables from `.env.example`.
   - For transcript extraction, set `GOOGLE_GEMINI_API_KEY` on Render.
4. Add `instagram_cookies.txt` in project root, or set `INSTAGRAM_COOKIES_CONTENT` in Render env with the full cookies.txt content.

## Wire with Vercel app

In `social-spark` Vercel env:

- `SOCIAL_EXTRACTOR_API_URL=https://<your-render-service>.onrender.com`
- `SOCIAL_EXTRACTOR_API_TOKEN=<same-token-as-render>`

The muslimah carousel generator derives `https://<your-render-service>.onrender.com/api/muslimah-carousel/worker` from `SOCIAL_EXTRACTOR_API_URL`; no separate worker URL is required.

## Local run

```bash
npm install
npm start
```

## Smoke test

```bash
curl -X POST "http://localhost:3000/api/extract-social-post" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{"url":"https://www.instagram.com/p/DUzSBRfjD5E/","platform":"instagram"}'

curl -X POST "http://localhost:3000/api/extract-video-frames" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{"url":"https://www.instagram.com/reel/DUzSBRfjD5E/","platform":"instagram","frameCount":6,"frameWidth":960}'
```
