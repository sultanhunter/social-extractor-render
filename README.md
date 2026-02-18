# Social Extractor (Render)

Standalone extractor service for Instagram/TikTok media URLs using `gallery-dl` and `yt-dlp`.

## Endpoints

- `GET /` - basic status
- `GET /health` - health + cookies/proxy status
- `POST /api/extract-social-post` - media extraction

## Request

```json
{
  "url": "https://www.instagram.com/p/xxxx/",
  "platform": "instagram",
  "sessionId": "abcd1234"
}
```

`platform` is optional. It is auto-detected from the URL.

## Auth

If `SOCIAL_EXTRACTOR_API_TOKEN` is set, send:

`Authorization: Bearer <token>`

## Render Deployment

1. Create a new Render service from this folder/repo.
2. Render will detect `Dockerfile` (or use `render.yaml`).
3. Set environment variables from `.env.example`.
4. Add `instagram_cookies.txt` in project root, or set `INSTAGRAM_COOKIES_CONTENT` in Render env with the full cookies.txt content.

## Wire with Vercel app

In `social-spark` Vercel env:

- `SOCIAL_EXTRACTOR_API_URL=https://<your-render-service>.onrender.com`
- `SOCIAL_EXTRACTOR_API_TOKEN=<same-token-as-render>`

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
```
