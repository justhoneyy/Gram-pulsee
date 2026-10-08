# GRAM-PULSE

Express + PostgreSQL app. One server file, three frontend files, no build step.

```
server.js          API, auth, simulation engines, database schema, demo seed
public/index.html  page markup
public/style.css   styles
public/app.js      frontend (API client, views, logic, Lucide icons)
package.json  render.yaml  .gitignore  README.md
```

## Deploy on Render

**Option A, Blueprint (fastest):** push this folder to GitHub, then in Render choose New > Blueprint and select the repo. `render.yaml` creates the PostgreSQL database and the web service and wires them together.

**Option B, manual:**
1. New > PostgreSQL. Copy its **Internal Database URL**.
2. New > Web Service from your repo. Build command `npm install`, start command `node server.js`, health check path `/api/health`.
3. Environment variables:

| Name | Value |
|---|---|
| `DATABASE_URL` | the Internal Database URL from step 1 |
| `SEED_PASSWORD` | a password of 12+ characters (used for the demo accounts) |
| `NODE_ENV` | `production` |
| `NODE_VERSION` | `22` |

On first start the app creates every table and loads the demo data by itself. Nothing to run in a shell. Later restarts skip both steps.

## Sign in

- `admin@gram-pulse.demo`
- `officer@gram-pulse.demo`
- `citizen@gram-pulse.demo`

Password is whatever `SEED_PASSWORD` is set to (with the Blueprint, open the service's Environment tab to read the generated value). Change it from Account > Profile after signing in.

## Optional variables

| Name | Purpose |
|---|---|
| `APP_ORIGIN` | your public URL; only needed if you use a custom domain |
| `PGSSLMODE=disable` | turn off SSL for a database that does not support it |
| `PG_POOL_MAX` | connection pool size (default 10) |

## Run locally

```
npm install
DATABASE_URL=postgres://user:pass@localhost:5432/grampulse SEED_PASSWORD=choose-a-long-password npm start
```

Open http://localhost:3000. For local use you can put those two variables in a `.env` file instead.

## Notes

- Uploaded report photos are stored in PostgreSQL, so they survive Render redeploys and restarts (Render's disk is temporary).
- Data is simulated demo data, as in the original project.
