// src/server/db.ts
import pg from "pg";
var SCHEMA = `
CREATE TABLE schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE villages(id text PRIMARY KEY, data jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE wards(id text PRIMARY KEY, village_id text NOT NULL REFERENCES villages(id), number integer NOT NULL CHECK(number BETWEEN 1 AND 6), data jsonb NOT NULL, UNIQUE(village_id,number));
CREATE TABLE users(id text PRIMARY KEY, village_id text NOT NULL REFERENCES villages(id), email text NOT NULL UNIQUE, name text NOT NULL, role text NOT NULL CHECK(role IN ('admin','officer','citizen')), password_hash text NOT NULL, disabled boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE sessions(token_hash text PRIMARY KEY, user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE, csrf text NOT NULL, expires_at timestamptz NOT NULL);
CREATE INDEX sessions_expiry ON sessions(expires_at);
CREATE TABLE assets(id text PRIMARY KEY, village_id text NOT NULL REFERENCES villages(id), ward_id text NOT NULL REFERENCES wards(id), data jsonb NOT NULL CHECK((data->>'condition')::numeric BETWEEN 0 AND 100), created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX assets_ward ON assets(ward_id);
CREATE INDEX assets_type ON assets((data->>'type'));
CREATE TABLE dependencies(id text PRIMARY KEY, source text NOT NULL REFERENCES assets(id) ON DELETE CASCADE, target text NOT NULL REFERENCES assets(id) ON DELETE CASCADE, type text NOT NULL CHECK(type IN ('depends_on','supplies','protects','serves','connected_to','accessible_by','affects','downstream_of','upstream_of')), strength double precision NOT NULL CHECK(strength>0 AND strength<=1), impact_weight double precision NOT NULL CHECK(impact_weight>0 AND impact_weight<=1), created_at timestamptz NOT NULL DEFAULT now(), CHECK(source<>target), UNIQUE(source,target,type));
CREATE INDEX dependencies_target ON dependencies(target);
CREATE TABLE uploads(id text PRIMARY KEY, user_id text NOT NULL REFERENCES users(id), data bytea NOT NULL, mime text NOT NULL, size integer NOT NULL CHECK(size>0), created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE reports(id text PRIMARY KEY,village_id text NOT NULL REFERENCES villages(id),ward_id text NOT NULL REFERENCES wards(id),asset_id text REFERENCES assets(id) ON DELETE SET NULL,user_id text NOT NULL REFERENCES users(id),assigned_officer text REFERENCES users(id),image_id text REFERENCES uploads(id),data jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX reports_asset ON reports(asset_id);
CREATE INDEX reports_owner ON reports(user_id,created_at);
CREATE INDEX reports_assignee ON reports(assigned_officer);
CREATE TABLE incidents(id text PRIMARY KEY,village_id text NOT NULL REFERENCES villages(id),asset_id text NOT NULL REFERENCES assets(id),data jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE incident_assets(incident_id text REFERENCES incidents(id) ON DELETE CASCADE,asset_id text REFERENCES assets(id),PRIMARY KEY(incident_id,asset_id));
CREATE TABLE interventions(id text PRIMARY KEY,village_id text NOT NULL REFERENCES villages(id),asset_id text NOT NULL REFERENCES assets(id),data jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE simulations(id text PRIMARY KEY,user_id text NOT NULL REFERENCES users(id),village_id text NOT NULL REFERENCES villages(id),kind text NOT NULL,input jsonb NOT NULL,result jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE settings(village_id text PRIMARY KEY REFERENCES villages(id),weights jsonb NOT NULL,active_simulation text REFERENCES simulations(id) ON DELETE SET NULL);
CREATE TABLE pulse_history(id bigserial PRIMARY KEY,village_id text NOT NULL REFERENCES villages(id),pulse double precision NOT NULL,reason text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX pulse_date ON pulse_history(village_id,created_at);
CREATE TABLE audit_log(id bigserial PRIMARY KEY,user_id text REFERENCES users(id),village_id text NOT NULL REFERENCES villages(id),action text NOT NULL,entity text NOT NULL,entity_id text,metadata jsonb NOT NULL DEFAULT '{}',created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE notifications(id bigserial PRIMARY KEY,village_id text NOT NULL REFERENCES villages(id),user_id text REFERENCES users(id),message text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE notification_reads(notification_id bigint REFERENCES notifications(id) ON DELETE CASCADE,user_id text REFERENCES users(id) ON DELETE CASCADE,PRIMARY KEY(notification_id,user_id));
INSERT INTO schema_migrations(version) VALUES (1)
`;
async function connect() {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("DATABASE_URL is not set. Add your PostgreSQL connection string as an environment variable.");
  const host = new URL(url).hostname;
  const isLocal = ["localhost", "127.0.0.1", "::1"].includes(host);
  const isRenderInternal = !host.includes(".");
  const useSsl = process.env.PGSSLMODE !== "disable" && !isLocal && !isRenderInternal;
  const pool = new pg.Pool({
    connectionString: url,
    max: Number(process.env.PG_POOL_MAX || 10),
    connectionTimeoutMillis: 15e3,
    idleTimeoutMillis: 3e4,
    ssl: useSsl ? { rejectUnauthorized: false } : void 0
  });
  pool.on("error", (err) => console.error("PostgreSQL pool error:", err.message));
  // Keep every table in its own schema so this app never collides with other tables in a shared database.
  pool.on("connect", (client) => { client.query("SET search_path TO gram_pulse"); });
  const wrap = (client) => ({
    query: async (sql, params = []) => (await client.query(sql, params)).rows,
    transaction: async (fn) => {
      const c = await pool.connect();
      try {
        await c.query("BEGIN");
        const result = await fn(wrap(c));
        await c.query("COMMIT");
        return result;
      } catch (e) {
        await c.query("ROLLBACK").catch(() => {
        });
        throw e;
      } finally {
        c.release();
      }
    },
    close: () => pool.end()
  });
  const db2 = wrap(pool);
  for (let attempt = 1; ; attempt++) {
    try {
      await db2.query("SELECT 1");
      break;
    } catch (e) {
      if (attempt >= 5) throw e;
      console.log(`Waiting for PostgreSQL (attempt ${attempt}): ${e.message}`);
      await new Promise((r) => setTimeout(r, 3e3));
    }
  }
  return db2;
}
async function migrate(db2) {
  await db2.transaction(async (tx) => {
    await tx.query("SELECT pg_advisory_xact_lock(727201)");
    await tx.query("CREATE SCHEMA IF NOT EXISTS gram_pulse");
    const exists = await tx.query("SELECT 1 FROM information_schema.tables WHERE table_schema='gram_pulse' AND table_name='schema_migrations'");
    if (exists.length) return;
    for (const statement of SCHEMA.split(";").map((s) => s.trim()).filter(Boolean)) await tx.query(statement);
    console.log("Database schema created.");
  });
}

// src/server/app.ts
import express from "express";
import helmet from "helmet";
import { rateLimit } from "express-rate-limit";
import { z as z2 } from "zod";
import { randomUUID, timingSafeEqual as timingSafeEqual2 } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import sharp from "sharp";

// src/server/auth.ts
import { scrypt, randomBytes, createHash, timingSafeEqual } from "node:crypto";
import { promisify } from "node:util";
var derive = promisify(scrypt);
async function hashPassword(password) {
  const salt = randomBytes(16).toString("hex");
  const key = await derive(password, salt, 64);
  return `${salt}:${key.toString("hex")}`;
}
async function verifyPassword(password, hash) {
  const [salt, hex] = hash.split(":");
  if (!salt || !hex) return false;
  const key = await derive(password, salt, 64);
  const expected = Buffer.from(hex, "hex");
  return expected.length === key.length && timingSafeEqual(key, expected);
}
var tokenHash = (token2) => createHash("sha256").update(token2).digest("hex");
var token = () => randomBytes(32).toString("hex");
async function session(db2, value) {
  if (!value) return null;
  const rows = await db2.query(`SELECT u.id,u.name,u.email,u.role,u.village_id AS "villageId",u.disabled,s.csrf FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.expires_at>now() AND NOT u.disabled`, [tokenHash(value)]);
  return rows[0] || null;
}

// src/shared/models.ts
import { z } from "zod";
var id = z.string().regex(/^[A-Za-z0-9_-]{1,80}$/);
var text = z.string().trim().min(1).max(200);
var score = z.number().min(0).max(100);
var roles = ["admin", "officer", "citizen"];
var assetSchema = z.object({ name: text, type: z.enum(["Road", "Drainage", "Water", "Education", "Healthcare", "Sanitation", "Streetlight", "Public Facility"]), ward: z.number().int().min(1).max(6), condition: score, pop: z.number().int().min(0).max(1e6), cost: z.number().positive().max(1e5), hazard: score, action: text, latitude: z.number().min(-90).max(90).default(23.012), longitude: z.number().min(-180).max(180).default(78.012), x: score.default(50), y: score.default(50), capacity: z.number().min(0).max(1e7).default(100), accessibility: score.default(70), status: z.enum(["Operational", "Degraded", "Unavailable", "Proposed", "Retired"]).default("Operational"), source: z.enum(["SIMULATED", "FIELD REPORTED", "VERIFIED", "MODELLED", "PREDICTED"]).default("FIELD REPORTED"), verifiedAt: z.iso.datetime().nullable().default(null), installationDate: z.iso.date().nullable().default(null), maintenanceDate: z.iso.date().nullable().default(null), age: z.number().min(0).max(500).default(0), geometry: z.array(z.tuple([z.number().min(-180).max(180), z.number().min(-90).max(90)])).max(1e3).default([]) }).strict();
var relationshipTypes = ["depends_on", "supplies", "protects", "serves", "connected_to", "accessible_by", "affects", "downstream_of", "upstream_of"];
var dependencySchema = z.object({ source: id, target: id, type: z.enum(relationshipTypes), strength: z.number().min(0.01).max(1).default(1), impactWeight: z.number().min(0.01).max(1).default(1) }).strict().refine((v) => v.source !== v.target, "Self dependencies are not allowed");
var reportSchema = z.object({ name: z.string().trim().max(100).optional(), category: text, title: text.optional(), description: z.string().trim().min(8).max(2e3), ward: z.number().int().min(1).max(6), assetId: id.nullable().default(null), imageId: id.nullable().default(null), latitude: z.number().min(-90).max(90).nullable().default(null), longitude: z.number().min(-180).max(180).nullable().default(null) }).strict();
var reportPatch = z.object({ status: z.enum(["Pending verification", "Verified", "Assigned", "In Progress", "Resolved", "Rejected"]).optional(), assignedOfficer: id.nullable().optional(), resolutionNotes: z.string().max(2e3).optional(), description: z.string().min(8).max(2e3).optional() }).strict();
var incidentSchema = z.object({ assetId: id, category: text.optional(), severity: z.enum(["High", "Medium", "Low"]), status: z.enum(["Open", "Investigating", "Assigned", "In Progress", "Resolved"]), description: z.string().trim().min(8).max(2e3), cause: z.string().max(1e3).default("Pending investigation"), resolution: z.string().max(2e3).default(""), date: z.iso.datetime().optional(), endTime: z.iso.datetime().nullable().default(null), affectedAssets: z.array(id).max(200).default([]), response: z.string().max(2e3).default("") }).strict();
var interventionSchema = z.object({ assetId: id, ward: z.number().int().min(1).max(6), name: text, cost: z.number().positive().max(1e5), riskReduction: score, serviceImprovement: score, populationBenefit: z.number().int().min(0).max(1e6), status: z.enum(["proposed", "approved", "in_progress", "completed", "rejected"]).default("proposed"), timeline: z.string().max(200).default("Field assessment required") }).strict();
var defaultWeights = { condition: 0.22, population: 0.2, service: 0.18, accessibility: 0.15, hazard: 0.15, evidence: 0.1 };
var weightsSchema = z.object({ condition: score, population: score, service: score, accessibility: score, hazard: score, evidence: score }).strict().refine((v) => Object.values(v).some((x) => x > 0), "At least one weight must be positive");

// src/server/engines.ts
var clamp = (n) => Math.max(0, Math.min(100, n));
var round = (n) => Math.round(n * 100) / 100;
function arcs(edges) {
  return edges.flatMap((e) => {
    const x = { source: e.source, target: e.target, weight: e.strength * e.impactWeight, type: e.type };
    if (["depends_on", "accessible_by", "downstream_of"].includes(e.type)) return [{ ...x, source: e.target, target: e.source }];
    if (e.type === "connected_to") return [x, { ...x, source: e.target, target: e.source }];
    return [x];
  });
}
function rice(assets, edges, wards, starters, rain = 0, sc = "custom") {
  const byId = new Map(assets.map((a) => [a.id, a])), links = arcs(edges).sort((a, b) => a.source.localeCompare(b.source) || a.target.localeCompare(b.target));
  const seen = /* @__PURE__ */ new Map(), queue = [...new Set(starters)].sort().filter((x) => byId.has(x));
  for (const s of queue) seen.set(s, { depth: 0, intensity: 1 });
  const paths = [];
  for (let i = 0; i < queue.length; i++) {
    const source = queue[i], v = seen.get(source);
    for (const e of links.filter((e2) => e2.source === source)) {
      const intensity = v.intensity * e.weight;
      if (intensity < 0.1 || seen.has(e.target)) continue;
      seen.set(e.target, { depth: v.depth + 1, intensity });
      queue.push(e.target);
      paths.push({ source, target: e.target, depth: v.depth + 1, intensity: round(intensity) });
    }
  }
  const affected = queue.map((x) => byId.get(x)), wardIds = new Set(affected.map((a) => a.ward));
  let pop = 0, households = 0;
  for (const w of wards) {
    const exposed = Math.min(w.population, Math.max(0, ...affected.filter((a) => a.ward === w.number).map((a) => a.pop * seen.get(a.id).intensity * (1 + Math.max(0, rain) / 200))));
    pop += exposed;
    households += w.population ? exposed / w.population * w.households : 0;
  }
  const depth = Math.max(0, ...[...seen.values()].map((v) => v.depth)), population = wards.reduce((s, w) => s + w.population, 0);
  return { sc, direct: [...new Set(starters)].sort(), affected: queue, paths, chain: affected.map((a) => [`${seen.get(a.id).depth ? "Dependency impact" : "Initiating failure"} · ${a.name}`, `${a.type} · Ward ${a.ward} · exposure ${Math.round(seen.get(a.id).intensity * 100)}%`]), depth, pop: Math.round(pop), households: Math.round(households), services: new Set(affected.map((a) => a.type)).size, wards: wardIds.size, impact: round(clamp(pop / Math.max(1, population) * 70 + depth * 5 + Math.max(0, rain) * 0.1)), schools: affected.filter((a) => a.type === "Education").length, healthcare: affected.filter((a) => a.type === "Healthcare").length, waterPoints: affected.filter((a) => a.type === "Water").length, accessibilityChange: -round(affected.filter((a) => a.type === "Road").length / Math.max(1, assets.filter((a) => a.type === "Road").length) * 100), label: "SIMULATED — directed graph / ward-capped overlapping catchments" };
}
function scoreAssets(assets, edges, reports, incidents, weights = defaultWeights, affected = []) {
  const links = arcs(edges), total = Object.values(weights).reduce((a, b) => a + b, 0);
  return assets.map((a) => {
    const evidence = reports.filter((r) => r.assetId === a.id), open = evidence.filter((r) => !["Resolved", "Rejected"].includes(r.status)), incident = incidents.filter((i) => (i.assetId === a.id || i.affectedAssets.includes(a.id)) && i.status !== "Resolved"), downstream = links.filter((e) => e.source === a.id), condition = a.status === "Unavailable" ? 0 : a.condition;
    const factors = { condition: 100 - condition, population: clamp(a.pop / 12), service: clamp(downstream.length * 20), accessibility: 100 - a.accessibility, hazard: clamp(a.hazard + incident.length * 10 + (affected.includes(a.id) ? 25 : 0)), evidence: clamp(open.reduce((s, r) => s + (r.severity === "HIGH" ? 20 : r.severity === "MEDIUM" ? 12 : 5), 0)) };
    const reason = Object.entries(weights).map(([k, v]) => [k, round(factors[k] * v / total)]);
    const critical = round(reason.reduce((s, [, v]) => s + v, 0));
    const confidenceFactors = { provenance: a.source === "VERIFIED" ? 40 : a.source === "FIELD REPORTED" ? 25 : 15, inspection: a.verifiedAt ? 25 : 0, evidence: Math.min(20, evidence.filter((r) => r.status === "Verified" || r.status === "Resolved").length * 4), completeness: a.latitude !== null && a.longitude !== null ? 15 : 0 };
    return { ...a, condition, critical, criticality: critical, risk: affected.includes(a.id) || critical >= 65 ? "High" : critical >= 40 ? "Medium" : "Low", reports: evidence.length, connections: downstream.map((e) => e.target), reason, confidenceFactors, confidence: Object.values(confidenceFactors).reduce((s, n) => s + n, 0), verified: a.verifiedAt || "Not field verified", cat: { Road: "road", Drainage: "drainage", Water: "water", Education: "education", Healthcare: "health" }[a.type] || "sanitation" };
  });
}
function pulse(assets, reports, incidents, affected = []) {
  const categories = {};
  for (const [type, label] of [["Water", "Water"], ["Road", "Roads"], ["Drainage", "Drainage"], ["Education", "Education"], ["Sanitation", "Sanitation"], ["Healthcare", "Health"]]) {
    const list = assets.filter((a) => type === "Sanitation" ? ["Sanitation", "Streetlight", "Public Facility"].includes(a.type) : a.type === type);
    categories[label] = list.length ? round(list.reduce((s, a) => s + clamp(a.condition - a.critical * 0.15 - (affected.includes(a.id) ? 20 : 0)), 0) / list.length) : 0;
  }
  const incidentPenalty = Math.min(10, incidents.filter((i) => i.status !== "Resolved").length * 0.25);
  const reportPenalty = Math.min(10, reports.filter((r) => !["Resolved", "Rejected"].includes(r.status)).length * 0.08);
  return { categories, pulse: round(clamp(Object.values(categories).reduce((s, v) => s + v, 0) / 6 - incidentPenalty - reportPenalty)), factors: { categoryWeight: 1 / 6, incidentPenalty, reportPenalty, missingCategoryScore: 0 } };
}
function optimize(assets, projects, wards, options) {
  const items = projects.filter((p) => ["proposed", "approved"].includes(p.status) && p.cost <= options.budget).flatMap((p) => {
    const a = assets.find((a2) => a2.id === p.assetId);
    return a && (!options.ward || p.ward === options.ward) && (!options.category || a.type === options.category) && a.critical >= options.minimumPriority ? [{ ...p, asset: a, value: round((p.populationBenefit * 0.4 + a.critical * 3 + p.riskReduction * 2 + p.serviceImprovement) * (a.confidence / 100)) }] : [];
  }).sort((a, b) => a.id.localeCompare(b.id));
  const states = Array.from({ length: options.maxProjects + 1 }, () => []);
  states[0] = [{ cost: 0, value: 0, items: [] }];
  const cap = Math.round(options.budget * 1e5);
  for (const item of items) {
    for (let k = options.maxProjects; k >= 1; k--) {
      const candidates = [...states[k], ...states[k - 1].filter((s) => s.cost + Math.round(item.cost * 1e5) <= cap).map((s) => ({ cost: s.cost + Math.round(item.cost * 1e5), value: s.value + (options.strategy === "cost" ? 1 / item.cost : item.value), items: [...s.items, item] }))].sort((a, b) => a.cost - b.cost || b.value - a.value);
      let best2 = -Infinity;
      states[k] = candidates.filter((s) => {
        if (s.value <= best2) return false;
        best2 = s.value;
        return true;
      });
    }
  }
  const best = states.slice(options.minProjects).flat().sort((a, b) => b.value - a.value || a.cost - b.cost)[0];
  if (!best) return { feasible: false, projects: [], totalCost: 0, remainingBudget: options.budget, residents: 0, risk: 0, services: 0, wardCoverage: [], expectedImpact: 0, reason: "No combination satisfies the budget and project-count constraints." };
  const residents = wards.reduce((sum, w) => sum + Math.min(w.population, Math.max(0, ...best.items.filter((p) => p.ward === w.number).map((p) => p.populationBenefit))), 0), risk = best.items.length ? round(best.items.reduce((s, p) => s + p.riskReduction, 0) / best.items.length) : 0;
  return { feasible: true, projects: best.items.map((p) => ({ ...p, asset: void 0 })), totalCost: best.cost / 1e5, remainingBudget: round(options.budget - best.cost / 1e5), residents, risk, services: new Set(best.items.map((p) => p.asset.type)).size, serviceImprovement: best.items.length ? round(best.items.reduce((s, p) => s + p.serviceImprovement, 0) / best.items.length) : 0, wardCoverage: [...new Set(best.items.map((p) => p.ward))], expectedImpact: round(best.value), method: "Exact 0/1 knapsack; ward-max catchments avoid double-counting beneficiaries", factors: { population: 0.4, criticality: 3, riskReduction: 2, serviceImprovement: 1, confidenceMultiplier: true } };
}
function whatIf(assets, edges, wards, reports, incidents, weights, input) {
  const changed2 = assets.map((a) => ({ ...a, condition: clamp(a.condition + (a.id === input.assetId ? input.condition : 0)), capacity: a.capacity * (a.id === input.assetId ? input.capacity : 1), hazard: clamp(a.hazard + input.rain * 0.25), pop: Math.round(a.pop * (1 + input.population / 100)), ...input.relocate?.assetId === a.id ? { latitude: input.relocate.latitude, longitude: input.relocate.longitude } : {} }));
  if (input.newFacility) {
    const f = input.newFacility;
    changed2.push({ ...assets[0], id: "SCENARIO-FACILITY", name: "Proposed facility", type: f.type, ward: f.ward, latitude: f.latitude, longitude: f.longitude, capacity: f.capacity, pop: f.capacity, condition: 100, hazard: 0, reports: 0, connections: [], source: "SIMULATED" });
  }
  function metrics(list, populationFactor) {
    const scored = scoreAssets(list, edges, reports, incidents, weights);
    const distance = (w, a) => Math.hypot((w.latitude - a.latitude) * 111, (w.longitude - a.longitude) * 111 * Math.cos(w.latitude * Math.PI / 180));
    const proximity = (type) => round(wards.reduce((s, w) => {
      const facilities = list.filter((a) => a.type === type && a.status !== "Unavailable");
      return s + w.population * (facilities.length ? Math.max(0, 1 - Math.min(...facilities.map((a) => distance(w, a))) / 5) : 0);
    }, 0) / Math.max(1, wards.reduce((s, w) => s + w.population, 0)) * 100);
    const population = wards.reduce((s, w) => s + w.population, 0) * populationFactor;
    return { pulse: pulse(scored, reports, incidents).pulse, waterCoverage: round(Math.min(100, list.filter((a) => a.type === "Water").reduce((s, a) => s + a.capacity * a.condition / 100, 0) / Math.max(1, population) * 100)), floodExposure: round(list.filter((a) => a.type === "Drainage").reduce((s, a) => s + a.hazard * (1 - a.condition / 100), 0) / Math.max(1, list.filter((a) => a.type === "Drainage").length)), schoolAccessibility: proximity("Education"), healthcareProximity: proximity("Healthcare"), accessibility: round(list.filter((a) => a.type === "Road").reduce((s, a) => s + a.condition, 0) / Math.max(1, list.filter((a) => a.type === "Road").length)), infrastructureRisk: round(scored.reduce((s, a) => s + a.critical, 0) / Math.max(1, scored.length)), population: Math.round(population) };
  }
  const before = metrics(assets, 1), after = metrics(changed2, 1 + input.population / 100);
  return { before, after, delta: Object.fromEntries(Object.keys(before).map((k) => [k, round(after[k] - before[k])])), label: "SIMULATED — capacity-weighted supply and 5 km schematic service catchments; base records unchanged" };
}

// src/server/services.ts
var VILLAGE = "adarsh";
var AppError = class extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
  status;
  code;
};
async function records(db2, table) {
  return db2.query(`SELECT data || jsonb_build_object('id',id) ${table === "assets" ? `|| jsonb_build_object('villageId',village_id,'createdAt',created_at,'updatedAt',updated_at)` : ""} AS record FROM ${table} ORDER BY id`).then((rows) => rows.map((r) => r.record));
}
async function getReports(db2) {
  return db2.query(`SELECT data || jsonb_build_object('id',id,'assetId',asset_id,'userId',user_id,'assignedOfficer',assigned_officer,'imageId',image_id,'photo',CASE WHEN image_id IS NULL THEN '' ELSE '/api/uploads/'||image_id END) AS record FROM reports ORDER BY created_at DESC`).then((rows) => rows.map((r) => r.record));
}
async function snapshot(db2, user2) {
  const [raw, wards, villages, dependencies, reports, incidents, interventions, settings, history] = await Promise.all([records(db2, "assets"), records(db2, "wards"), records(db2, "villages"), db2.query('SELECT id,source,target,type,strength,impact_weight AS "impactWeight" FROM dependencies ORDER BY id'), getReports(db2), records(db2, "incidents"), records(db2, "interventions"), db2.query(`SELECT weights,result FROM settings LEFT JOIN simulations ON simulations.id=settings.active_simulation WHERE settings.village_id=$1`, [user2.villageId]), db2.query("SELECT created_at AS date,pulse,reason FROM pulse_history WHERE village_id=$1 ORDER BY id DESC LIMIT 30", [user2.villageId])]);
  const simulation = settings[0]?.result || null, weights = settings[0]?.weights || defaultWeights, affected = simulation?.affected || [], assets = scoreAssets(raw, dependencies, reports, incidents, weights, affected), health = pulse(assets, reports, incidents, affected);
  return { village: villages[0], wards, assets, dependencies, reports: reports.filter((r) => user2.role === "admin" || (user2.role === "officer" ? r.assignedOfficer === user2.id : r.userId === user2.id)), incidents, interventions, weights, simAffected: affected, simulation, pulseHistory: history.reverse(), ...health, user: user2, revision: String(history.at(-1)?.date || "0") };
}
async function audit(db2, user2, action, entity, entityId, metadata = {}) {
  await db2.query("INSERT INTO audit_log(user_id,village_id,action,entity,entity_id,metadata) VALUES($1,$2,$3,$4,$5,$6)", [user2.id, user2.villageId, action, entity, entityId, JSON.stringify(metadata)]);
}
async function changed(db2, user2, action, entity, entityId, privateUser = null) {
  await audit(db2, user2, action, entity, entityId);
  const s = await snapshot(db2, { ...user2, role: "admin" });
  await db2.query("INSERT INTO pulse_history(village_id,pulse,reason) VALUES($1,$2,$3)", [user2.villageId, s.pulse, action]);
  await db2.query("INSERT INTO notifications(village_id,user_id,message) VALUES($1,$2,$3)", [user2.villageId, privateUser, `${action}${entityId ? " · " + entityId : ""}`]);
}
async function requireAsset(db2, id2, villageId = VILLAGE) {
  const rows = await db2.query("SELECT data FROM assets WHERE id=$1 AND village_id=$2", [id2, villageId]);
  if (!rows[0]) throw new AppError(404, "ASSET_NOT_FOUND", "Infrastructure asset was not found.");
  return { ...rows[0].data, id: id2 };
}
async function classify(db2, category, description, ward, assetId) {
  const severe = /blocked|collapse|unsafe|no water|flood|emergency|broken/i.test(description), medium = /leak|damage|intermittent|repair/i.test(description);
  const type = /drain|waterlog|flood/i.test(description) ? "Drainage" : /road|pothole|access/i.test(description) ? "Road" : /pump|tank|water/i.test(description) ? "Water" : category;
  if (!assetId) {
    const found = await db2.query("SELECT id FROM assets WHERE ward_id=$1 AND data->>'type'=$2 ORDER BY id LIMIT 1", [`ward-${ward}`, type]);
    assetId = found[0]?.id || null;
  }
  return { assetId, severity: severe ? "HIGH" : medium ? "MEDIUM" : "LOW", confidence: severe ? 70 : 55, classificationMethod: "Deterministic keyword triage, not trained AI; field verification required" };
}

// src/server/app.ts
var pagination = z2.object({ page: z2.coerce.number().int().min(1).max(1e5).default(1), pageSize: z2.coerce.number().int().min(1).max(200).default(25), q: z2.string().max(200).default(""), type: z2.string().max(80).optional(), ward: z2.coerce.number().int().min(1).max(6).optional(), status: z2.string().max(80).optional(), sort: z2.enum(["id", "name", "type", "ward", "condition", "criticality", "risk", "pop", "cost"]).default("id"), direction: z2.enum(["asc", "desc"]).default("asc") });
var ok = (res, data, status = 200) => res.status(status).json({ success: true, data });
var user = (res) => res.locals.user;
var cookie = (req) => req.headers.cookie?.split(";").map((s) => s.trim()).find((s) => s.startsWith("gp_session="))?.slice(11);
var auth = (req, res, next) => res.locals.user ? next() : next(new AppError(401, "AUTH_REQUIRED", "Please sign in."));
var permit = (...allowed) => [auth, (_req, res, next) => allowed.includes(user(res).role) ? next() : next(new AppError(403, "FORBIDDEN", "Your role does not permit this action."))];
function supplied(parsed, body) {
  return Object.fromEntries(Object.entries(parsed).filter(([key]) => Object.hasOwn(body, key)));
}
var safeUser = (u) => ({ id: u.id, name: u.name, email: u.email, role: u.role, villageId: u.villageId, disabled: u.disabled });
function page(res, rows, query) {
  const p = pagination.parse(query);
  res.json({ success: true, data: rows.slice((p.page - 1) * p.pageSize, p.page * p.pageSize), pagination: { page: p.page, pageSize: p.pageSize, total: rows.length } });
}
function createApp(db2) {
  const app = express();
  app.disable("x-powered-by");
  app.set("trust proxy", 1);
  app.use(helmet({ contentSecurityPolicy: { directives: { defaultSrc: ["'self'"], scriptSrcAttr: ["'unsafe-inline'"], scriptSrc: ["'self'", "'unsafe-inline'"], styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"], imgSrc: ["'self'", "blob:", "data:"], connectSrc: ["'self'"], fontSrc: ["'self'", "https://fonts.gstatic.com"], objectSrc: ["'none'"], upgradeInsecureRequests: process.env.NODE_ENV === "production" ? [] : null } }, crossOriginEmbedderPolicy: false }));
  app.use((req, res, next) => {
    res.locals.requestId = randomUUID();
    res.setHeader("X-Request-ID", res.locals.requestId);
    const start = Date.now();
    res.on("finish", () => {
      if (process.env.NODE_ENV !== "test") console.log(JSON.stringify({ requestId: res.locals.requestId, method: req.method, path: req.path, status: res.statusCode, ms: Date.now() - start }));
    });
    next();
  });
  app.use("/api", rateLimit({ windowMs: 6e4, limit: 600, standardHeaders: "draft-8", legacyHeaders: false, message: { success: false, error: { code: "RATE_LIMITED", message: "Too many requests. Try again shortly." } } }));
  app.use("/api/uploads", express.raw({ type: ["image/jpeg", "image/png", "image/webp"], limit: "8mb" }));
  app.use(express.json({ limit: "256kb" }));
  app.use("/api", async (req, res, next) => {
    res.setHeader("Cache-Control", "no-store");
    res.locals.user = await session(db2, cookie(req));
    if (!["GET", "HEAD", "OPTIONS"].includes(req.method)) {
      const origin = req.headers.origin;
      const allowed = [process.env.APP_ORIGIN, process.env.RENDER_EXTERNAL_URL, `${req.protocol}://${req.get("host")}`].filter(Boolean).map((o) => String(o).replace(/\/+$/, ""));
      if (origin && !allowed.includes(origin)) throw new AppError(403, "ORIGIN_REJECTED", "This request origin is not allowed.");
      if (res.locals.user && req.path !== "/auth/login") {
        const expected = res.locals.user.csrf, provided = req.headers["x-csrf-token"];
        if (typeof provided !== "string" || provided.length !== expected.length || !timingSafeEqual2(Buffer.from(provided), Buffer.from(expected))) throw new AppError(403, "CSRF_REJECTED", "Refresh the page before trying again.");
      }
    }
    next();
  });
  app.get("/api/health", async (_req, res) => {
    await db2.query("SELECT 1");
    ok(res, { status: "ready" });
  });
  app.post("/api/auth/login", rateLimit({ windowMs: 15 * 6e4, limit: 30, message: { success: false, error: { code: "LOGIN_LIMIT", message: "Too many sign-in attempts. Try again later." } } }), async (req, res) => {
    const input = z2.object({ email: z2.email().max(200), password: z2.string().min(1).max(200) }).strict().parse(req.body);
    const rows = await db2.query(`SELECT id,name,email,role,village_id AS "villageId",disabled,password_hash FROM users WHERE email=$1`, [input.email.toLowerCase()]);
    const u = rows[0];
    if (!u || u.disabled || !await verifyPassword(input.password, u.password_hash)) throw new AppError(401, "INVALID_LOGIN", "Email or password is incorrect.");
    const value = token(), csrf = token();
    await db2.query("DELETE FROM sessions WHERE expires_at<now()");
    await db2.query("INSERT INTO sessions(token_hash,user_id,csrf,expires_at) VALUES($1,$2,$3,now()+interval '12 hours')", [tokenHash(value), u.id, csrf]);
    res.cookie("gp_session", value, { httpOnly: true, sameSite: "strict", secure: process.env.NODE_ENV === "production", maxAge: 12 * 36e5, path: "/" });
    await audit(db2, u, "User signed in", "user", u.id);
    ok(res, { user: safeUser(u), csrf });
  });
  app.get("/api/auth/me", auth, (_req, res) => ok(res, { user: safeUser(user(res)), csrf: res.locals.user.csrf }));
  app.post("/api/auth/logout", auth, async (req, res) => {
    await db2.query("DELETE FROM sessions WHERE token_hash=$1", [tokenHash(cookie(req) || "")]);
    res.clearCookie("gp_session", { path: "/" });
    ok(res, { loggedOut: true });
  });
  app.patch("/api/auth/profile", auth, async (req, res) => {
    const body = z2.object({ name: z2.string().trim().min(1).max(100), currentPassword: z2.string().max(200).optional(), password: z2.string().min(12).max(200).optional() }).strict().parse(req.body);
    await db2.transaction(async (tx) => {
      if (body.password) {
        const [u] = await tx.query("SELECT password_hash FROM users WHERE id=$1", [user(res).id]);
        if (!body.currentPassword || !await verifyPassword(body.currentPassword, u.password_hash)) throw new AppError(400, "PASSWORD_MISMATCH", "Current password is incorrect.");
        await tx.query("UPDATE users SET password_hash=$1 WHERE id=$2", [await hashPassword(body.password), user(res).id]);
        await tx.query("DELETE FROM sessions WHERE user_id=$1 AND token_hash<>$2", [user(res).id, tokenHash(cookie(req) || "")]);
      }
      await tx.query("UPDATE users SET name=$1 WHERE id=$2", [body.name, user(res).id]);
      await audit(tx, user(res), "Profile updated", "user", user(res).id);
    });
    ok(res, { ...safeUser(user(res)), name: body.name });
  });
  app.get("/api/state", auth, async (_req, res) => ok(res, await snapshot(db2, user(res))));
  app.get("/api/villages", auth, async (_req, res) => ok(res, [(await snapshot(db2, user(res))).village]));
  app.get("/api/villages/:id", auth, async (req, res) => {
    if (req.params.id !== user(res).villageId) throw new AppError(404, "NOT_FOUND", "Village not found.");
    ok(res, (await snapshot(db2, user(res))).village);
  });
  app.patch("/api/villages/:id", ...permit("admin"), async (req, res) => {
    if (req.params.id !== user(res).villageId) throw new AppError(404, "NOT_FOUND", "Village not found.");
    const input = z2.object({ name: z2.string().min(1).max(100), district: z2.string().min(1).max(100), state: z2.string().min(1).max(100), area: z2.number().positive().max(1e5) }).strict().parse(req.body);
    await db2.transaction(async (tx) => {
      await tx.query("UPDATE villages SET data=data||$1::jsonb,updated_at=now() WHERE id=$2", [JSON.stringify(input), req.params.id]);
      await changed(tx, user(res), "Village updated", "village", String(req.params.id));
    });
    ok(res, input);
  });
  app.get("/api/wards", auth, async (_req, res) => ok(res, (await snapshot(db2, user(res))).wards));
  app.patch("/api/wards/:id", ...permit("admin"), async (req, res) => {
    const input = z2.object({ name: z2.string().min(1).max(100), population: z2.number().int().min(0).max(1e6), households: z2.number().int().min(0).max(1e5) }).strict().parse(req.body);
    await db2.transaction(async (tx) => {
      const result = await tx.query("UPDATE wards SET data=data||$1::jsonb WHERE id=$2 AND village_id=$3 RETURNING id", [JSON.stringify(input), req.params.id, user(res).villageId]);
      if (!result.length) throw new AppError(404, "NOT_FOUND", "Ward not found.");
      await tx.query(`UPDATE villages SET data=data||jsonb_build_object('population',(SELECT sum((data->>'population')::integer) FROM wards WHERE village_id=$1),'households',(SELECT sum((data->>'households')::integer) FROM wards WHERE village_id=$1)),updated_at=now() WHERE id=$1`, [user(res).villageId]);
      await changed(tx, user(res), "Ward updated", "ward", String(req.params.id));
    });
    ok(res, input);
  });
  app.get(["/api/assets", "/api/villages/:id/assets", "/api/priorities"], auth, async (req, res) => {
    if (req.params.id && req.params.id !== user(res).villageId) throw new AppError(404, "NOT_FOUND", "Village not found.");
    const p = pagination.parse(req.query);
    const s = await snapshot(db2, user(res));
    const rows = s.assets.filter((a) => (!p.type || a.type === p.type) && (!p.ward || a.ward === p.ward) && `${a.id} ${a.name}`.toLowerCase().includes(p.q.toLowerCase()));
    const key = req.path === "/api/priorities" ? "criticality" : p.sort;
    rows.sort((a, b) => {
      const x = a[key], y = b[key];
      return (typeof x === "number" && typeof y === "number" ? x - y : String(x).localeCompare(String(y))) * (req.path === "/api/priorities" || p.direction === "desc" ? -1 : 1);
    });
    page(res, rows, req.query);
  });
  app.get("/api/assets/:id", auth, async (req, res) => {
    const s = await snapshot(db2, user(res)), a = s.assets.find((a2) => a2.id === req.params.id);
    if (!a) throw new AppError(404, "ASSET_NOT_FOUND", "Infrastructure asset was not found.");
    ok(res, a);
  });
  async function saveAsset(req, res) {
    const partial = req.method === "PATCH";
    const shape = assetSchema.extend({ connections: z2.array(id).max(200).optional() });
    const parsed = (partial ? shape.partial() : shape).parse(req.body);
    const { connections, ...input } = partial ? supplied(parsed, req.body) : parsed;
    const assetId = partial ? id.parse(req.params.id) : "AS-" + randomUUID();
    await db2.transaction(async (tx) => {
      const old = partial ? await requireAsset(tx, assetId) : null;
      const merged = { ...old, ...input };
      delete merged.id;
      const data = assetSchema.parse(merged);
      if (partial) await tx.query("UPDATE assets SET data=$1,ward_id=$2,updated_at=now() WHERE id=$3", [JSON.stringify(data), `ward-${data.ward}`, assetId]);
      else await tx.query("INSERT INTO assets(id,village_id,ward_id,data) VALUES($1,$2,$3,$4)", [assetId, user(res).villageId, `ward-${data.ward}`, JSON.stringify(data)]);
      if (connections) {
        for (const target of connections) {
          if (target === assetId) throw new AppError(400, "SELF_DEPENDENCY", "An asset cannot depend on itself.");
          await requireAsset(tx, target);
        }
        const existing = await tx.query("SELECT id,target FROM dependencies WHERE source=$1", [assetId]);
        for (const e of existing) if (!connections.includes(e.target)) await tx.query("DELETE FROM dependencies WHERE id=$1", [e.id]);
        for (const target of connections) if (!existing.some((e) => e.target === target)) await tx.query("INSERT INTO dependencies(id,source,target,type,strength,impact_weight) VALUES($1,$2,$3,$4,1,1)", ["DEP-" + randomUUID(), assetId, target, "affects"]);
      }
      await changed(tx, user(res), partial ? "Asset updated" : "Asset created", "asset", assetId);
    });
    ok(res, { id: assetId }, partial ? 200 : 201);
  }
  app.post("/api/assets", ...permit("admin"), saveAsset);
  app.patch("/api/assets/:id", ...permit("admin"), saveAsset);
  app.delete("/api/assets/:id", ...permit("admin"), async (req, res) => {
    await db2.transaction(async (tx) => {
      await requireAsset(tx, id.parse(req.params.id));
      await tx.query("DELETE FROM assets WHERE id=$1", [req.params.id]);
      await changed(tx, user(res), "Asset deleted", "asset", String(req.params.id));
    });
    ok(res, { deleted: true });
  });
  app.get("/api/dependencies", auth, async (req, res) => page(res, (await snapshot(db2, user(res))).dependencies, req.query));
  app.post("/api/dependencies", ...permit("admin"), async (req, res) => {
    const input = dependencySchema.parse(req.body), key = "DEP-" + randomUUID();
    await db2.transaction(async (tx) => {
      await requireAsset(tx, input.source);
      await requireAsset(tx, input.target);
      await tx.query("INSERT INTO dependencies(id,source,target,type,strength,impact_weight) VALUES($1,$2,$3,$4,$5,$6)", [key, input.source, input.target, input.type, input.strength, input.impactWeight]);
      await changed(tx, user(res), "Dependency created", "dependency", key);
    });
    ok(res, { id: key, ...input }, 201);
  });
  app.patch("/api/dependencies/:id", ...permit("admin"), async (req, res) => {
    const input = dependencySchema.parse(req.body);
    await db2.transaction(async (tx) => {
      await requireAsset(tx, input.source);
      await requireAsset(tx, input.target);
      const rows = await tx.query("UPDATE dependencies SET source=$1,target=$2,type=$3,strength=$4,impact_weight=$5 WHERE id=$6 RETURNING id", [input.source, input.target, input.type, input.strength, input.impactWeight, req.params.id]);
      if (!rows.length) throw new AppError(404, "NOT_FOUND", "Dependency not found.");
      await changed(tx, user(res), "Dependency updated", "dependency", String(req.params.id));
    });
    ok(res, input);
  });
  app.delete("/api/dependencies/:id", ...permit("admin"), async (req, res) => {
    await db2.transaction(async (tx) => {
      const rows = await tx.query("DELETE FROM dependencies WHERE id=$1 RETURNING id", [req.params.id]);
      if (!rows.length) throw new AppError(404, "NOT_FOUND", "Dependency not found.");
      await changed(tx, user(res), "Dependency deleted", "dependency", String(req.params.id));
    });
    ok(res, { deleted: true });
  });
  app.post("/api/uploads", auth, async (req, res) => {
    if (!Buffer.isBuffer(req.body) || !req.body.length) throw new AppError(400, "INVALID_IMAGE", "Upload a JPEG, PNG or WebP image.");
    let output;
    try {
      const metadata = await sharp(req.body, { limitInputPixels: 25e6 }).metadata();
      if (!["jpeg", "png", "webp"].includes(metadata.format)) throw new Error("Unsupported image content");
      output = await sharp(req.body, { limitInputPixels: 25e6, animated: false }).rotate().resize({ width: 1600, height: 1600, fit: "inside", withoutEnlargement: true }).webp({ quality: 82 }).toBuffer();
    } catch {
      throw new AppError(400, "INVALID_IMAGE", "The file is not a supported image or is too large.");
    }
    const key = randomUUID();
    await db2.query("INSERT INTO uploads(id,user_id,data,mime,size) VALUES($1,$2,$3,$4,$5)", [key, user(res).id, output, "image/webp", output.length]);
    ok(res, { id: key, url: "/api/uploads/" + key }, 201);
  });
  app.get("/api/uploads/:id", auth, async (req, res) => {
    const rows = await db2.query("SELECT data,user_id FROM uploads WHERE id=$1", [req.params.id]);
    const row = rows[0];
    if (!row) throw new AppError(404, "NOT_FOUND", "Image not found.");
    if (user(res).role !== "admin" && row.user_id !== user(res).id) {
      const allowed = await db2.query("SELECT id FROM reports WHERE image_id=$1 AND assigned_officer=$2", [req.params.id, user(res).id]);
      if (!allowed.length) throw new AppError(403, "FORBIDDEN", "This image is private.");
    }
    res.type("webp").send(row.data);
  });
  app.get("/api/reports", auth, async (req, res) => {
    const p = pagination.parse(req.query), s = await snapshot(db2, user(res));
    page(res, s.reports.filter((r) => (!p.status || r.status === p.status) && `${r.id} ${r.description} ${r.category}`.toLowerCase().includes(p.q.toLowerCase())), req.query);
  });
  app.post("/api/reports", auth, async (req, res) => {
    const input = reportSchema.parse(req.body), key = "CIT-" + randomUUID();
    let report;
    await db2.transaction(async (tx) => {
      if (input.imageId) {
        const [upload] = await tx.query("SELECT user_id FROM uploads WHERE id=$1", [input.imageId]);
        if (!upload || upload.user_id !== user(res).id) throw new AppError(400, "INVALID_IMAGE", "Upload an image from your account first.");
      }
      const classification = await classify(tx, input.category, input.description, input.ward, input.assetId);
      if (classification.assetId) {
        const a = await requireAsset(tx, classification.assetId);
        if (a.ward !== input.ward) throw new AppError(400, "WARD_MISMATCH", "The selected asset belongs to another ward.");
      }
      report = { ...input, ...classification, id: key, userId: user(res).id, status: "Pending verification", timestamp: (/* @__PURE__ */ new Date()).toISOString(), synthetic: false, assignedOfficer: null, resolutionNotes: "", photo: input.imageId ? "/api/uploads/" + input.imageId : "", verifiedAt: null };
      await tx.query("INSERT INTO reports(id,village_id,ward_id,asset_id,user_id,image_id,data) VALUES($1,$2,$3,$4,$5,$6,$7)", [key, user(res).villageId, `ward-${input.ward}`, classification.assetId, user(res).id, input.imageId, JSON.stringify(report)]);
      await changed(tx, user(res), "Citizen report submitted", "report", key, user(res).id);
    });
    ok(res, report, 201);
  });
  app.patch("/api/reports/:id", auth, async (req, res) => {
    const input = reportPatch.parse(req.body);
    await db2.transaction(async (tx) => {
      const rows = await tx.query("SELECT data,user_id,assigned_officer FROM reports WHERE id=$1 FOR UPDATE", [req.params.id]);
      const row = rows[0];
      if (!row) throw new AppError(404, "NOT_FOUND", "Report not found.");
      const u = user(res);
      if (u.role === "citizen" && (row.user_id !== u.id || Object.keys(input).some((k) => k !== "description"))) throw new AppError(403, "FORBIDDEN", "Citizens may edit only their own report descriptions.");
      if (u.role === "officer" && (row.assigned_officer !== u.id || input.assignedOfficer !== void 0)) throw new AppError(403, "FORBIDDEN", "Only your assigned reports may be updated.");
      if (input.assignedOfficer) {
        const officer = await tx.query("SELECT id FROM users WHERE id=$1 AND role='officer' AND NOT disabled AND village_id=$2", [input.assignedOfficer, u.villageId]);
        if (!officer.length) throw new AppError(400, "INVALID_OFFICER", "Select an active field officer.");
      }
      const data = { ...row.data, ...input, ...input.status === "Verified" ? { verifiedAt: (/* @__PURE__ */ new Date()).toISOString() } : {}, ...input.description ? await classify(tx, row.data.category, input.description, row.data.ward, row.data.assetId) : {} };
      await tx.query("UPDATE reports SET data=$1,assigned_officer=$2,updated_at=now() WHERE id=$3", [JSON.stringify(data), input.assignedOfficer === void 0 ? row.assigned_officer : input.assignedOfficer, req.params.id]);
      await changed(tx, u, "Report updated", "report", String(req.params.id), row.user_id);
    });
    ok(res, { updated: true });
  });
  app.delete("/api/reports/:id", ...permit("admin"), async (req, res) => {
    await db2.transaction(async (tx) => {
      const rows = await tx.query("DELETE FROM reports WHERE id=$1 RETURNING id", [req.params.id]);
      if (!rows.length) throw new AppError(404, "NOT_FOUND", "Report not found.");
      await changed(tx, user(res), "Report deleted", "report", String(req.params.id));
    });
    ok(res, { deleted: true });
  });
  app.get("/api/incidents", ...permit("admin", "officer"), async (req, res) => {
    const p = pagination.parse(req.query);
    page(res, (await snapshot(db2, user(res))).incidents.filter((i) => !p.status || i.status === p.status), req.query);
  });
  async function saveIncident(req, res) {
    const partial = req.method === "PATCH", input = partial ? supplied(incidentSchema.partial().parse(req.body), req.body) : incidentSchema.parse(req.body), key = partial ? id.parse(req.params.id) : "INC-" + randomUUID();
    await db2.transaction(async (tx) => {
      const [old] = partial ? await tx.query("SELECT data FROM incidents WHERE id=$1 FOR UPDATE", [key]) : [];
      if (partial && !old) throw new AppError(404, "NOT_FOUND", "Incident not found.");
      const merged = { ...old?.data, ...input };
      const data = incidentSchema.parse(Object.fromEntries(Object.keys(incidentSchema.shape).filter((k) => k in merged).map((k) => [k, merged[k]])));
      const a = await requireAsset(tx, data.assetId);
      const record = { ...data, date: data.date || (/* @__PURE__ */ new Date()).toISOString(), endTime: data.status === "Resolved" ? data.endTime || (/* @__PURE__ */ new Date()).toISOString() : null, category: data.category || a.type, affectedPopulation: a.pop, cost: a.cost };
      for (const affected of record.affectedAssets) await requireAsset(tx, affected);
      if (partial) await tx.query("UPDATE incidents SET data=$1,asset_id=$2,updated_at=now() WHERE id=$3", [JSON.stringify(record), data.assetId, key]);
      else await tx.query("INSERT INTO incidents(id,village_id,asset_id,data) VALUES($1,$2,$3,$4)", [key, user(res).villageId, data.assetId, JSON.stringify(record)]);
      await tx.query("DELETE FROM incident_assets WHERE incident_id=$1", [key]);
      for (const asset of /* @__PURE__ */ new Set([record.assetId, ...record.affectedAssets])) await tx.query("INSERT INTO incident_assets VALUES($1,$2)", [key, asset]);
      await changed(tx, user(res), "Incident " + (partial ? "updated" : "created"), "incident", key);
    });
    ok(res, { id: key }, partial ? 200 : 201);
  }
  app.post("/api/incidents", ...permit("admin", "officer"), saveIncident);
  app.patch("/api/incidents/:id", ...permit("admin", "officer"), saveIncident);
  app.get("/api/interventions", ...permit("admin"), async (req, res) => page(res, (await snapshot(db2, user(res))).interventions, req.query));
  app.post("/api/interventions", ...permit("admin"), async (req, res) => {
    const data = interventionSchema.parse(req.body), key = "PR-" + randomUUID();
    await db2.transaction(async (tx) => {
      const a = await requireAsset(tx, data.assetId);
      if (a.ward !== data.ward) throw new AppError(400, "WARD_MISMATCH", "Use the asset ward.");
      await tx.query("INSERT INTO interventions(id,village_id,asset_id,data) VALUES($1,$2,$3,$4)", [key, user(res).villageId, data.assetId, JSON.stringify(data)]);
      await changed(tx, user(res), "Intervention created", "intervention", key);
    });
    ok(res, { id: key }, 201);
  });
  app.patch("/api/interventions/:id", ...permit("admin"), async (req, res) => {
    const input = supplied(interventionSchema.partial().parse(req.body), req.body);
    await db2.transaction(async (tx) => {
      const [old] = await tx.query("SELECT data FROM interventions WHERE id=$1 FOR UPDATE", [req.params.id]);
      if (!old) throw new AppError(404, "NOT_FOUND", "Intervention not found.");
      const data = interventionSchema.parse({ ...old.data, ...input });
      const a = await requireAsset(tx, data.assetId);
      if (a.ward !== data.ward) throw new AppError(400, "WARD_MISMATCH", "Use the asset ward.");
      await tx.query("UPDATE interventions SET data=$1,asset_id=$2,updated_at=now() WHERE id=$3", [JSON.stringify(data), data.assetId, req.params.id]);
      await changed(tx, user(res), "Intervention updated", "intervention", String(req.params.id));
    });
    ok(res, { updated: true });
  });
  app.get("/api/users", ...permit("admin"), async (req, res) => page(res, await db2.query("SELECT id,name,email,role,disabled FROM users WHERE village_id=$1 ORDER BY name", [user(res).villageId]), req.query));
  app.post("/api/users", ...permit("admin"), async (req, res) => {
    const input = z2.object({ name: z2.string().min(1).max(100), email: z2.email().max(200), password: z2.string().min(12).max(200), role: z2.enum(roles) }).strict().parse(req.body), key = "USR-" + randomUUID(), hash = await hashPassword(input.password);
    await db2.transaction(async (tx) => {
      await tx.query("INSERT INTO users(id,village_id,email,name,role,password_hash) VALUES($1,$2,$3,$4,$5,$6)", [key, user(res).villageId, input.email.toLowerCase(), input.name, input.role, hash]);
      await audit(tx, user(res), "User created", "user", key);
    });
    ok(res, { id: key }, 201);
  });
  app.patch("/api/users/:id", ...permit("admin"), async (req, res) => {
    const input = z2.object({ name: z2.string().min(1).max(100).optional(), role: z2.enum(roles).optional(), disabled: z2.boolean().optional() }).strict().parse(req.body);
    if (req.params.id === user(res).id && (input.disabled || input.role && input.role !== "admin")) throw new AppError(400, "SELF_LOCKOUT", "Another administrator must change your access.");
    await db2.transaction(async (tx) => {
      const rows = await tx.query("UPDATE users SET name=COALESCE($1,name),role=COALESCE($2,role),disabled=COALESCE($3,disabled) WHERE id=$4 AND village_id=$5 RETURNING id", [input.name ?? null, input.role ?? null, input.disabled ?? null, req.params.id, user(res).villageId]);
      if (!rows.length) throw new AppError(404, "NOT_FOUND", "User not found.");
      await tx.query("DELETE FROM sessions WHERE user_id=$1", [req.params.id]);
      await audit(tx, user(res), "User permissions changed", "user", String(req.params.id));
    });
    ok(res, { updated: true });
  });
  app.post(["/api/simulations/failure", "/api/rice/analyze"], ...permit("admin"), async (req, res) => {
    const input = z2.object({ scenario: id.default("custom"), assetIds: z2.array(id).max(30).default([]), rain: z2.number().min(-20).max(100).default(20), apply: z2.boolean().default(false) }).strict().parse(req.body);
    let result;
    await db2.transaction(async (tx) => {
      const s = await snapshot(tx, user(res));
      let starters = input.assetIds;
      if (!starters.length) starters = input.scenario === "multi" ? ["D04", "R07", "W02"] : input.scenario === "rain" ? s.assets.filter((a) => a.type === "Drainage" && a.hazard * (1 + input.rain / 100) > 50).map((a) => a.id) : input.scenario === "power" ? s.assets.filter((a) => a.type === "Streetlight" || /^W0/.test(a.id)).map((a) => a.id) : input.scenario === "supply" ? s.assets.filter((a) => /^W0/.test(a.id)).map((a) => a.id) : [input.scenario];
      for (const asset of starters) await requireAsset(tx, asset);
      if (!starters.length) throw new AppError(400, "NO_STARTERS", "No initiating assets meet the scenario threshold.");
      result = rice(s.assets, s.dependencies, s.wards, starters, input.rain, input.scenario);
      const key = randomUUID();
      await tx.query("INSERT INTO simulations(id,user_id,village_id,kind,input,result) VALUES($1,$2,$3,$4,$5,$6)", [key, user(res).id, user(res).villageId, "failure", JSON.stringify(input), JSON.stringify(result)]);
      if (input.apply) await tx.query("UPDATE settings SET active_simulation=$1 WHERE village_id=$2", [key, user(res).villageId]);
      await changed(tx, user(res), "Failure simulation completed", "simulation", key);
    });
    ok(res, result);
  });
  app.delete("/api/simulations/active", ...permit("admin"), async (_req, res) => {
    await db2.transaction(async (tx) => {
      await tx.query("UPDATE settings SET active_simulation=NULL WHERE village_id=$1", [user(res).villageId]);
      await changed(tx, user(res), "Simulation overlay cleared", "simulation", null);
    });
    ok(res, { cleared: true });
  });
  app.get("/api/trace/:id", auth, async (req, res) => {
    const s = await snapshot(db2, user(res));
    await requireAsset(db2, id.parse(req.params.id));
    ok(res, rice(s.assets, s.dependencies, s.wards, [String(req.params.id)], 0));
  });
  app.post("/api/budget/optimize", ...permit("admin"), async (req, res) => {
    const input = z2.object({ budget: z2.number().positive().max(1e3), minProjects: z2.number().int().min(0).max(30).default(0), maxProjects: z2.number().int().min(1).max(30).default(10), ward: z2.number().int().min(1).max(6).optional(), category: z2.string().max(80).optional(), minimumPriority: z2.number().min(0).max(100).default(0), strategy: z2.enum(["impact", "cost"]).default("impact") }).strict().refine((x) => x.minProjects <= x.maxProjects, "Minimum count exceeds maximum").parse(req.body);
    let result;
    await db2.transaction(async (tx) => {
      const s = await snapshot(tx, user(res));
      result = optimize(s.assets, s.interventions, s.wards, input);
      const key = randomUUID();
      await tx.query("INSERT INTO simulations(id,user_id,village_id,kind,input,result) VALUES($1,$2,$3,$4,$5,$6)", [key, user(res).id, user(res).villageId, "budget", JSON.stringify(input), JSON.stringify(result)]);
      await audit(tx, user(res), "Budget optimized", "simulation", key);
      await tx.query("INSERT INTO notifications(village_id,message) VALUES($1,$2)", [user(res).villageId, "Budget plan generated"]);
    });
    ok(res, result);
  });
  app.post("/api/simulations/what-if", ...permit("admin"), async (req, res) => {
    const input = z2.object({ rain: z2.number().min(-20).max(100), population: z2.number().min(-50).max(100), assetId: id, condition: z2.number().min(-100).max(100), capacity: z2.number().min(0.1).max(10).default(1), newFacility: z2.object({ type: assetSchema.shape.type, ward: z2.number().int().min(1).max(6), latitude: z2.number().min(-90).max(90), longitude: z2.number().min(-180).max(180), capacity: z2.number().positive().max(1e5) }).optional(), relocate: z2.object({ assetId: id, latitude: z2.number().min(-90).max(90), longitude: z2.number().min(-180).max(180) }).optional() }).strict().parse(req.body);
    let result;
    await db2.transaction(async (tx) => {
      await requireAsset(tx, input.assetId);
      if (input.relocate) await requireAsset(tx, input.relocate.assetId);
      const s = await snapshot(tx, user(res));
      result = whatIf(s.assets, s.dependencies, s.wards, s.reports, s.incidents, s.weights, input);
      await tx.query("INSERT INTO simulations(id,user_id,village_id,kind,input,result) VALUES($1,$2,$3,$4,$5,$6)", [randomUUID(), user(res).id, user(res).villageId, "what-if", JSON.stringify(input), JSON.stringify(result)]);
      await audit(tx, user(res), "What-if calculated", "simulation", null);
    });
    ok(res, result);
  });
  app.get("/api/simulations", ...permit("admin"), async (req, res) => {
    const p = pagination.parse(req.query);
    const [{ total }] = await db2.query("SELECT count(*) AS total FROM simulations WHERE village_id=$1", [user(res).villageId]);
    const rows = await db2.query("SELECT id,kind,input,result,created_at FROM simulations WHERE village_id=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3", [user(res).villageId, p.pageSize, (p.page - 1) * p.pageSize]);
    res.json({ success: true, data: rows, pagination: { page: p.page, pageSize: p.pageSize, total: Number(total) } });
  });
  app.patch("/api/settings/weights", ...permit("admin"), async (req, res) => {
    const weights = weightsSchema.parse(req.body);
    await db2.transaction(async (tx) => {
      await tx.query("UPDATE settings SET weights=$1 WHERE village_id=$2", [JSON.stringify(weights), user(res).villageId]);
      await changed(tx, user(res), "Model weights updated", "settings", null);
    });
    ok(res, weights);
  });
  app.get(["/api/analytics/pulse", "/api/analytics/dashboard"], auth, async (_req, res) => {
    const s = await snapshot(db2, user(res));
    ok(res, { pulse: s.pulse, categories: s.categories, history: s.pulseHistory, assets: s.assets.length, critical: s.assets.filter((a) => a.risk === "High").length, reports: s.reports.length, unresolved: s.reports.filter((r) => !["Resolved", "Rejected"].includes(r.status)).length, incidents: s.incidents.length, wards: s.wards.map((w) => ({ ...w, risk: s.assets.filter((a) => a.ward === w.number).reduce((n, a) => n + a.critical, 0) / Math.max(1, s.assets.filter((a) => a.ward === w.number).length) })) });
  });
  app.get("/api/search", auth, async (req, res) => {
    const q = z2.string().max(200).parse(req.query.q || "");
    const s = await snapshot(db2, user(res));
    const records2 = [...s.assets.map((a) => ({ id: a.id, title: a.name, kind: "Asset", page: "assets" })), ...s.reports.map((r) => ({ id: r.id, title: r.description, kind: "Report", page: "reports" })), ...s.incidents.map((i) => ({ id: i.id, title: i.description, kind: "Incident", page: "incidents" })), ...s.wards.map((w) => ({ id: w.id, title: w.name, kind: "Ward", page: "management" })), ...s.interventions.map((p) => ({ id: p.id, title: p.name, kind: "Intervention", page: "management" }))];
    ok(res, records2.filter((r) => `${r.id} ${r.title}`.toLowerCase().includes(q.toLowerCase())).slice(0, 30));
  });
  app.get("/api/audit", ...permit("admin"), async (req, res) => {
    const p = pagination.parse(req.query), [{ total }] = await db2.query("SELECT count(*) AS total FROM audit_log WHERE village_id=$1", [user(res).villageId]);
    res.json({ success: true, data: await db2.query("SELECT a.id,a.action,a.entity,a.entity_id,a.created_at,u.name FROM audit_log a LEFT JOIN users u ON u.id=a.user_id WHERE a.village_id=$1 ORDER BY a.id DESC LIMIT $2 OFFSET $3", [user(res).villageId, p.pageSize, (p.page - 1) * p.pageSize]), pagination: { page: p.page, pageSize: p.pageSize, total: Number(total) } });
  });
  app.get("/api/notifications", auth, async (_req, res) => ok(res, await db2.query(`SELECT n.id,n.message,n.created_at,(r.user_id IS NOT NULL) AS read FROM notifications n LEFT JOIN notification_reads r ON r.notification_id=n.id AND r.user_id=$1 WHERE n.village_id=$2 AND (n.user_id=$1 OR (n.user_id IS NULL AND $3<>'citizen')) ORDER BY n.id DESC LIMIT 100`, [user(res).id, user(res).villageId, user(res).role])));
  app.post("/api/notifications/:id/read", auth, async (req, res) => {
    const rows = await db2.query(`SELECT id FROM notifications WHERE id=$1 AND village_id=$2 AND (user_id=$3 OR (user_id IS NULL AND $4<>'citizen'))`, [z2.coerce.number().int().positive().parse(req.params.id), user(res).villageId, user(res).id, user(res).role]);
    if (!rows.length) throw new AppError(404, "NOT_FOUND", "Notification not found.");
    await db2.query("INSERT INTO notification_reads VALUES($1,$2) ON CONFLICT DO NOTHING", [req.params.id, user(res).id]);
    ok(res, { read: true });
  });
  app.post("/api/demo/reports", ...permit("admin"), async (req, res) => {
    const { count } = z2.object({ count: z2.number().int().min(1).max(20) }).strict().parse(req.body);
    await db2.transaction(async (tx) => {
      const s = await snapshot(tx, user(res));
      for (let i = 0; i < count; i++) {
        const a = s.assets[i % s.assets.length], key = "DEMO-" + randomUUID();
        await tx.query("INSERT INTO reports(id,village_id,ward_id,asset_id,user_id,data) VALUES($1,$2,$3,$4,$5,$6)", [key, user(res).villageId, `ward-${a.ward}`, a.id, user(res).id, JSON.stringify({ category: a.type, description: `Generated demo evidence: blocked or damaged infrastructure near ${a.name}.`, ward: a.ward, severity: "HIGH", confidence: 70, status: "Pending verification", timestamp: (/* @__PURE__ */ new Date()).toISOString(), synthetic: true, resolutionNotes: "", verifiedAt: null, classificationMethod: "Generated demo evidence" })]);
      }
      await changed(tx, user(res), "Demo reports generated", "report", null);
    });
    ok(res, { created: count }, 201);
  });
  app.post("/api/demo/reset", ...permit("admin"), async (_req, res) => {
    await db2.transaction(async (tx) => {
      await tx.query("DELETE FROM reports WHERE id LIKE 'DEMO-%'");
      await tx.query("UPDATE settings SET weights=$1,active_simulation=NULL WHERE village_id=$2", [JSON.stringify(defaultWeights), user(res).villageId]);
      await changed(tx, user(res), "Generated demo reports and scenario reset; user records preserved", "settings", null);
    });
    ok(res, { reset: true });
  });
  app.get("/api/export", ...permit("admin"), async (_req, res) => ok(res, await snapshot(db2, user(res))));
  app.use("/api", (_req, _res, next) => next(new AppError(404, "NOT_FOUND", "API endpoint not found.")));
  const publicDir = path.join(path.dirname(fileURLToPath(import.meta.url)), "public");
  app.use(express.static(publicDir, { index: "index.html" }));
  app.get("/{*path}", (_req, res) => res.sendFile(path.join(publicDir, "index.html")));
  app.use((err, _req, res, _next) => {
    let status = 500, code = "INTERNAL_ERROR", message = "Unable to complete the request. Please retry.";
    if (err instanceof z2.ZodError) {
      status = 400;
      code = "VALIDATION_ERROR";
      message = err.issues.map((i) => `${i.path.join(".") || "request"}: ${i.message}`).join("; ");
    } else if (err instanceof AppError) {
      ({ status, code, message } = err);
    } else if (typeof err === "object" && err !== null) {
      const e = err;
      if (e.code === "23503") {
        status = 409;
        code = "RELATED_RECORDS";
        message = "This record is linked to other records. Reassign them or retire the asset.";
      }
      if (e.code === "23505") {
        status = 409;
        code = "DUPLICATE";
        message = "A record with these values already exists.";
      }
      if (e.status === 413) {
        status = 413;
        code = "TOO_LARGE";
        message = "The upload or request exceeds the size limit.";
      }
      if (e.status === 400) {
        status = 400;
        code = "INVALID_REQUEST";
        message = "The request body is not valid.";
      }
    }
    if (status === 500) console.error(JSON.stringify({ requestId: res.locals.requestId, error: err instanceof Error ? err.message : String(err) }));
    res.status(status).json({ success: false, error: { code, message, requestId: res.locals.requestId } });
  });
  return app;
}

// src/server/seed-data.ts
var SEED_ASSETS = [
  { "id": "D04", "name": "Drain D04", "type": "Drainage", "ward": 4, "condition": 38, "pop": 910, "cost": 1.2, "hazard": 86, "action": "Desilt and repair drain", "x": 59, "y": 58 },
  { "id": "R07", "name": "Road R07", "type": "Road", "ward": 4, "condition": 52, "pop": 1240, "cost": 3.8, "hazard": 74, "action": "Patch and restore access", "x": 48, "y": 50 },
  { "id": "W02", "name": "Water Tank W02", "type": "Water", "ward": 3, "condition": 63, "pop": 1100, "cost": 2.4, "hazard": 36, "action": "Inspect pump and storage", "x": 37, "y": 36 },
  { "id": "S02", "name": "Primary School S02", "type": "Education", "ward": 4, "condition": 76, "pop": 430, "cost": 1.4, "hazard": 53, "action": "Repair access approach", "x": 69, "y": 39 },
  { "id": "PHC01", "name": "Primary Health Centre", "type": "Healthcare", "ward": 2, "condition": 83, "pop": 3e3, "cost": 2.1, "hazard": 28, "action": "Maintain all-weather access", "x": 26, "y": 64 },
  { "id": "SL02", "name": "Streetlight cluster SL02", "type": "Sanitation", "ward": 2, "condition": 49, "pop": 250, "cost": 0.8, "hazard": 12, "action": "Replace failed luminaires", "x": 28, "y": 28 },
  { "id": "D09", "name": "Drain D09", "type": "Drainage", "ward": 6, "condition": 47, "pop": 520, "cost": 0.9, "hazard": 63, "action": "Clear silt and inspect outfall", "x": 78, "y": 71 },
  { "id": "WP06", "name": "Water Point WP06", "type": "Water", "ward": 6, "condition": 45, "pop": 360, "cost": 1.1, "hazard": 30, "action": "Restore community water point", "x": 83, "y": 49 },
  { "id": "H03", "name": "Hand Pump H03", "type": "Water", "ward": 3, "condition": 68, "pop": 180, "cost": 0.55, "hazard": 18, "action": "Preventive pump servicing", "x": 44, "y": 29 },
  { "id": "R03", "name": "Road R03", "type": "Road", "ward": 2, "condition": 74, "pop": 490, "cost": 1.6, "hazard": 22, "action": "Routine surface maintenance", "x": 34, "y": 48 },
  { "id": "R11", "name": "Road R11", "type": "Road", "ward": 6, "condition": 69, "pop": 610, "cost": 2.2, "hazard": 43, "action": "Repair edge and shoulder", "x": 73, "y": 60 },
  { "id": "A01", "name": "Anganwadi A01", "type": "Education", "ward": 1, "condition": 88, "pop": 120, "cost": 0.65, "hazard": 15, "action": "Routine maintenance", "x": 22, "y": 39 },
  { "id": "S01", "name": "Secondary School S01", "type": "Education", "ward": 1, "condition": 92, "pop": 620, "cost": 1, "hazard": 11, "action": "Routine maintenance", "x": 16, "y": 49 },
  { "id": "D02", "name": "Drain D02", "type": "Drainage", "ward": 1, "condition": 72, "pop": 410, "cost": 0.7, "hazard": 32, "action": "Seasonal desilting", "x": 17, "y": 68 },
  { "id": "D12", "name": "Drain D12", "type": "Drainage", "ward": 5, "condition": 60, "pop": 440, "cost": 0.75, "hazard": 47, "action": "Inspect and clear outfall", "x": 60, "y": 78 },
  { "id": "W01", "name": "Water Tank W01", "type": "Water", "ward": 1, "condition": 88, "pop": 970, "cost": 2, "hazard": 10, "action": "Routine tank inspection", "x": 17, "y": 28 },
  { "id": "R21", "name": "Road R21", "type": "Road", "ward": 2, "condition": 69, "pop": 253, "cost": 1.15, "hazard": 25, "action": "Routine road maintenance", "x": 32, "y": 41 },
  { "id": "R22", "name": "Road R22", "type": "Road", "ward": 3, "condition": 76, "pop": 346, "cost": 1.5, "hazard": 38, "action": "Routine road maintenance", "x": 49, "y": 60 },
  { "id": "R23", "name": "Road R23", "type": "Road", "ward": 4, "condition": 83, "pop": 439, "cost": 1.8499999999999999, "hazard": 51, "action": "Routine road maintenance", "x": 66, "y": 79 },
  { "id": "R24", "name": "Road R24", "type": "Road", "ward": 5, "condition": 90, "pop": 532, "cost": 0.8, "hazard": 16, "action": "Routine road maintenance", "x": 83, "y": 38 },
  { "id": "R25", "name": "Road R25", "type": "Road", "ward": 6, "condition": 63, "pop": 625, "cost": 1.15, "hazard": 29, "action": "Routine road maintenance", "x": 30, "y": 57 },
  { "id": "R26", "name": "Road R26", "type": "Road", "ward": 1, "condition": 70, "pop": 168, "cost": 1.5, "hazard": 42, "action": "Routine road maintenance", "x": 47, "y": 76 },
  { "id": "R27", "name": "Road R27", "type": "Road", "ward": 2, "condition": 77, "pop": 261, "cost": 1.8499999999999999, "hazard": 55, "action": "Routine road maintenance", "x": 64, "y": 35 },
  { "id": "R28", "name": "Road R28", "type": "Road", "ward": 3, "condition": 84, "pop": 354, "cost": 0.8, "hazard": 20, "action": "Routine road maintenance", "x": 81, "y": 54 },
  { "id": "R29", "name": "Road R29", "type": "Road", "ward": 4, "condition": 91, "pop": 447, "cost": 1.15, "hazard": 33, "action": "Routine road maintenance", "x": 28, "y": 73 },
  { "id": "R30", "name": "Road R30", "type": "Road", "ward": 5, "condition": 64, "pop": 540, "cost": 1.5, "hazard": 46, "action": "Routine road maintenance", "x": 45, "y": 32 },
  { "id": "R31", "name": "Road R31", "type": "Road", "ward": 6, "condition": 71, "pop": 633, "cost": 1.8499999999999999, "hazard": 59, "action": "Routine road maintenance", "x": 62, "y": 51 },
  { "id": "R32", "name": "Road R32", "type": "Road", "ward": 1, "condition": 78, "pop": 176, "cost": 0.8, "hazard": 24, "action": "Routine road maintenance", "x": 79, "y": 70 },
  { "id": "R33", "name": "Road R33", "type": "Road", "ward": 2, "condition": 85, "pop": 269, "cost": 1.15, "hazard": 37, "action": "Routine road maintenance", "x": 26, "y": 29 },
  { "id": "R34", "name": "Road R34", "type": "Road", "ward": 3, "condition": 92, "pop": 362, "cost": 1.5, "hazard": 50, "action": "Routine road maintenance", "x": 43, "y": 48 },
  { "id": "R35", "name": "Road R35", "type": "Road", "ward": 4, "condition": 65, "pop": 455, "cost": 1.8499999999999999, "hazard": 15, "action": "Routine road maintenance", "x": 60, "y": 67 },
  { "id": "R36", "name": "Road R36", "type": "Road", "ward": 5, "condition": 72, "pop": 548, "cost": 0.8, "hazard": 28, "action": "Routine road maintenance", "x": 77, "y": 26 },
  { "id": "R37", "name": "Road R37", "type": "Road", "ward": 6, "condition": 79, "pop": 641, "cost": 1.15, "hazard": 41, "action": "Routine road maintenance", "x": 24, "y": 45 },
  { "id": "R38", "name": "Road R38", "type": "Road", "ward": 1, "condition": 86, "pop": 184, "cost": 1.5, "hazard": 54, "action": "Routine road maintenance", "x": 41, "y": 64 },
  { "id": "R39", "name": "Road R39", "type": "Road", "ward": 2, "condition": 93, "pop": 277, "cost": 1.8499999999999999, "hazard": 19, "action": "Routine road maintenance", "x": 58, "y": 23 },
  { "id": "R40", "name": "Road R40", "type": "Road", "ward": 3, "condition": 66, "pop": 370, "cost": 0.8, "hazard": 32, "action": "Routine road maintenance", "x": 75, "y": 42 },
  { "id": "R41", "name": "Road R41", "type": "Road", "ward": 4, "condition": 73, "pop": 463, "cost": 1.15, "hazard": 45, "action": "Routine road maintenance", "x": 22, "y": 61 },
  { "id": "R42", "name": "Road R42", "type": "Road", "ward": 5, "condition": 80, "pop": 556, "cost": 1.5, "hazard": 58, "action": "Routine road maintenance", "x": 39, "y": 80 },
  { "id": "R43", "name": "Road R43", "type": "Road", "ward": 6, "condition": 87, "pop": 649, "cost": 1.8499999999999999, "hazard": 23, "action": "Routine road maintenance", "x": 56, "y": 39 },
  { "id": "R44", "name": "Road R44", "type": "Road", "ward": 1, "condition": 94, "pop": 192, "cost": 0.8, "hazard": 36, "action": "Routine road maintenance", "x": 73, "y": 58 },
  { "id": "R45", "name": "Road R45", "type": "Road", "ward": 2, "condition": 67, "pop": 285, "cost": 1.15, "hazard": 49, "action": "Routine road maintenance", "x": 20, "y": 77 },
  { "id": "D21", "name": "Drain D21", "type": "Drainage", "ward": 2, "condition": 65, "pop": 197, "cost": 0.65, "hazard": 24, "action": "Seasonal drain maintenance", "x": 33, "y": 38 },
  { "id": "D22", "name": "Drain D22", "type": "Drainage", "ward": 3, "condition": 76, "pop": 264, "cost": 0.8500000000000001, "hazard": 33, "action": "Seasonal drain maintenance", "x": 54, "y": 51 },
  { "id": "D23", "name": "Drain D23", "type": "Drainage", "ward": 4, "condition": 87, "pop": 331, "cost": 0.45, "hazard": 42, "action": "Seasonal drain maintenance", "x": 75, "y": 64 },
  { "id": "D24", "name": "Drain D24", "type": "Drainage", "ward": 5, "condition": 58, "pop": 398, "cost": 0.65, "hazard": 51, "action": "Seasonal drain maintenance", "x": 20, "y": 77 },
  { "id": "D25", "name": "Drain D25", "type": "Drainage", "ward": 6, "condition": 69, "pop": 465, "cost": 0.8500000000000001, "hazard": 60, "action": "Seasonal drain maintenance", "x": 41, "y": 28 },
  { "id": "D26", "name": "Drain D26", "type": "Drainage", "ward": 1, "condition": 80, "pop": 142, "cost": 0.45, "hazard": 19, "action": "Seasonal drain maintenance", "x": 62, "y": 41 },
  { "id": "D27", "name": "Drain D27", "type": "Drainage", "ward": 2, "condition": 91, "pop": 209, "cost": 0.65, "hazard": 28, "action": "Seasonal drain maintenance", "x": 83, "y": 54 },
  { "id": "D28", "name": "Drain D28", "type": "Drainage", "ward": 3, "condition": 62, "pop": 276, "cost": 0.8500000000000001, "hazard": 37, "action": "Seasonal drain maintenance", "x": 28, "y": 67 },
  { "id": "D29", "name": "Drain D29", "type": "Drainage", "ward": 4, "condition": 73, "pop": 343, "cost": 0.45, "hazard": 46, "action": "Seasonal drain maintenance", "x": 49, "y": 80 },
  { "id": "D30", "name": "Drain D30", "type": "Drainage", "ward": 5, "condition": 84, "pop": 410, "cost": 0.65, "hazard": 55, "action": "Seasonal drain maintenance", "x": 70, "y": 31 },
  { "id": "D31", "name": "Drain D31", "type": "Drainage", "ward": 6, "condition": 55, "pop": 477, "cost": 0.8500000000000001, "hazard": 64, "action": "Seasonal drain maintenance", "x": 15, "y": 44 },
  { "id": "D32", "name": "Drain D32", "type": "Drainage", "ward": 1, "condition": 66, "pop": 154, "cost": 0.45, "hazard": 23, "action": "Seasonal drain maintenance", "x": 36, "y": 57 },
  { "id": "D33", "name": "Drain D33", "type": "Drainage", "ward": 2, "condition": 77, "pop": 221, "cost": 0.65, "hazard": 32, "action": "Seasonal drain maintenance", "x": 57, "y": 70 },
  { "id": "D34", "name": "Drain D34", "type": "Drainage", "ward": 3, "condition": 88, "pop": 288, "cost": 0.8500000000000001, "hazard": 41, "action": "Seasonal drain maintenance", "x": 78, "y": 83 },
  { "id": "D35", "name": "Drain D35", "type": "Drainage", "ward": 4, "condition": 59, "pop": 355, "cost": 0.45, "hazard": 50, "action": "Seasonal drain maintenance", "x": 23, "y": 34 },
  { "id": "D36", "name": "Drain D36", "type": "Drainage", "ward": 5, "condition": 70, "pop": 422, "cost": 0.65, "hazard": 59, "action": "Seasonal drain maintenance", "x": 44, "y": 47 },
  { "id": "D37", "name": "Drain D37", "type": "Drainage", "ward": 6, "condition": 81, "pop": 489, "cost": 0.8500000000000001, "hazard": 18, "action": "Seasonal drain maintenance", "x": 65, "y": 60 },
  { "id": "D38", "name": "Drain D38", "type": "Drainage", "ward": 1, "condition": 92, "pop": 166, "cost": 0.45, "hazard": 27, "action": "Seasonal drain maintenance", "x": 86, "y": 73 },
  { "id": "D39", "name": "Drain D39", "type": "Drainage", "ward": 2, "condition": 63, "pop": 233, "cost": 0.65, "hazard": 36, "action": "Seasonal drain maintenance", "x": 31, "y": 86 },
  { "id": "D40", "name": "Drain D40", "type": "Drainage", "ward": 3, "condition": 74, "pop": 300, "cost": 0.8500000000000001, "hazard": 45, "action": "Seasonal drain maintenance", "x": 52, "y": 37 },
  { "id": "D41", "name": "Drain D41", "type": "Drainage", "ward": 4, "condition": 85, "pop": 367, "cost": 0.45, "hazard": 54, "action": "Seasonal drain maintenance", "x": 73, "y": 50 },
  { "id": "D42", "name": "Drain D42", "type": "Drainage", "ward": 5, "condition": 56, "pop": 434, "cost": 0.65, "hazard": 63, "action": "Seasonal drain maintenance", "x": 18, "y": 63 },
  { "id": "D43", "name": "Drain D43", "type": "Drainage", "ward": 6, "condition": 67, "pop": 501, "cost": 0.8500000000000001, "hazard": 22, "action": "Seasonal drain maintenance", "x": 39, "y": 76 },
  { "id": "D44", "name": "Drain D44", "type": "Drainage", "ward": 1, "condition": 78, "pop": 178, "cost": 0.45, "hazard": 31, "action": "Seasonal drain maintenance", "x": 60, "y": 27 },
  { "id": "D45", "name": "Drain D45", "type": "Drainage", "ward": 2, "condition": 89, "pop": 245, "cost": 0.65, "hazard": 40, "action": "Seasonal drain maintenance", "x": 81, "y": 40 },
  { "id": "HP01", "name": "Hand Pump H01", "type": "Water", "ward": 2, "condition": 68, "pop": 74, "cost": 0.24, "hazard": 19, "action": "Preventive inspection", "x": 33, "y": 37 },
  { "id": "HP02", "name": "Hand Pump H02", "type": "Water", "ward": 3, "condition": 75, "pop": 113, "cost": 0.36, "hazard": 30, "action": "Preventive inspection", "x": 56, "y": 54 },
  { "id": "HP03", "name": "Hand Pump H03", "type": "Water", "ward": 4, "condition": 82, "pop": 152, "cost": 0.48, "hazard": 41, "action": "Preventive inspection", "x": 79, "y": 71 },
  { "id": "HP04", "name": "Hand Pump H04", "type": "Water", "ward": 5, "condition": 89, "pop": 41, "cost": 0.6, "hazard": 17, "action": "Preventive inspection", "x": 22, "y": 23 },
  { "id": "HP05", "name": "Sanitation point S05", "type": "Sanitation", "ward": 6, "condition": 96, "pop": 80, "cost": 0.12, "hazard": 28, "action": "Sanitation upkeep", "x": 45, "y": 40 },
  { "id": "HP06", "name": "Hand Pump H06", "type": "Water", "ward": 1, "condition": 66, "pop": 119, "cost": 0.24, "hazard": 39, "action": "Preventive inspection", "x": 68, "y": 57 },
  { "id": "HP07", "name": "Hand Pump H07", "type": "Water", "ward": 2, "condition": 73, "pop": 158, "cost": 0.36, "hazard": 15, "action": "Preventive inspection", "x": 11, "y": 74 },
  { "id": "HP08", "name": "Hand Pump H08", "type": "Water", "ward": 3, "condition": 80, "pop": 47, "cost": 0.48, "hazard": 26, "action": "Preventive inspection", "x": 34, "y": 26 },
  { "id": "HP09", "name": "Hand Pump H09", "type": "Water", "ward": 4, "condition": 87, "pop": 86, "cost": 0.6, "hazard": 37, "action": "Preventive inspection", "x": 57, "y": 43 },
  { "id": "HP10", "name": "Sanitation point S10", "type": "Sanitation", "ward": 5, "condition": 94, "pop": 125, "cost": 0.12, "hazard": 13, "action": "Sanitation upkeep", "x": 80, "y": 60 },
  { "id": "HP11", "name": "Hand Pump H11", "type": "Water", "ward": 6, "condition": 64, "pop": 164, "cost": 0.24, "hazard": 24, "action": "Preventive inspection", "x": 23, "y": 77 },
  { "id": "HP12", "name": "Hand Pump H12", "type": "Water", "ward": 1, "condition": 71, "pop": 53, "cost": 0.36, "hazard": 35, "action": "Preventive inspection", "x": 46, "y": 29 },
  { "id": "HP13", "name": "Hand Pump H13", "type": "Water", "ward": 2, "condition": 78, "pop": 92, "cost": 0.48, "hazard": 11, "action": "Preventive inspection", "x": 69, "y": 46 },
  { "id": "HP14", "name": "Hand Pump H14", "type": "Water", "ward": 3, "condition": 85, "pop": 131, "cost": 0.6, "hazard": 22, "action": "Preventive inspection", "x": 12, "y": 63 },
  { "id": "HP15", "name": "Sanitation point S15", "type": "Sanitation", "ward": 4, "condition": 92, "pop": 170, "cost": 0.12, "hazard": 33, "action": "Sanitation upkeep", "x": 35, "y": 80 },
  { "id": "HP16", "name": "Hand Pump H16", "type": "Water", "ward": 5, "condition": 62, "pop": 59, "cost": 0.24, "hazard": 9, "action": "Preventive inspection", "x": 58, "y": 32 },
  { "id": "HP17", "name": "Hand Pump H17", "type": "Water", "ward": 6, "condition": 69, "pop": 98, "cost": 0.36, "hazard": 20, "action": "Preventive inspection", "x": 81, "y": 49 },
  { "id": "HP18", "name": "Hand Pump H18", "type": "Water", "ward": 1, "condition": 76, "pop": 137, "cost": 0.48, "hazard": 31, "action": "Preventive inspection", "x": 24, "y": 66 },
  { "id": "HP19", "name": "Hand Pump H19", "type": "Water", "ward": 2, "condition": 83, "pop": 176, "cost": 0.6, "hazard": 42, "action": "Preventive inspection", "x": 47, "y": 83 },
  { "id": "HP20", "name": "Sanitation point S20", "type": "Sanitation", "ward": 3, "condition": 90, "pop": 65, "cost": 0.12, "hazard": 18, "action": "Sanitation upkeep", "x": 70, "y": 35 },
  { "id": "HP21", "name": "Hand Pump H21", "type": "Water", "ward": 4, "condition": 97, "pop": 104, "cost": 0.24, "hazard": 29, "action": "Preventive inspection", "x": 13, "y": 52 },
  { "id": "HP22", "name": "Hand Pump H22", "type": "Water", "ward": 5, "condition": 67, "pop": 143, "cost": 0.36, "hazard": 40, "action": "Preventive inspection", "x": 36, "y": 69 },
  { "id": "HP23", "name": "Hand Pump H23", "type": "Water", "ward": 6, "condition": 74, "pop": 182, "cost": 0.48, "hazard": 16, "action": "Preventive inspection", "x": 59, "y": 21 },
  { "id": "HP24", "name": "Hand Pump H24", "type": "Water", "ward": 1, "condition": 81, "pop": 71, "cost": 0.6, "hazard": 27, "action": "Preventive inspection", "x": 82, "y": 38 },
  { "id": "HP25", "name": "Sanitation point S25", "type": "Sanitation", "ward": 2, "condition": 88, "pop": 110, "cost": 0.12, "hazard": 38, "action": "Sanitation upkeep", "x": 25, "y": 55 },
  { "id": "HP26", "name": "Hand Pump H26", "type": "Water", "ward": 3, "condition": 95, "pop": 149, "cost": 0.24, "hazard": 14, "action": "Preventive inspection", "x": 48, "y": 72 },
  { "id": "HP27", "name": "Hand Pump H27", "type": "Water", "ward": 4, "condition": 65, "pop": 38, "cost": 0.36, "hazard": 25, "action": "Preventive inspection", "x": 71, "y": 24 },
  { "id": "HP28", "name": "Hand Pump H28", "type": "Water", "ward": 5, "condition": 72, "pop": 77, "cost": 0.48, "hazard": 36, "action": "Preventive inspection", "x": 14, "y": 41 },
  { "id": "HP29", "name": "Hand Pump H29", "type": "Water", "ward": 6, "condition": 79, "pop": 116, "cost": 0.6, "hazard": 12, "action": "Preventive inspection", "x": 37, "y": 58 },
  { "id": "HP30", "name": "Sanitation point S30", "type": "Sanitation", "ward": 1, "condition": 86, "pop": 155, "cost": 0.12, "hazard": 23, "action": "Sanitation upkeep", "x": 60, "y": 75 },
  { "id": "HP31", "name": "Hand Pump H31", "type": "Water", "ward": 2, "condition": 93, "pop": 44, "cost": 0.24, "hazard": 34, "action": "Preventive inspection", "x": 83, "y": 27 },
  { "id": "HP32", "name": "Hand Pump H32", "type": "Water", "ward": 3, "condition": 63, "pop": 83, "cost": 0.36, "hazard": 10, "action": "Preventive inspection", "x": 26, "y": 44 },
  { "id": "HP33", "name": "Hand Pump H33", "type": "Water", "ward": 4, "condition": 70, "pop": 122, "cost": 0.48, "hazard": 21, "action": "Preventive inspection", "x": 49, "y": 61 },
  { "id": "HP34", "name": "Hand Pump H34", "type": "Water", "ward": 5, "condition": 77, "pop": 161, "cost": 0.6, "hazard": 32, "action": "Preventive inspection", "x": 72, "y": 78 },
  { "id": "HP35", "name": "Sanitation point S35", "type": "Sanitation", "ward": 6, "condition": 84, "pop": 50, "cost": 0.12, "hazard": 8, "action": "Sanitation upkeep", "x": 15, "y": 30 },
  { "id": "HP36", "name": "Hand Pump H36", "type": "Water", "ward": 1, "condition": 91, "pop": 89, "cost": 0.24, "hazard": 19, "action": "Preventive inspection", "x": 38, "y": 47 },
  { "id": "HP37", "name": "Hand Pump H37", "type": "Water", "ward": 2, "condition": 61, "pop": 128, "cost": 0.36, "hazard": 30, "action": "Preventive inspection", "x": 61, "y": 64 },
  { "id": "HP38", "name": "Hand Pump H38", "type": "Water", "ward": 3, "condition": 68, "pop": 167, "cost": 0.48, "hazard": 41, "action": "Preventive inspection", "x": 84, "y": 81 },
  { "id": "HP39", "name": "Hand Pump H39", "type": "Water", "ward": 4, "condition": 75, "pop": 56, "cost": 0.6, "hazard": 17, "action": "Preventive inspection", "x": 27, "y": 33 },
  { "id": "HP40", "name": "Sanitation point S40", "type": "Sanitation", "ward": 5, "condition": 82, "pop": 95, "cost": 0.12, "hazard": 28, "action": "Sanitation upkeep", "x": 50, "y": 50 },
  { "id": "HP41", "name": "Hand Pump H41", "type": "Water", "ward": 6, "condition": 89, "pop": 134, "cost": 0.24, "hazard": 39, "action": "Preventive inspection", "x": 73, "y": 67 },
  { "id": "HP42", "name": "Hand Pump H42", "type": "Water", "ward": 1, "condition": 96, "pop": 173, "cost": 0.36, "hazard": 15, "action": "Preventive inspection", "x": 16, "y": 84 },
  { "id": "HP43", "name": "Hand Pump H43", "type": "Water", "ward": 2, "condition": 66, "pop": 62, "cost": 0.48, "hazard": 26, "action": "Preventive inspection", "x": 39, "y": 36 },
  { "id": "HP44", "name": "Hand Pump H44", "type": "Water", "ward": 3, "condition": 73, "pop": 101, "cost": 0.6, "hazard": 37, "action": "Preventive inspection", "x": 62, "y": 53 },
  { "id": "HP45", "name": "Sanitation point S45", "type": "Sanitation", "ward": 4, "condition": 80, "pop": 140, "cost": 0.12, "hazard": 13, "action": "Sanitation upkeep", "x": 85, "y": 70 },
  { "id": "HP46", "name": "Hand Pump H46", "type": "Water", "ward": 5, "condition": 87, "pop": 179, "cost": 0.24, "hazard": 24, "action": "Preventive inspection", "x": 28, "y": 22 },
  { "id": "HP47", "name": "Hand Pump H47", "type": "Water", "ward": 6, "condition": 94, "pop": 68, "cost": 0.36, "hazard": 35, "action": "Preventive inspection", "x": 51, "y": 39 },
  { "id": "HP48", "name": "Hand Pump H48", "type": "Water", "ward": 1, "condition": 64, "pop": 107, "cost": 0.48, "hazard": 11, "action": "Preventive inspection", "x": 74, "y": 56 },
  { "id": "HP49", "name": "Hand Pump H49", "type": "Water", "ward": 2, "condition": 71, "pop": 146, "cost": 0.6, "hazard": 22, "action": "Preventive inspection", "x": 17, "y": 73 },
  { "id": "HP50", "name": "Sanitation point S50", "type": "Sanitation", "ward": 3, "condition": 78, "pop": 35, "cost": 0.12, "hazard": 33, "action": "Sanitation upkeep", "x": 40, "y": 25 },
  { "id": "W03", "name": "Water Tank W03", "type": "Water", "ward": 5, "condition": 78, "pop": 860, "cost": 2.3, "hazard": 42, "action": "Inspect distribution valves", "x": 69, "y": 72 },
  { "id": "S03", "name": "Upper Primary School S03", "type": "Education", "ward": 6, "condition": 86, "pop": 390, "cost": 1.3, "hazard": 20, "action": "Routine school maintenance", "x": 87, "y": 37 },
  { "id": "A02", "name": "Anganwadi A02", "type": "Education", "ward": 5, "condition": 81, "pop": 105, "cost": 0.6, "hazard": 31, "action": "Routine facility maintenance", "x": 62, "y": 69 },
  { "id": "SL31", "name": "Streetlight Cluster SL31", "type": "Streetlight", "ward": 2, "condition": 57, "pop": 99, "cost": 0.3, "hazard": 15, "action": "Repair failed luminaires", "x": 29, "y": 41 },
  { "id": "SL32", "name": "Streetlight Cluster SL32", "type": "Streetlight", "ward": 3, "condition": 66, "pop": 128, "cost": 0.42, "hazard": 22, "action": "Repair failed luminaires", "x": 48, "y": 64 },
  { "id": "SL33", "name": "Streetlight Cluster SL33", "type": "Streetlight", "ward": 4, "condition": 75, "pop": 157, "cost": 0.54, "hazard": 29, "action": "Repair failed luminaires", "x": 67, "y": 19 },
  { "id": "SL34", "name": "Streetlight Cluster SL34", "type": "Streetlight", "ward": 5, "condition": 84, "pop": 186, "cost": 0.18, "hazard": 36, "action": "Repair failed luminaires", "x": 86, "y": 42 },
  { "id": "SL35", "name": "Streetlight Cluster SL35", "type": "Streetlight", "ward": 6, "condition": 93, "pop": 215, "cost": 0.3, "hazard": 11, "action": "Repair failed luminaires", "x": 25, "y": 65 },
  { "id": "SL36", "name": "Streetlight Cluster SL36", "type": "Streetlight", "ward": 1, "condition": 54, "pop": 244, "cost": 0.42, "hazard": 18, "action": "Repair failed luminaires", "x": 44, "y": 20 },
  { "id": "SL37", "name": "Streetlight Cluster SL37", "type": "Streetlight", "ward": 2, "condition": 63, "pop": 273, "cost": 0.54, "hazard": 25, "action": "Repair failed luminaires", "x": 63, "y": 43 },
  { "id": "SL38", "name": "Streetlight Cluster SL38", "type": "Streetlight", "ward": 3, "condition": 72, "pop": 82, "cost": 0.18, "hazard": 32, "action": "Repair failed luminaires", "x": 82, "y": 66 },
  { "id": "SL39", "name": "Streetlight Cluster SL39", "type": "Streetlight", "ward": 4, "condition": 81, "pop": 111, "cost": 0.3, "hazard": 39, "action": "Repair failed luminaires", "x": 21, "y": 21 },
  { "id": "SL40", "name": "Streetlight Cluster SL40", "type": "Streetlight", "ward": 5, "condition": 90, "pop": 140, "cost": 0.42, "hazard": 14, "action": "Repair failed luminaires", "x": 40, "y": 44 },
  { "id": "SL41", "name": "Streetlight Cluster SL41", "type": "Streetlight", "ward": 6, "condition": 51, "pop": 169, "cost": 0.54, "hazard": 21, "action": "Repair failed luminaires", "x": 59, "y": 67 },
  { "id": "SL42", "name": "Streetlight Cluster SL42", "type": "Streetlight", "ward": 1, "condition": 60, "pop": 198, "cost": 0.18, "hazard": 28, "action": "Repair failed luminaires", "x": 78, "y": 22 },
  { "id": "SL43", "name": "Streetlight Cluster SL43", "type": "Streetlight", "ward": 2, "condition": 69, "pop": 227, "cost": 0.3, "hazard": 35, "action": "Repair failed luminaires", "x": 17, "y": 45 },
  { "id": "SL44", "name": "Streetlight Cluster SL44", "type": "Streetlight", "ward": 3, "condition": 78, "pop": 256, "cost": 0.42, "hazard": 10, "action": "Repair failed luminaires", "x": 36, "y": 68 },
  { "id": "SL45", "name": "Streetlight Cluster SL45", "type": "Streetlight", "ward": 4, "condition": 87, "pop": 285, "cost": 0.54, "hazard": 17, "action": "Repair failed luminaires", "x": 55, "y": 23 },
  { "id": "SL46", "name": "Streetlight Cluster SL46", "type": "Streetlight", "ward": 5, "condition": 48, "pop": 94, "cost": 0.18, "hazard": 24, "action": "Repair failed luminaires", "x": 74, "y": 46 },
  { "id": "SL47", "name": "Streetlight Cluster SL47", "type": "Streetlight", "ward": 6, "condition": 57, "pop": 123, "cost": 0.3, "hazard": 31, "action": "Repair failed luminaires", "x": 13, "y": 69 },
  { "id": "SL48", "name": "Streetlight Cluster SL48", "type": "Streetlight", "ward": 1, "condition": 66, "pop": 152, "cost": 0.42, "hazard": 38, "action": "Repair failed luminaires", "x": 32, "y": 24 },
  { "id": "SL49", "name": "Streetlight Cluster SL49", "type": "Streetlight", "ward": 2, "condition": 75, "pop": 181, "cost": 0.54, "hazard": 13, "action": "Repair failed luminaires", "x": 51, "y": 47 },
  { "id": "SL50", "name": "Streetlight Cluster SL50", "type": "Streetlight", "ward": 3, "condition": 84, "pop": 210, "cost": 0.18, "hazard": 20, "action": "Repair failed luminaires", "x": 70, "y": 70 },
  { "id": "SL51", "name": "Streetlight Cluster SL51", "type": "Streetlight", "ward": 4, "condition": 93, "pop": 239, "cost": 0.3, "hazard": 27, "action": "Repair failed luminaires", "x": 89, "y": 25 },
  { "id": "SL52", "name": "Streetlight Cluster SL52", "type": "Streetlight", "ward": 5, "condition": 54, "pop": 268, "cost": 0.42, "hazard": 34, "action": "Repair failed luminaires", "x": 28, "y": 48 },
  { "id": "SL53", "name": "Streetlight Cluster SL53", "type": "Streetlight", "ward": 6, "condition": 63, "pop": 77, "cost": 0.54, "hazard": 9, "action": "Repair failed luminaires", "x": 47, "y": 71 },
  { "id": "SL54", "name": "Streetlight Cluster SL54", "type": "Streetlight", "ward": 1, "condition": 72, "pop": 106, "cost": 0.18, "hazard": 16, "action": "Repair failed luminaires", "x": 66, "y": 26 },
  { "id": "CF01", "name": "Market Shelter CF01", "type": "Public Facility", "ward": 2, "condition": 69, "pop": 141, "cost": 0.52, "hazard": 18, "action": "Schedule public facility upkeep", "x": 31, "y": 39 },
  { "id": "CF02", "name": "Public Toilet CF02", "type": "Public Facility", "ward": 3, "condition": 74, "pop": 182, "cost": 0.74, "hazard": 26, "action": "Schedule public facility upkeep", "x": 48, "y": 60 },
  { "id": "CF03", "name": "Solid Waste Point CF03", "type": "Public Facility", "ward": 4, "condition": 79, "pop": 223, "cost": 0.96, "hazard": 34, "action": "Schedule public facility upkeep", "x": 65, "y": 81 },
  { "id": "CF04", "name": "Community Hall CF04", "type": "Public Facility", "ward": 5, "condition": 84, "pop": 264, "cost": 1.18, "hazard": 42, "action": "Schedule public facility upkeep", "x": 82, "y": 32 },
  { "id": "CF05", "name": "Market Shelter CF05", "type": "Public Facility", "ward": 6, "condition": 89, "pop": 305, "cost": 0.3, "hazard": 10, "action": "Schedule public facility upkeep", "x": 24, "y": 53 },
  { "id": "CF06", "name": "Public Toilet CF06", "type": "Public Facility", "ward": 1, "condition": 94, "pop": 346, "cost": 0.52, "hazard": 18, "action": "Schedule public facility upkeep", "x": 41, "y": 74 },
  { "id": "CF07", "name": "Solid Waste Point CF07", "type": "Public Facility", "ward": 2, "condition": 67, "pop": 387, "cost": 0.74, "hazard": 26, "action": "Schedule public facility upkeep", "x": 58, "y": 25 },
  { "id": "CF08", "name": "Community Hall CF08", "type": "Public Facility", "ward": 3, "condition": 72, "pop": 428, "cost": 0.96, "hazard": 34, "action": "Schedule public facility upkeep", "x": 75, "y": 46 },
  { "id": "CF09", "name": "Market Shelter CF09", "type": "Public Facility", "ward": 4, "condition": 77, "pop": 469, "cost": 1.18, "hazard": 42, "action": "Schedule public facility upkeep", "x": 17, "y": 67 },
  { "id": "CF10", "name": "Public Toilet CF10", "type": "Public Facility", "ward": 5, "condition": 82, "pop": 110, "cost": 0.3, "hazard": 10, "action": "Schedule public facility upkeep", "x": 34, "y": 18 },
  { "id": "CF11", "name": "Solid Waste Point CF11", "type": "Public Facility", "ward": 6, "condition": 87, "pop": 151, "cost": 0.52, "hazard": 18, "action": "Schedule public facility upkeep", "x": 51, "y": 39 },
  { "id": "CF12", "name": "Community Hall CF12", "type": "Public Facility", "ward": 1, "condition": 92, "pop": 192, "cost": 0.74, "hazard": 26, "action": "Schedule public facility upkeep", "x": 68, "y": 60 }
];

// src/server/seed.ts
async function seed(db2) {
  if ((await db2.query("SELECT id FROM villages LIMIT 1")).length) return false;
  const password = process.env.SEED_PASSWORD;
  if (!password || password.length < 12) throw new Error("Set SEED_PASSWORD to at least 12 characters before seeding.");
  const hash = await hashPassword(password), assets = SEED_ASSETS;
  await db2.transaction(async (tx) => {
    await tx.query("INSERT INTO villages(id,data) VALUES($1,$2)", [VILLAGE, JSON.stringify({ name: "Adarsh Gram", code: "DEMO-001", district: "Simulated District", state: "Demo State", population: 3e3, households: 650, area: 6.8, latitude: 23.0125, longitude: 78.0125 })]);
    for (let w = 1; w <= 6; w++) await tx.query("INSERT INTO wards(id,village_id,number,data) VALUES($1,$2,$3,$4)", [`ward-${w}`, VILLAGE, w, JSON.stringify({ number: w, name: `Ward ${w}`, population: [450, 480, 520, 620, 460, 470][w - 1], households: [98, 104, 113, 134, 100, 101][w - 1], latitude: 23.004 + w * 25e-4, longitude: 78.003 + w * 25e-4, geometry: [[10 + (w - 1) % 3 * 28, 15 + Math.floor((w - 1) / 3) * 35], [36 + (w - 1) % 3 * 28, 15 + Math.floor((w - 1) / 3) * 35], [36 + (w - 1) % 3 * 28, 48 + Math.floor((w - 1) / 3) * 35], [10 + (w - 1) % 3 * 28, 48 + Math.floor((w - 1) / 3) * 35]] })]);
    for (const role of ["admin", "officer", "citizen"]) await tx.query("INSERT INTO users(id,village_id,email,name,role,password_hash) VALUES($1,$2,$3,$4,$5,$6)", [role, VILLAGE, `${role}@gram-pulse.demo`, `${role[0].toUpperCase() + role.slice(1)} demo`, role, hash]);
    for (const a of assets) {
      const data = assetSchema.parse({ name: a.name, type: a.type, ward: a.ward, condition: a.condition, pop: a.pop, cost: Math.round(a.cost * 100) / 100, hazard: a.hazard, action: a.action, latitude: 23 + a.y / 100 * 0.025, longitude: 78 + a.x / 100 * 0.025, x: a.x, y: a.y, capacity: Math.round(a.pop * 1.15), accessibility: Math.round(100 - a.hazard * 0.6), source: "SIMULATED" });
      await tx.query("INSERT INTO assets(id,village_id,ward_id,data) VALUES($1,$2,$3,$4)", [a.id, VILLAGE, `ward-${a.ward}`, JSON.stringify(data)]);
      await tx.query("INSERT INTO interventions(id,village_id,asset_id,data) VALUES($1,$2,$3,$4)", [`PR-${a.id}`, VILLAGE, a.id, JSON.stringify({ assetId: a.id, ward: a.ward, name: a.action, cost: data.cost, riskReduction: Math.round((100 - a.condition) * 0.65), serviceImprovement: Math.round((100 - a.condition) * 0.5), populationBenefit: a.pop, status: "proposed", timeline: "30–90 days after field assessment" })]);
    }
    const edges = /* @__PURE__ */ new Map();
    const add = (source, target, type) => {
      if (source !== target && assets.some((a) => a.id === target)) edges.set(`${source}-${target}-${type}`, { source, target, type });
    };
    for (const a of assets) {
      if (a.type === "Drainage") {
        const road = assets.find((b) => b.type === "Road" && b.ward === a.ward);
        if (road) add(a.id, road.id, "protects");
      }
      if (a.type === "Education" || a.type === "Healthcare" || a.type === "Public Facility") {
        const road = assets.find((b) => b.type === "Road" && b.ward === a.ward) || assets.find((b) => b.id === "R07");
        if (road) add(road.id, a.id, "serves");
      }
      if (a.type === "Water" && !/^W0/.test(a.id)) add(a.ward < 3 ? "W01" : a.ward < 5 ? "W02" : "W03", a.id, "supplies");
    }
    add("D04", "R07", "protects");
    add("R07", "S02", "serves");
    add("R07", "PHC01", "serves");
    add("R07", "W02", "affects");
    for (const [key, e] of edges) await tx.query("INSERT INTO dependencies(id,source,target,type,strength,impact_weight) VALUES($1,$2,$3,$4,$5,$6)", [key, e.source, e.target, e.type, 1, e.type === "supplies" ? 1 : 0.85]);
    for (let i = 0; i < 56; i++) {
      const a = assets[i % 10], date = new Date(Date.UTC(2026, 9, 8, 12) - (i + 2) * 864e5).toISOString();
      await tx.query("INSERT INTO reports(id,village_id,ward_id,asset_id,user_id,assigned_officer,data,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", [`DR-${String(i + 1).padStart(4, "0")}`, VILLAGE, `ward-${a.ward}`, a.id, "citizen", "officer", JSON.stringify({ category: a.type, title: `${a.name} inspection observation`, description: `Synthetic historical ${a.type.toLowerCase()} condition observation near ${a.name}.`, ward: a.ward, severity: i % 4 === 0 ? "HIGH" : i % 3 === 0 ? "MEDIUM" : "LOW", confidence: 55, status: i % 4 === 0 ? "Verified" : "Pending verification", timestamp: date, synthetic: true, resolutionNotes: "", verifiedAt: null, classificationMethod: "Seeded demonstration report", latitude: 23 + a.y * 25e-5, longitude: 78 + a.x * 25e-5 }), date]);
    }
    for (let i = 0; i < 15; i++) {
      const a = assets[(i * 7 + 2) % assets.length], date = new Date(Date.UTC(2026, 9, 8, 12) - (i + 1) * 3 * 864e5).toISOString(), status = i % 3 === 0 ? "Open" : "Resolved";
      await tx.query("INSERT INTO incidents(id,village_id,asset_id,data,created_at) VALUES($1,$2,$3,$4,$5)", [`INC-${i + 1}`, VILLAGE, a.id, JSON.stringify({ assetId: a.id, category: a.type, severity: i % 4 === 0 ? "High" : "Medium", status, description: `Synthetic historical interruption at ${a.name}.`, cause: "Seasonal exposure and condition deterioration", resolution: status === "Resolved" ? "Inspection and corrective work recorded" : "", date, endTime: status === "Resolved" ? date : null, affectedAssets: [a.id], affectedPopulation: a.pop, cost: a.cost, response: "Field inspection" }), date]);
      await tx.query("INSERT INTO incident_assets VALUES($1,$2)", [`INC-${i + 1}`, a.id]);
    }
    await tx.query("INSERT INTO settings(village_id,weights) VALUES($1,$2)", [VILLAGE, JSON.stringify(defaultWeights)]);
    const s = await snapshot(tx, { id: "admin", villageId: VILLAGE, name: "Admin", email: "admin@gram-pulse.demo", role: "admin", disabled: false });
    await tx.query("INSERT INTO pulse_history(village_id,pulse,reason) VALUES($1,$2,$3)", [VILLAGE, s.pulse, "Initial database seed — SIMULATED / DEMO DATA"]);
  });
  return true;
}

// src/server/index.ts
try {
  process.loadEnvFile();
} catch {
}
var db = await connect();
await migrate(db);
if (process.env.SEED_PASSWORD) {
  const created = await seed(db);
  console.log(created ? "Demo data seeded." : "Database already contains data; seed skipped.");
  if (process.env.RESET_PASSWORDS === "true") {
    const hash = await hashPassword(process.env.SEED_PASSWORD);
    await db.query("UPDATE users SET password_hash=$1 WHERE email IN ('admin@gram-pulse.demo','officer@gram-pulse.demo','citizen@gram-pulse.demo')", [hash]);
    await db.query("DELETE FROM sessions");
    console.log("Demo account passwords reset to SEED_PASSWORD. Remove RESET_PASSWORDS now.");
  }
} else {
  const users = await db.query("SELECT 1 FROM users LIMIT 1");
  if (!users.length) console.warn("No users exist and SEED_PASSWORD is not set. Set SEED_PASSWORD (12+ characters) and redeploy to create the demo accounts.");
}
var port = Number(process.env.PORT || 3e3);
var server = createApp(db).listen(port, "0.0.0.0", () => console.log(`GRAM-PULSE listening on port ${port}`));
server.keepAliveTimeout = 65e3;
server.headersTimeout = 66e3;
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => server.close(() => {
  void db.close().then(() => process.exit(0));
}));
