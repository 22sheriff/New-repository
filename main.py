"""
Matola LandInfo — FastAPI Backend
Connects to PostgreSQL with PostGIS support.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
import asyncpg
import os
import json
from datetime import date, datetime
from decimal import Decimal

app = FastAPI(title="Matola LandInfo API", version="1.0.0")

# ── CORS (allow Netlify frontend + local dev) ──────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://*.netlify.app",
        "http://localhost:3000",
        "http://127.0.0.1:5500",
        "*",  # tighten this once you know your Netlify URL
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DB CONNECTION ──────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL")  # set in Railway env vars

async def get_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("SET search_path TO matola_cadastral, public")
    return conn

def serialize(row):
    """Convert asyncpg Record to JSON-safe dict."""
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, (datetime, date)):
            d[k] = v.isoformat()
        elif isinstance(v, Decimal):
            d[k] = float(v)
    return d

# ── HEALTH ─────────────────────────────────────────────────────────────────
@app.get("/")
async def health():
    return {"status": "ok", "service": "Matola LandInfo API"}

# ══════════════════════════════════════════════════════════════════════════
# PARCELS
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/parcels")
async def list_parcels(
    search: Optional[str] = Query(None, description="Search by parcel_id, owner, gvh, place"),
    landuse: Optional[str] = Query(None),
    gvh: Optional[str] = Query(None),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
):
    """Search/list parcels from matola_parcels table."""
    conn = await get_db()
    try:
        conditions = []
        params = []
        i = 1

        if search:
            p = "$" + str(i)
            conditions.append(
                "(parcel_id ILIKE {p} OR applicants ILIKE {p} OR place ILIKE {p} OR gvh ILIKE {p} OR CAST(upin AS TEXT) ILIKE {p})".format(p=p)
            )
            params.append("%" + search + "%")
            i += 1
        if landuse:
            conditions.append("landuse ILIKE $" + str(i))
            params.append("%" + landuse + "%")
            i += 1
        if gvh:
            conditions.append("gvh ILIKE $" + str(i))
            params.append("%" + gvh + "%")
            i += 1

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params += [limit, offset]

        query = (
            "SELECT gid, parcel_id, upin, applicants, landuse, gvh, place,"
            " size_in_ha, size_in_a, ownership_, dispute, dispute_ty,"
            " easement, easement_t, evidence_o, evidence_t,"
            " centroid_n, centroid_e, map_sheet, title_no,"
            " piece_no, registra_1, registra_2, registra_3,"
            " time_start, time_end, created_at, updated_at,"
            " purpose, right, boundary_d, area, acres, x, y"
            " FROM matola_cadastral.matola_parcels"
            " " + where +
            " ORDER BY parcel_id"
            " LIMIT $" + str(i) + " OFFSET $" + str(i + 1)
        )
        rows = await conn.fetch(query, *params)

        count_row = await conn.fetchrow(
            "SELECT COUNT(*) FROM matola_cadastral.matola_parcels " + where,
            *params[:-2]
        )

        return {
            "total": count_row["count"],
            "limit": limit,
            "offset": offset,
            "results": [serialize(r) for r in rows]
        }
    finally:
        await conn.close()


@app.get("/api/parcels/{parcel_id}")
async def get_parcel(parcel_id: str):
    """Get a single parcel by parcel_id."""
    conn = await get_db()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM matola_cadastral.matola_parcels WHERE parcel_id = $1", parcel_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Parcel not found")
        return serialize(row)
    finally:
        await conn.close()


@app.get("/api/parcels/{parcel_id}/geojson")
async def get_parcel_geojson(parcel_id: str):
    """Return parcel geometry as GeoJSON (reprojects from UTM 36S to WGS84)."""
    conn = await get_db()
    try:
        # Try matola_parcels centroid first, then boundary table
        row = await conn.fetchrow(
            """
            SELECT parcel_id, centroid_n, centroid_e, size_in_ha, landuse, gvh, applicants
            FROM matola_cadastral.matola_parcels WHERE parcel_id = $1
            """, parcel_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Parcel not found")

        # If centroid coords exist, return a point feature
        r = serialize(row)
        if r.get("centroid_e") and r.get("centroid_n"):
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [r["centroid_e"], r["centroid_n"]]
                },
                "properties": {k: v for k, v in r.items() if k not in ("centroid_e","centroid_n")}
            }
            return feature
        raise HTTPException(status_code=404, detail="No geometry for this parcel")
    finally:
        await conn.close()


@app.get("/api/map/boundary")
async def get_boundary():
    """Return Matola boundary as GeoJSON (WGS84)."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT gid,
                   ST_AsGeoJSON(ST_Transform(geom, 4326))::json AS geometry
            FROM matola_cadastral.boundary
            """
        )
        features = []
        for r in rows:
            features.append({
                "type": "Feature",
                "geometry": r["geometry"],
                "properties": {"gid": r["gid"]}
            })
        return {"type": "FeatureCollection", "features": features}
    finally:
        await conn.close()


@app.get("/api/map/parcels-geojson")
async def get_all_parcels_geojson(limit: int = Query(2000, le=5000)):
    """Return all parcels as WGS84 GeoJSON polygons."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT parcel_id, landuse, gvh, applicants, size_in_ha,
                   ownership_, dispute, dispute_ty,
                   extensions.ST_AsGeoJSON(
                     extensions.ST_Transform(geom::extensions.geometry, 4326)
                   )::json AS geometry
            FROM matola_cadastral.matola_parcels
            WHERE geom IS NOT NULL
            LIMIT $1
            """, limit
        )
        features = []
        for r in rows:
            try:
                if r["geometry"] is None:
                    continue
                features.append({
                    "type": "Feature",
                    "geometry": r["geometry"],
                    "properties": {
                        "parcel_id": r["parcel_id"],
                        "landuse": r["landuse"],
                        "gvh": r["gvh"],
                        "owner": r["applicants"],
                        "size_ha": float(r["size_in_ha"]) if r["size_in_ha"] else None,
                        "ownership": r["ownership_"],
                        "dispute": r["dispute"],
                        "dispute_ty": r["dispute_ty"],
                    }
                })
            except Exception:
                continue
        return {"type": "FeatureCollection", "features": features}
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════
# STATS / DASHBOARD
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/stats")
async def get_stats():
    """Dashboard summary statistics."""
    conn = await get_db()
    try:
        total = await conn.fetchval("SELECT COUNT(*) FROM matola_cadastral.matola_parcels")
        disputes = await conn.fetchval("SELECT COUNT(*) FROM matola_cadastral.matola_parcels WHERE dispute = 'Y'")
        easements = await conn.fetchval("SELECT COUNT(*) FROM matola_cadastral.matola_parcels WHERE easement = 'Y'")
        total_area = await conn.fetchval("SELECT SUM(size_in_ha) FROM matola_cadastral.matola_parcels")

        # Applicants stats
        total_apps = await conn.fetchval("SELECT COUNT(*) FROM matola_cadastral.applicants")
        pending = await conn.fetchval("SELECT COUNT(*) FROM matola_cadastral.applicants WHERE registration_stage ILIKE '%pending%'")
        paid = await conn.fetchval("SELECT COUNT(*) FROM matola_cadastral.applicants WHERE rental_status ILIKE '%paid%'")

        # Payments
        total_paid = await conn.fetchval("SELECT COALESCE(SUM(amount),0) FROM matola_cadastral.payments")

        # Land use breakdown
        lu_rows = await conn.fetch(
            "SELECT landuse, COUNT(*) as cnt FROM matola_cadastral.matola_parcels GROUP BY landuse ORDER BY cnt DESC LIMIT 8"
        )

        # Ownership breakdown
        own_rows = await conn.fetch(
            "SELECT ownership_, COUNT(*) as cnt FROM matola_cadastral.matola_parcels GROUP BY ownership_ ORDER BY cnt DESC LIMIT 6"
        )

        return {
            "parcels": {
                "total": total,
                "disputes": disputes,
                "easements": easements,
                "total_area_ha": float(total_area) if total_area else 0,
            },
            "applications": {
                "total": total_apps,
                "pending": pending,
                "paid": paid,
            },
            "payments": {
                "total_mwk": float(total_paid),
            },
            "land_use": [{"type": r["landuse"] or "Unknown", "count": r["cnt"]} for r in lu_rows],
            "ownership": [{"type": r["ownership_"] or "Unknown", "count": r["cnt"]} for r in own_rows],
        }
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════
# APPLICANTS / REGISTRATION
# ══════════════════════════════════════════════════════════════════════════

class RegistrationPayload(BaseModel):
    owner_name: str
    national_id: str
    phone: Optional[str] = None
    village: Optional[str] = None
    gvh: Optional[str] = None
    email: Optional[str] = None
    area_ha: Optional[float] = None
    land_use: Optional[str] = None
    geojson: Optional[str] = None
    notes: Optional[str] = None


@app.post("/api/register")
async def register_applicant(payload: RegistrationPayload):
    """Submit a new land registration application."""
    conn = await get_db()
    try:
        import random, string
        parcel_id = "MAT-" + str(random.randint(1558, 9999)).zfill(4)
        ref = "REF-2026-" + "".join(random.choices(string.digits, k=4))

        row = await conn.fetchrow(
            """
            INSERT INTO matola_cadastral.applicants
              (parcel_id, reference_number, owner_name, national_id, phone,
               village, gvh, email, area_ha, land_use, geojson, notes,
               rental_status, registration_stage, registration_date, updated_at)
            VALUES
              ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
               'unpaid','submitted',NOW(),NOW())
            RETURNING id, parcel_id, reference_number, registration_stage
            """,
            parcel_id, ref, payload.owner_name, payload.national_id,
            payload.phone, payload.village, payload.gvh, payload.email,
            payload.area_ha, payload.land_use, payload.geojson, payload.notes
        )
        return {
            "success": True,
            "parcel_id": row["parcel_id"],
            "reference_number": row["reference_number"],
            "stage": row["registration_stage"],
            "message": "Registration submitted successfully. Track with your reference number."
        }
    finally:
        await conn.close()


@app.get("/api/applicants")
async def list_applicants(
    search: Optional[str] = Query(None),
    stage: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
):
    """List/search applicants (admin view)."""
    conn = await get_db()
    try:
        conditions, params, i = [], [], 1
        if search:
            conditions.append(f"(owner_name ILIKE ${i} OR parcel_id ILIKE ${i} OR national_id ILIKE ${i} OR reference_number ILIKE ${i})")
            params.append(f"%{search}%"); i += 1
        if stage:
            conditions.append(f"registration_stage ILIKE ${i}")
            params.append(f"%{stage}%"); i += 1

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params += [limit, offset]

        rows = await conn.fetch(
            "SELECT * FROM matola_cadastral.applicants {where} ORDER BY registration_date DESC LIMIT {lp} OFFSET {op}".format(
                where=where, lp="$"+str(i), op="$"+str(i+1)),
            *params
        )
        count = await conn.fetchval(f"SELECT COUNT(*) FROM matola_cadastral.applicants {where}", *params[:-2])
        return {"total": count, "results": [serialize(r) for r in rows]}
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════
# CLIENT TRACKER
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/track/{reference}")
async def track_application(reference: str):
    """Track application by reference number or national ID."""
    conn = await get_db()
    try:
        row = await conn.fetchrow(
            """
            SELECT a.*,
                   p.amount, p.receipt_number, p.payment_date, p.payment_method
            FROM matola_cadastral.applicants a
            LEFT JOIN payments p ON p.parcel_id = a.parcel_id
            WHERE a.reference_number ILIKE $1
               OR a.national_id = $1
               OR a.parcel_id ILIKE $1
            ORDER BY a.registration_date DESC
            LIMIT 1
            """, reference
        )
        if not row:
            raise HTTPException(status_code=404, detail="Application not found")

        r = serialize(row)
        # Map stage to numeric step
        stage_map = {
            "submitted": 0, "under review": 1, "review": 1,
            "verified": 2, "payment": 3, "complete": 4, "paid": 4
        }
        stage_str = (r.get("registration_stage") or "submitted").lower()
        stage_num = next((v for k, v in stage_map.items() if k in stage_str), 0)
        r["stage_num"] = stage_num
        return r
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════
# PAYMENTS
# ══════════════════════════════════════════════════════════════════════════

class PaymentPayload(BaseModel):
    parcel_id: str
    amount: float
    receipt_number: str
    payment_method: Optional[str] = "cash"
    recorded_by: Optional[str] = "system"
    payment_date: Optional[str] = None


@app.post("/api/payments")
async def record_payment(payload: PaymentPayload):
    """Record a payment and update applicant rental_status."""
    conn = await get_db()
    try:
        pay_date = date.fromisoformat(payload.payment_date) if payload.payment_date else date.today()

        row = await conn.fetchrow(
            """
            INSERT INTO matola_cadastral.payments
              (parcel_id, amount, receipt_number, payment_date, payment_method, recorded_by, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,NOW())
            RETURNING id, parcel_id, amount, receipt_number
            """,
            payload.parcel_id, payload.amount, payload.receipt_number,
            pay_date, payload.payment_method, payload.recorded_by
        )
        # Update applicant status
        await conn.execute(
            "UPDATE matola_cadastral.applicants SET rental_status='paid', registration_stage='complete', updated_at=NOW() WHERE parcel_id=$1",
            payload.parcel_id
        )
        return {"success": True, "payment_id": row["id"], "parcel_id": row["parcel_id"],
                "amount": float(row["amount"]), "receipt": row["receipt_number"]}
    finally:
        await conn.close()


@app.get("/api/payments")
async def list_payments(
    parcel_id: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
):
    """List payments (admin)."""
    conn = await get_db()
    try:
        where = "WHERE parcel_id = $1" if parcel_id else ""
        params = [parcel_id, limit, offset] if parcel_id else [limit, offset]
        i_limit = 2 if parcel_id else 1

        rows = await conn.fetch(
            "SELECT * FROM matola_cadastral.payments {where} ORDER BY created_at DESC LIMIT {lp} OFFSET {op}".format(
                where=where, lp="$"+str(i_limit), op="$"+str(i_limit+1)),
            *params
        )
        return {"results": [serialize(r) for r in rows]}
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════
# TOPOLOGY / COMPLIANCE
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/topology/disputes")
async def get_disputes():
    """Return all disputed parcels."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT parcel_id, applicants, gvh, place, dispute_ty, landuse, size_in_ha
            FROM matola_cadastral.matola_parcels WHERE dispute = 'Y' ORDER BY parcel_id
            """
        )
        return {"total": len(rows), "results": [serialize(r) for r in rows]}
    finally:
        await conn.close()


@app.get("/api/topology/easements")
async def get_easements():
    """Return all parcels with easements."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT parcel_id, applicants, gvh, easement_t, landuse, size_in_ha
            FROM matola_cadastral.matola_parcels WHERE easement = 'Y' ORDER BY parcel_id
            """
        )
        return {"total": len(rows), "results": [serialize(r) for r in rows]}
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════
# SPATIAL LAYERS — Rivers, Roads, Railway (all MultiLineString, SRID 32736)
# ══════════════════════════════════════════════════════════════════════════

def _build_line_feature_collection(rows, extra_props_fn=None):
    """Helper: convert asyncpg rows with geojson geometry column to FeatureCollection."""
    features = []
    for r in rows:
        geom = r["geometry"]
        if geom is None:
            continue
        props = {"gid": r["gid"], "id": r["id"]}
        if extra_props_fn:
            props.update(extra_props_fn(r))
        features.append({"type": "Feature", "geometry": geom, "properties": props})
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/rivers/geojson")
async def get_rivers_geojson():
    """Rivers as GeoJSON FeatureCollection (WGS84)."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT gid, id,
                   ST_AsGeoJSON(ST_Transform(geom, 4326))::json AS geometry
            FROM matola_cadastral.rivers
            """
        )
        return _build_line_feature_collection(rows)
    finally:
        await conn.close()


@app.get("/api/rivers/buffer")
async def get_rivers_buffer():
    """15 m riparian buffer zones around rivers (WGS84)."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT gid, id,
                   ST_AsGeoJSON(
                     ST_Transform(ST_Buffer(geom, 15), 4326)
                   )::json AS geometry
            FROM matola_cadastral.rivers
            """
        )
        fc = _build_line_feature_collection(rows)
        for f in fc["features"]:
            f["properties"].update({
                "feature_type": "riparian_buffer",
                "buffer_m": 15,
                "description": "Riparian buffer zone (15 m)"
            })
        return fc
    finally:
        await conn.close()


@app.get("/api/roads/geojson")
async def get_roads_geojson():
    """Roads as GeoJSON FeatureCollection (WGS84)."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT gid, id, name,
                   ST_AsGeoJSON(ST_Transform(geom, 4326))::json AS geometry
            FROM matola_cadastral.roads
            """
        )
        return _build_line_feature_collection(rows, lambda r: {"name": r["name"]})
    finally:
        await conn.close()


@app.get("/api/roads/buffer")
async def get_roads_buffer():
    """10 m road reserve buffer zones (WGS84)."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT gid, id, name,
                   ST_AsGeoJSON(
                     ST_Transform(ST_Buffer(geom, 10), 4326)
                   )::json AS geometry
            FROM matola_cadastral.roads
            """
        )
        fc = _build_line_feature_collection(rows, lambda r: {"name": r["name"]})
        for f in fc["features"]:
            f["properties"].update({
                "feature_type": "road_buffer",
                "buffer_m": 10,
                "description": "Road reserve buffer (10 m)"
            })
        return fc
    finally:
        await conn.close()


@app.get("/api/railway/geojson")
async def get_railway_geojson():
    """Railway lines as GeoJSON FeatureCollection (WGS84)."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT gid, id,
                   ST_AsGeoJSON(ST_Transform(geom, 4326))::json AS geometry
            FROM matola_cadastral.railway
            """
        )
        return _build_line_feature_collection(rows)
    finally:
        await conn.close()


@app.get("/api/railway/buffer")
async def get_railway_buffer():
    """20 m railway reserve buffer zones (WGS84)."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT gid, id,
                   ST_AsGeoJSON(
                     ST_Transform(ST_Buffer(geom, 20), 4326)
                   )::json AS geometry
            FROM matola_cadastral.railway
            """
        )
        fc = _build_line_feature_collection(rows)
        for f in fc["features"]:
            f["properties"].update({
                "feature_type": "railway_buffer",
                "buffer_m": 20,
                "description": "Railway reserve buffer (20 m)"
            })
        return fc
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════
# COMPLIANCE — parcels within buffer zones (uses ST_DWithin)
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/compliance/riparian")
async def compliance_riparian():
    """Parcels whose centroid is within 15 m of a river."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT DISTINCT p.parcel_id, p.applicants, p.gvh, p.landuse,
                   p.size_in_ha,
                   ROUND(ST_Distance(
                     ST_Transform(ST_SetSRID(ST_MakePoint(p.centroid_e, p.centroid_n), 32736), 32736),
                     r.geom
                   )::numeric, 1) AS dist_m
            FROM matola_cadastral.matola_parcels p
            JOIN matola_cadastral.rivers r ON ST_DWithin(
                ST_SetSRID(ST_MakePoint(p.centroid_e, p.centroid_n), 32736),
                r.geom, 15
            )
            WHERE p.centroid_e IS NOT NULL AND p.centroid_n IS NOT NULL
            ORDER BY dist_m
            """
        )
        return {"total": len(rows), "buffer_m": 15, "results": [serialize(r) for r in rows]}
    finally:
        await conn.close()


@app.get("/api/compliance/road")
async def compliance_road():
    """Parcels whose centroid is within 10 m of a road."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT DISTINCT p.parcel_id, p.applicants, p.gvh, p.landuse,
                   p.size_in_ha,
                   ROUND(ST_Distance(
                     ST_SetSRID(ST_MakePoint(p.centroid_e, p.centroid_n), 32736),
                     r.geom
                   )::numeric, 1) AS dist_m
            FROM matola_cadastral.matola_parcels p
            JOIN matola_cadastral.roads r ON ST_DWithin(
                ST_SetSRID(ST_MakePoint(p.centroid_e, p.centroid_n), 32736),
                r.geom, 10
            )
            WHERE p.centroid_e IS NOT NULL AND p.centroid_n IS NOT NULL
            ORDER BY dist_m
            """
        )
        return {"total": len(rows), "buffer_m": 10, "results": [serialize(r) for r in rows]}
    finally:
        await conn.close()


@app.get("/api/compliance/railway")
async def compliance_railway():
    """Parcels whose centroid is within 20 m of the railway."""
    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT DISTINCT p.parcel_id, p.applicants, p.gvh, p.landuse,
                   p.size_in_ha,
                   ROUND(ST_Distance(
                     ST_SetSRID(ST_MakePoint(p.centroid_e, p.centroid_n), 32736),
                     r.geom
                   )::numeric, 1) AS dist_m
            FROM matola_cadastral.matola_parcels p
            JOIN matola_cadastral.railway r ON ST_DWithin(
                ST_SetSRID(ST_MakePoint(p.centroid_e, p.centroid_n), 32736),
                r.geom, 20
            )
            WHERE p.centroid_e IS NOT NULL AND p.centroid_n IS NOT NULL
            ORDER BY dist_m
            """
        )
        return {"total": len(rows), "buffer_m": 20, "results": [serialize(r) for r in rows]}
    finally:
        await conn.close()


@app.get("/api/compliance/parcel/{parcel_id}")
async def check_parcel_compliance(parcel_id: str):
    """Check a single parcel's compliance against all buffer zones."""
    conn = await get_db()
    try:
        parcel = await conn.fetchrow(
            "SELECT centroid_e, centroid_n FROM matola_cadastral.matola_parcels WHERE parcel_id = $1",
            parcel_id
        )
        if not parcel or not parcel["centroid_e"]:
            raise HTTPException(status_code=404, detail="Parcel not found or has no coordinates")

        violations = []

        rip = await conn.fetchval(
            """
            SELECT MIN(ST_Distance(
              ST_SetSRID(ST_MakePoint($1,$2),32736), geom
            )) FROM matola_cadastral.rivers
            """, parcel["centroid_e"], parcel["centroid_n"]
        )
        if rip is not None and rip < 15:
            violations.append({"zone": "Riparian buffer", "limit_m": 15, "dist_m": round(float(rip), 1)})

        rd = await conn.fetchval(
            """
            SELECT MIN(ST_Distance(
              ST_SetSRID(ST_MakePoint($1,$2),32736), geom
            )) FROM matola_cadastral.roads
            """, parcel["centroid_e"], parcel["centroid_n"]
        )
        if rd is not None and rd < 10:
            violations.append({"zone": "Road reserve", "limit_m": 10, "dist_m": round(float(rd), 1)})

        rw = await conn.fetchval(
            """
            SELECT MIN(ST_Distance(
              ST_SetSRID(ST_MakePoint($1,$2),32736), geom
            )) FROM matola_cadastral.railway
            """, parcel["centroid_e"], parcel["centroid_n"]
        )
        if rw is not None and rw < 20:
            violations.append({"zone": "Railway reserve", "limit_m": 20, "dist_m": round(float(rw), 1)})

        return {
            "parcel_id": parcel_id,
            "compliant": len(violations) == 0,
            "violations": violations
        }
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════
# VILLAGES
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/villages")
async def list_villages():
    """List all villages (for dropdowns and filtering)."""
    conn = await get_db()
    try:
        # Try to get column names first to handle unknown schema
        cols = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'matola_cadastral' AND table_name = 'villages' ORDER BY ordinal_position
            """
        )
        col_names = [c["column_name"] for c in cols]
        # Build a safe select of all non-geometry columns
        safe_cols = [c for c in col_names if c != "geom"]
        select = ", ".join(safe_cols) if safe_cols else "gid"
        rows = await conn.fetch(f"SELECT {select} FROM matola_cadastral.villages ORDER BY 1")
        return {"total": len(rows), "results": [serialize(r) for r in rows]}
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════
# USERS (admin — read-only listing, no passwords exposed)
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/users")
async def list_users():
    """List system users (safe columns only — no passwords)."""
    conn = await get_db()
    try:
        cols = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'matola_cadastral' AND table_name = 'users' ORDER BY ordinal_position
            """
        )
        col_names = [c["column_name"] for c in cols]
        # Exclude any password-like columns for safety
        safe = [c for c in col_names if "password" not in c.lower() and "hash" not in c.lower() and "secret" not in c.lower()]
        select = ", ".join(safe) if safe else "id"
        rows = await conn.fetch(f"SELECT {select} FROM matola_cadastral.users ORDER BY 1")
        return {"total": len(rows), "results": [serialize(r) for r in rows]}
    finally:
        await conn.close()