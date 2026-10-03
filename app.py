"""Maritime search-and-rescue coordination service."""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "maritime_sar.db"
ACTIVE_INCIDENT = {"reported", "coordinating", "recovering"}
CLOSED_INCIDENT = {"closed", "cancelled", "duplicate"}
ACTIVE_ASSIGNMENT = {"issued", "executing"}


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400, extra: dict[str, Any] | None = None):
        super().__init__(message)
        self.status = status
        self.extra = extra or {}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def require_role(role: str, allowed: set[str], action: str) -> None:
    if role not in allowed:
        raise DomainError("角色无权执行：%s" % action, 403)


def clean_actor(actor: str) -> str:
    actor = (actor or "").strip()
    if not actor:
        raise DomainError("缺少操作人")
    return actor


def validate_position(lat: Any, lon: Any) -> tuple[float, float]:
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError) as exc:
        raise DomainError("经纬度必须是数值") from exc
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise DomainError("经纬度超出有效范围")
    return lat, lon


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class MaritimeSARService:
    def __init__(self, db_path: str | os.PathLike[str] = DEFAULT_DB):
        self.db_path = str(db_path)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    vessel_name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    uncertainty_km REAL NOT NULL,
                    drift_direction REAL NOT NULL DEFAULT 0,
                    drift_speed_kn REAL NOT NULL DEFAULT 0,
                    sea_state INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'reported',
                    lead_org TEXT NOT NULL,
                    duplicate_of INTEGER REFERENCES incidents(id),
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    capabilities TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'available',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    speed_kn REAL NOT NULL,
                    range_km REAL NOT NULL,
                    max_sea_state INTEGER NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS search_areas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    code TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    center_lat REAL NOT NULL,
                    center_lon REAL NOT NULL,
                    radius_km REAL NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 3,
                    status TEXT NOT NULL DEFAULT 'planned',
                    assigned_asset_id INTEGER REFERENCES assets(id),
                    note TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    client_event_id TEXT UNIQUE,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    area_id INTEGER NOT NULL REFERENCES search_areas(id),
                    asset_id INTEGER NOT NULL REFERENCES assets(id),
                    status TEXT NOT NULL DEFAULT 'issued',
                    basis TEXT NOT NULL DEFAULT '{}',
                    review_pending INTEGER NOT NULL DEFAULT 0,
                    review_reasons TEXT NOT NULL DEFAULT '[]',
                    invalid_reason TEXT,
                    incident_version INTEGER NOT NULL,
                    area_version INTEGER NOT NULL,
                    asset_version INTEGER NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS assignment_drafts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    area_id INTEGER NOT NULL,
                    asset_id INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    conflict TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'open',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS clues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    area_id INTEGER REFERENCES search_areas(id),
                    client_event_id TEXT NOT NULL UNIQUE,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    confidence REAL NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'unverified',
                    distance_from_incident_km REAL NOT NULL,
                    reporter TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT '',
                    recorded_at TEXT NOT NULL,
                    merged_at TEXT
                );
                CREATE TABLE IF NOT EXISTS clue_conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_event_id TEXT NOT NULL,
                    clue_id INTEGER NOT NULL REFERENCES clues(id),
                    field TEXT NOT NULL,
                    existing_value TEXT,
                    incoming_value TEXT,
                    batch_id TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS offline_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_batch_id TEXT NOT NULL UNIQUE,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    merged_at TEXT,
                    summary TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER REFERENCES incidents(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clues_incident ON clues(incident_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline(incident_id, id);
                CREATE INDEX IF NOT EXISTS idx_assignments_area ON assignments(area_id, status);
                CREATE INDEX IF NOT EXISTS idx_assignments_asset ON assignments(asset_id, status);
                """
            )

    def _audit(self, conn: sqlite3.Connection, incident_id: int | None, actor: str, action: str, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO timeline(incident_id,actor,action,details,created_at) VALUES(?,?,?,?,?)",
            (incident_id, actor, action, json_dump(details), utcnow()),
        )

    # ------------------------------------------------------------------
    # 派单内部助手：版本冻结、有效性校验、占用、失效与待复核
    # ------------------------------------------------------------------

    def _freeze_versions(self, conn: sqlite3.Connection, actor: str, action: str,
                         incident: sqlite3.Row, area: sqlite3.Row, asset: sqlite3.Row,
                         expected_incident_version: Any, expected_area_version: Any,
                         expected_asset_version: Any, payload: dict[str, Any]) -> None:
        """改派前冻结事件与区域（可选资源）版本；不一致时保留草稿并报告当前版本。"""
        current = {"incident_version": incident["version"], "area_version": area["version"], "asset_version": asset["version"]}
        expected: dict[str, int] = {}
        mismatched: dict[str, int] = {}
        for key, value in (("incident_version", expected_incident_version),
                           ("area_version", expected_area_version),
                           ("asset_version", expected_asset_version)):
            if value is None:
                continue
            expected[key] = int(value)
            if current[key] != int(value):
                mismatched[key] = current[key]
        if not mismatched:
            return
        now = utcnow()
        cur = conn.execute(
            """INSERT INTO assignment_drafts(actor,action,area_id,asset_id,payload,conflict,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (actor, action, area["id"], asset["id"], json_dump(payload),
             json_dump({"expected": expected, "current": current, "mismatched": mismatched}), now, now),
        )
        draft_id = int(cur.lastrowid)
        self._audit(conn, incident["id"], actor, "assignment.draft_saved",
                    {"draft_id": draft_id, "action": action, "area_id": area["id"],
                     "asset_id": asset["id"], "mismatched": mismatched})
        # 草稿必须在 409 之前落库，否则随事务回滚丢失
        conn.commit()
        raise DomainError("事件或区域版本已变化，改派请求已保留为草稿", 409,
                          extra={"draft_id": draft_id, "current_versions": current, "mismatched": mismatched})

    def _validate_assignment(self, incident: sqlite3.Row, area: sqlite3.Row, asset: sqlite3.Row) -> float:
        """按航程、能力和海况确认有效占用，返回航程公里数。"""
        if asset["status"] != "available":
            raise DomainError("资源当前不可用", 409)
        if incident["sea_state"] > asset["max_sea_state"]:
            raise DomainError("海况超出资源能力", 409)
        capabilities = json.loads(asset["capabilities"])
        if area["kind"] not in capabilities:
            raise DomainError("资源不具备该搜索区域能力", 409)
        distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
        if distance > asset["range_km"]:
            raise DomainError("搜索区域超出资源航程", 409)
        return distance

    def _create_assignment(self, conn: sqlite3.Connection, actor: str, incident: sqlite3.Row,
                           area: sqlite3.Row, asset: sqlite3.Row, distance: float,
                           client_event_id: str | None = None) -> dict[str, Any]:
        """在同一事务内占用资源、更新区域并签发派单，避免资源被占而区域未派单。"""
        now = utcnow()
        changed = conn.execute(
            "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=? AND status='available' AND version=?",
            (now, asset["id"], asset["version"]),
        )
        if changed.rowcount != 1:
            raise DomainError("资源已被其他任务占用", 409)
        basis = {
            "distance_km": round(distance, 2),
            "sea_state": incident["sea_state"],
            "asset_max_sea_state": asset["max_sea_state"],
            "asset_range_km": asset["range_km"],
            "asset_capabilities": json.loads(asset["capabilities"]),
            "area_kind": area["kind"],
            "incident_version": incident["version"],
            "area_version": area["version"],
            "asset_version": asset["version"],
        }
        next_id = conn.execute("SELECT COALESCE(MAX(id),0)+1 AS n FROM assignments").fetchone()["n"]
        code = "ASG-%04d" % next_id
        try:
            cur = conn.execute(
                """INSERT INTO assignments(code,client_event_id,incident_id,area_id,asset_id,status,basis,
                   incident_version,area_version,asset_version,created_by,created_at,updated_at)
                   VALUES(?,?,?,?,?,'issued',?,?,?,?,?,?,?)""",
                (code, client_event_id, incident["id"], area["id"], asset["id"], json_dump(basis),
                 incident["version"], area["version"], asset["version"], actor, now, now),
            )
        except sqlite3.IntegrityError as exc:
            raise DomainError("离线派单已合并，不能重复占用资源", 409) from exc
        conn.execute(
            "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
            (asset["id"], now, area["id"]),
        )
        return dict(conn.execute("SELECT * FROM assignments WHERE id=?", (cur.lastrowid,)).fetchone())

    def _active_order(self, conn: sqlite3.Connection, area_id: int) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM assignments WHERE area_id=? AND status IN ('issued','executing') ORDER BY id DESC",
            (area_id,),
        ).fetchone()

    def _release_asset(self, conn: sqlite3.Connection, asset_id: int, exclude_order_id: int | None = None) -> None:
        """资源没有其他有效派单时才释放，避免误放仍在执行的任务。"""
        query = "SELECT 1 FROM assignments WHERE asset_id=? AND status IN ('issued','executing')"
        params: list[Any] = [asset_id]
        if exclude_order_id is not None:
            query += " AND id!=?"
            params.append(exclude_order_id)
        if conn.execute(query, params).fetchone():
            return
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        if asset and asset["status"] == "assigned":
            conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
                         (utcnow(), asset_id))

    def _invalidate_order(self, conn: sqlite3.Connection, actor: str, order: sqlite3.Row,
                          reason: str, area_outcome: str = "planned") -> None:
        """未执行派单失效：释放资源、区域回到待派（或放弃），并记录时间线。"""
        now = utcnow()
        conn.execute(
            "UPDATE assignments SET status='invalidated',review_pending=0,invalid_reason=?,updated_at=? WHERE id=?",
            (reason, now, order["id"]),
        )
        self._release_asset(conn, order["asset_id"], exclude_order_id=order["id"])
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (order["area_id"],)).fetchone()
        if area and area["assigned_asset_id"] == order["asset_id"] and area["status"] in ("assigned", "active"):
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=NULL,status=?,version=version+1,updated_at=? WHERE id=?",
                (area_outcome, now, area["id"]),
            )
            self._audit(conn, order["incident_id"], actor,
                        "area.unassigned" if area_outcome == "planned" else "area." + area_outcome,
                        {"area_id": area["id"], "assignment": order["code"], "reason": reason})
        self._audit(conn, order["incident_id"], actor, "assignment.invalidated",
                    {"assignment": order["code"], "area_id": order["area_id"],
                     "asset_id": order["asset_id"], "reason": reason})

    def _flag_review(self, conn: sqlite3.Connection, actor: str, order: sqlite3.Row, reason: str) -> None:
        """执行中的派单保留原依据，仅追加待复核原因。"""
        reasons = json.loads(order["review_reasons"])
        if reason not in reasons:
            reasons.append(reason)
        conn.execute(
            "UPDATE assignments SET review_pending=1,review_reasons=?,updated_at=? WHERE id=?",
            (json_dump(reasons), utcnow(), order["id"]),
        )
        self._audit(conn, order["incident_id"], actor, "assignment.review_pending",
                    {"assignment": order["code"], "area_id": order["area_id"],
                     "asset_id": order["asset_id"], "reasons": reasons})

    # ------------------------------------------------------------------
    # 事件与资源
    # ------------------------------------------------------------------

    def create_incident(self, actor: str, role: str, code: str, vessel_name: str,
                        latitude: float, longitude: float, uncertainty_km: float,
                        sea_state: int, lead_org: str, drift_direction: float = 0,
                        drift_speed_kn: float = 0, description: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "创建遇险事件")
        code, vessel_name, lead_org = code.strip(), vessel_name.strip(), lead_org.strip()
        if not code or not vessel_name or not lead_org:
            raise DomainError("事件编号、船名和负责机构不能为空")
        lat, lon = validate_position(latitude, longitude)
        try:
            uncertainty_km = float(uncertainty_km)
            sea_state = int(sea_state)
            drift_direction = float(drift_direction)
            drift_speed_kn = float(drift_speed_kn)
        except (TypeError, ValueError) as exc:
            raise DomainError("不确定半径、海况和漂移参数必须是数值") from exc
        if uncertainty_km <= 0 or uncertainty_km > 1000:
            raise DomainError("不确定半径应在 0 到 1000 公里之间")
        if not 0 <= sea_state <= 9 or drift_speed_kn < 0:
            raise DomainError("海况或漂移速度无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            duplicate = conn.execute(
                "SELECT * FROM incidents WHERE vessel_name=? AND status IN ('reported','coordinating','recovering') ORDER BY id DESC",
                (vessel_name,),
            ).fetchall()
            duplicate_of = None
            for row in duplicate:
                if haversine_km(lat, lon, row["latitude"], row["longitude"]) <= max(20.0, uncertainty_km + row["uncertainty_km"]):
                    duplicate_of = row["id"]
                    break
            status = "duplicate" if duplicate_of else "reported"
            try:
                cur = conn.execute(
                    """INSERT INTO incidents(code,vessel_name,description,latitude,longitude,uncertainty_km,
                       drift_direction,drift_speed_kn,sea_state,status,lead_org,duplicate_of,created_by,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (code, vessel_name, description.strip(), lat, lon, uncertainty_km, drift_direction, drift_speed_kn,
                     sea_state, status, lead_org, duplicate_of, actor, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("事件编号已存在", 409) from exc
            incident_id = int(cur.lastrowid)
            self._audit(conn, incident_id, actor, "incident.reported", {"duplicate_of": duplicate_of})
            if duplicate_of:
                self._audit(conn, duplicate_of, actor, "incident.duplicate_detected", {"duplicate_incident": code})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def list_assets(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM assets ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def add_asset(self, actor: str, role: str, name: str, kind: str,
                  capabilities: list[str], latitude: float, longitude: float,
                  speed_kn: float, range_km: float, max_sea_state: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "登记搜救资源")
        lat, lon = validate_position(latitude, longitude)
        name, kind = name.strip(), kind.strip()
        caps = sorted({str(item).strip() for item in capabilities if str(item).strip()})
        if not name or not kind or not caps:
            raise DomainError("资源名称、类型和能力不能为空")
        try:
            speed_kn, range_km, max_sea_state = float(speed_kn), float(range_km), int(max_sea_state)
        except (TypeError, ValueError) as exc:
            raise DomainError("速度和航程参数必须是数值") from exc
        if speed_kn <= 0 or range_km <= 0 or not 0 <= max_sea_state <= 9:
            raise DomainError("速度、航程或适用海况无效")
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    """INSERT INTO assets(name,kind,capabilities,latitude,longitude,speed_kn,range_km,max_sea_state,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (name, kind, json_dump(caps), lat, lon, speed_kn, range_km, max_sea_state, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("资源名称已存在", 409) from exc
            self._audit(conn, None, actor, "asset.registered", {"asset_id": cur.lastrowid, "name": name})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_search_area(self, actor: str, role: str, incident_id: int, code: str,
                           kind: str, center_lat: float, center_lon: float,
                           radius_km: float, priority: int = 3, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "创建搜索区域")
        lat, lon = validate_position(center_lat, center_lon)
        kind, code = kind.strip(), code.strip()
        if not kind or not code:
            raise DomainError("区域类型和编号不能为空")
        try:
            radius_km, priority = float(radius_km), int(priority)
        except (TypeError, ValueError) as exc:
            raise DomainError("半径和优先级必须是数值") from exc
        if radius_km <= 0 or not 1 <= priority <= 5:
            raise DomainError("搜索半径或优先级无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("当前事件不能创建搜索区域", 409)
            try:
                cur = conn.execute(
                    """INSERT INTO search_areas(incident_id,code,kind,center_lat,center_lon,radius_km,priority,note,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (incident_id, code, kind, lat, lon, radius_km, priority, note.strip(), now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("搜索区域编号已存在", 409) from exc
            self._audit(conn, incident_id, actor, "area.created", {"area_id": cur.lastrowid, "code": code})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (cur.lastrowid,)).fetchone())

    # ------------------------------------------------------------------
    # 派单：签发、改派、执行、复核、草稿
    # ------------------------------------------------------------------

    def assign_area(self, actor: str, role: str, area_id: int, asset_id: int,
                    expected_asset_version: int | None = None,
                    expected_area_version: int | None = None,
                    expected_incident_version: int | None = None,
                    client_event_id: str | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "分配搜索任务")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not area or not asset:
                raise DomainError("搜索区域或资源不存在", 404)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
            if not incident or incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("事件当前不可分配", 409)
            self._freeze_versions(conn, actor, "assign", incident, area, asset,
                                  expected_incident_version, expected_area_version, expected_asset_version,
                                  {"area_id": area_id, "asset_id": asset_id})
            if area["assigned_asset_id"] is not None or self._active_order(conn, area_id):
                raise DomainError("搜索区域已经分配", 409)
            distance = self._validate_assignment(incident, area, asset)
            order = self._create_assignment(conn, actor, incident, area, asset, distance, client_event_id)
            self._audit(conn, area["incident_id"], actor, "assignment.issued",
                        {"assignment": order["code"], "area_id": area_id, "asset_id": asset_id,
                         "distance_km": json.loads(order["basis"])["distance_km"]})
            updated = dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())
            updated["assignment"] = order
            return updated

    def reassign_area(self, actor: str, role: str, area_id: int, asset_id: int,
                      expected_incident_version: int | None = None,
                      expected_area_version: int | None = None,
                      expected_asset_version: int | None = None,
                      note: str = "") -> dict[str, Any]:
        """改派：冻结事件与区域版本后，在同一事务内作废旧派单、释放旧资源、签发新派单。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "改派搜索任务")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not area or not asset:
                raise DomainError("搜索区域或资源不存在", 404)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
            if not incident or incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("事件当前不可改派", 409)
            self._freeze_versions(conn, actor, "reassign", incident, area, asset,
                                  expected_incident_version, expected_area_version, expected_asset_version,
                                  {"area_id": area_id, "asset_id": asset_id, "note": note.strip()})
            order = self._active_order(conn, area_id)
            if not order:
                raise DomainError("区域当前没有可改派的派单", 409)
            if order["asset_id"] == asset["id"]:
                raise DomainError("新资源与当前派单相同", 409)
            distance = self._validate_assignment(incident, area, asset)
            now = utcnow()
            conn.execute("UPDATE assignments SET status='superseded',updated_at=? WHERE id=?", (now, order["id"]))
            self._release_asset(conn, order["asset_id"], exclude_order_id=order["id"])
            new_order = self._create_assignment(conn, actor, incident, area, asset, distance)
            self._audit(conn, incident["id"], actor, "assignment.reassigned",
                        {"assignment": new_order["code"], "superseded": order["code"], "area_id": area_id,
                         "from_asset_id": order["asset_id"], "to_asset_id": asset["id"], "note": note.strip()})
            updated = dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())
            updated["assignment"] = new_order
            return updated

    def start_assignment(self, actor: str, role: str, assignment_id: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "确认派单执行")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            order = conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone()
            if not order:
                raise DomainError("派单不存在", 404)
            if order["status"] != "issued":
                raise DomainError("派单当前不能转为执行", 409)
            conn.execute("UPDATE assignments SET status='executing',updated_at=? WHERE id=?",
                         (utcnow(), assignment_id))
            self._audit(conn, order["incident_id"], actor, "assignment.executing",
                        {"assignment": order["code"], "area_id": order["area_id"], "asset_id": order["asset_id"]})
            return dict(conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone())

    def review_assignment(self, actor: str, role: str, assignment_id: int, decision: str,
                          note: str = "") -> dict[str, Any]:
        """复核执行中的派单：confirm 继续执行，invalidate 作废并释放资源。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "复核派单")
        if decision not in {"confirm", "invalidate"}:
            raise DomainError("复核结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            order = conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone()
            if not order:
                raise DomainError("派单不存在", 404)
            if not order["review_pending"]:
                raise DomainError("派单没有待复核项", 409)
            reasons = json.loads(order["review_reasons"])
            if decision == "confirm":
                conn.execute(
                    "UPDATE assignments SET review_pending=0,review_reasons='[]',updated_at=? WHERE id=?",
                    (utcnow(), assignment_id),
                )
                self._audit(conn, order["incident_id"], actor, "assignment.review_confirmed",
                            {"assignment": order["code"], "reasons": reasons, "note": note.strip()})
            else:
                reason = "；".join(reasons + ([note.strip()] if note.strip() else [])) or "复核作废"
                self._invalidate_order(conn, actor, order, reason, area_outcome="planned")
            return dict(conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone())

    def submit_assignment_draft(self, actor: str, role: str, draft_id: int) -> dict[str, Any]:
        """版本冲突后保留的草稿，按当前状态重新提交。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "提交改派草稿")
        with self.connect() as conn:
            draft = conn.execute("SELECT * FROM assignment_drafts WHERE id=?", (draft_id,)).fetchone()
            if not draft:
                raise DomainError("草稿不存在", 404)
            if draft["status"] != "open":
                raise DomainError("草稿已处理", 409)
            payload = json.loads(draft["payload"])
            action = draft["action"]
        if action == "reassign":
            result = self.reassign_area(actor, role, payload["area_id"], payload["asset_id"],
                                        note=payload.get("note", ""))
        else:
            result = self.assign_area(actor, role, payload["area_id"], payload["asset_id"])
        with self.connect() as conn:
            conn.execute("UPDATE assignment_drafts SET status='submitted',updated_at=? WHERE id=?",
                         (utcnow(), draft_id))
        result["draft_id"] = draft_id
        return result

    def discard_assignment_draft(self, actor: str, role: str, draft_id: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "放弃改派草稿")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            draft = conn.execute("SELECT * FROM assignment_drafts WHERE id=?", (draft_id,)).fetchone()
            if not draft:
                raise DomainError("草稿不存在", 404)
            if draft["status"] != "open":
                raise DomainError("草稿已处理", 409)
            conn.execute("UPDATE assignment_drafts SET status='discarded',updated_at=? WHERE id=?",
                         (utcnow(), draft_id))
            return dict(conn.execute("SELECT * FROM assignment_drafts WHERE id=?", (draft_id,)).fetchone())

    # ------------------------------------------------------------------
    # 线索
    # ------------------------------------------------------------------

    def record_clue(self, actor: str, role: str, incident_id: int, client_event_id: str,
                    latitude: float, longitude: float, confidence: float, source: str,
                    area_id: int | None = None, details: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator", "field"}, "记录搜索线索")
        lat, lon = validate_position(latitude, longitude)
        event_id, source = client_event_id.strip(), source.strip()
        if not event_id or not source:
            raise DomainError("事件幂等编号和线索来源不能为空")
        try:
            confidence = float(confidence)
        except (TypeError, ValueError) as exc:
            raise DomainError("线索置信度必须是数值") from exc
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
            if existing:
                return dict(existing)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能新增线索", 409)
            if area_id is not None:
                area = conn.execute("SELECT * FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)).fetchone()
                if not area:
                    raise DomainError("搜索区域不属于该事件", 409)
            distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
            status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
            cur = conn.execute(
                """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
                   distance_from_incident_km,reporter,details,recorded_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (incident_id, area_id, event_id, lat, lon, confidence, source, status, distance, actor, details.strip(), utcnow()),
            )
            self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (cur.lastrowid,)).fetchone())

    def verify_clue(self, actor: str, role: str, clue_id: int, status: str,
                    expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "analyst"}, "核验线索")
        if status not in {"verified", "rejected", "unverified"}:
            raise DomainError("线索状态无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            clue = conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone()
            if not clue:
                raise DomainError("线索不存在", 404)
            if status == "verified" and clue["status"] == "invalid" and role != "coordinator":
                raise DomainError("异常位置线索只能由协调员确认", 403)
            conn.execute("UPDATE clues SET status=?,merged_at=? WHERE id=?", (status, utcnow(), clue_id))
            self._audit(conn, clue["incident_id"], actor, "clue.reviewed", {"clue_id": clue_id, "status": status})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone())

    # ------------------------------------------------------------------
    # 失效触发：资源撤回、区域结束、海况更新、事件结束
    # ------------------------------------------------------------------

    def withdraw_asset(self, actor: str, role: str, asset_id: int, reason: str,
                       expected_asset_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "撤回资源")
        if not reason.strip():
            raise DomainError("撤回原因不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not asset:
                raise DomainError("资源不存在", 404)
            if asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] == "available":
                raise DomainError("资源当前未分配", 409)
            now = utcnow()
            orders = conn.execute(
                "SELECT * FROM assignments WHERE asset_id=? AND status IN ('issued','executing')", (asset_id,)
            ).fetchall()
            for order in orders:
                if order["status"] == "issued":
                    self._invalidate_order(conn, actor, order, "资源撤回：" + reason.strip(), area_outcome="planned")
                else:
                    self._flag_review(conn, actor, order, "资源已撤回：" + reason.strip())
            # 兼容没有派单的历史区域占用
            areas = conn.execute(
                "SELECT * FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active')", (asset_id,)
            ).fetchall()
            for area in areas:
                if not self._active_order(conn, area["id"]):
                    conn.execute(
                        "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
                        (now, area["id"]),
                    )
                    self._audit(conn, area["incident_id"], actor, "area.unassigned",
                                {"area_id": area["id"], "reason": reason.strip()})
            conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, asset_id))
            self._audit(conn, None, actor, "asset.withdrawn", {"asset_id": asset_id, "reason": reason.strip()})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone())

    def transfer_incident(self, actor: str, role: str, incident_id: int, new_org: str,
                          expected_version: int, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "移交事件")
        new_org = new_org.strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能移交", 409)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            conn.execute(
                "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=? AND version=?",
                (new_org, utcnow(), incident_id, expected_version),
            )
            self._audit(conn, incident_id, actor, "incident.transferred", {"from": incident["lead_org"], "to": new_org, "note": note.strip()})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def complete_area(self, actor: str, role: str, area_id: int, outcome: str,
                      expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束搜索区域")
        if outcome not in {"completed", "abandoned"}:
            raise DomainError("区域结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if not area:
                raise DomainError("搜索区域不存在", 404)
            if area["status"] in {"completed", "abandoned"}:
                raise DomainError("搜索区域已经结束", 409)
            if expected_version is not None and area["version"] != int(expected_version):
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            now = utcnow()
            order = self._active_order(conn, area_id)
            if order:
                if order["status"] == "executing" and outcome == "completed":
                    conn.execute("UPDATE assignments SET status='completed',updated_at=? WHERE id=?", (now, order["id"]))
                    self._audit(conn, area["incident_id"], actor, "assignment.completed",
                                {"assignment": order["code"], "area_id": area_id})
                else:
                    reason = ("区域已结束（%s），派单未执行" % outcome) if order["status"] == "issued" else ("区域已结束（%s）" % outcome)
                    conn.execute(
                        "UPDATE assignments SET status='invalidated',review_pending=0,invalid_reason=?,updated_at=? WHERE id=?",
                        (reason, now, order["id"]),
                    )
                    self._audit(conn, area["incident_id"], actor, "assignment.invalidated",
                                {"assignment": order["code"], "area_id": area_id, "reason": reason})
            if area["assigned_asset_id"] is not None:
                self._release_asset(conn, area["assigned_asset_id"])
            conn.execute("UPDATE search_areas SET status=?,assigned_asset_id=NULL,version=version+1,updated_at=? WHERE id=?",
                         (outcome, now, area_id))
            self._audit(conn, area["incident_id"], actor, "area." + outcome, {"area_id": area_id})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def update_sea_state(self, actor: str, role: str, incident_id: int, sea_state: int,
                         expected_version: int | None = None) -> dict[str, Any]:
        """海况更新后重算派单：未执行的失效，执行中的保留原依据待复核。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "更新海况")
        try:
            sea_state = int(sea_state)
        except (TypeError, ValueError) as exc:
            raise DomainError("海况必须是数值") from exc
        if not 0 <= sea_state <= 9:
            raise DomainError("海况无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能更新海况", 409)
            if expected_version is not None and incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            old_sea_state = incident["sea_state"]
            conn.execute("UPDATE incidents SET sea_state=?,version=version+1,updated_at=? WHERE id=?",
                         (sea_state, utcnow(), incident_id))
            invalidated, flagged = 0, 0
            orders = conn.execute(
                "SELECT * FROM assignments WHERE incident_id=? AND status IN ('issued','executing')", (incident_id,)
            ).fetchall()
            for order in orders:
                asset = conn.execute("SELECT * FROM assets WHERE id=?", (order["asset_id"],)).fetchone()
                if not asset or sea_state <= asset["max_sea_state"]:
                    continue
                reason = "海况由%d级升至%d级，超出资源适用上限%d级" % (old_sea_state, sea_state, asset["max_sea_state"])
                if order["status"] == "issued":
                    self._invalidate_order(conn, actor, order, reason, area_outcome="planned")
                    invalidated += 1
                else:
                    self._flag_review(conn, actor, order, reason)
                    flagged += 1
            self._audit(conn, incident_id, actor, "incident.sea_state_updated",
                        {"from": old_sea_state, "to": sea_state, "invalidated": invalidated, "review_pending": flagged})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def close_incident(self, actor: str, role: str, incident_id: int, outcome: str,
                       expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束事件")
        if outcome not in {"resolved", "cancelled", "false_alarm"}:
            raise DomainError("结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            active_area = conn.execute(
                "SELECT COUNT(*) AS c FROM search_areas WHERE incident_id=? AND status IN ('planned','assigned','active')",
                (incident_id,),
            ).fetchone()["c"]
            if active_area and outcome != "false_alarm":
                raise DomainError("仍有未结束搜索区域，不能关闭事件", 409)
            # 事件结束：未执行派单失效重算，执行中的保留原依据待复核
            orders = conn.execute(
                "SELECT * FROM assignments WHERE incident_id=? AND status IN ('issued','executing')", (incident_id,)
            ).fetchall()
            for order in orders:
                if order["status"] == "issued":
                    self._invalidate_order(conn, actor, order, "事件已结束", area_outcome="abandoned")
                else:
                    self._flag_review(conn, actor, order, "事件已结束，待复核")
            status = "closed" if outcome == "resolved" else "cancelled"
            conn.execute("UPDATE incidents SET status=?,version=version+1,updated_at=? WHERE id=?", (status, utcnow(), incident_id))
            self._audit(conn, incident_id, actor, "incident.closed", {"outcome": outcome})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    # ------------------------------------------------------------------
    # 离线批次：幂等合并、字段冲突并列保留、失败整批恢复
    # ------------------------------------------------------------------

    def _merge_offline_clue(self, conn: sqlite3.Connection, actor: str, batch_id: str,
                            event_id: str, event: dict[str, Any]) -> dict[str, Any]:
        existing = conn.execute("SELECT * FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
        if existing:
            conflicts: list[tuple[str, Any, Any]] = []
            for field in ("incident_id", "area_id", "latitude", "longitude", "confidence", "source", "details"):
                incoming = event.get(field)
                if incoming is None:
                    continue
                current = existing[field]
                if field in ("latitude", "longitude", "confidence"):
                    same = abs(float(incoming) - float(current)) < 1e-9
                elif field in ("incident_id", "area_id"):
                    same = current is not None and int(incoming) == int(current)
                else:
                    same = str(incoming).strip() == str(current or "").strip()
                if not same:
                    conflicts.append((field, current, incoming))
            if conflicts:
                now = utcnow()
                for field, current, incoming in conflicts:
                    conn.execute(
                        """INSERT INTO clue_conflicts(client_event_id,clue_id,field,existing_value,incoming_value,batch_id,actor,created_at)
                           VALUES(?,?,?,?,?,?,?,?)""",
                        (event_id, existing["id"], field,
                         None if current is None else str(current), str(incoming), batch_id, actor, now),
                    )
                fields = [item[0] for item in conflicts]
                self._audit(conn, existing["incident_id"], actor, "clue.conflict",
                            {"clue_id": existing["id"], "event_id": event_id, "batch_id": batch_id, "fields": fields})
                return {"client_event_id": event_id, "status": "conflict", "record_id": existing["id"], "fields": fields}
            return {"client_event_id": event_id, "status": "merged", "record_id": existing["id"], "idempotent": True}
        incident_id = int(event["incident_id"])
        lat, lon = validate_position(event["latitude"], event["longitude"])
        confidence = float(event["confidence"])
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        if incident["status"] in CLOSED_INCIDENT:
            raise DomainError("已结束事件不能新增线索", 409)
        area_id = event.get("area_id")
        if area_id is not None and not conn.execute(
            "SELECT 1 FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)
        ).fetchone():
            raise DomainError("搜索区域不属于该事件", 409)
        distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
        status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
        cur = conn.execute(
            """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
               distance_from_incident_km,reporter,details,recorded_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (incident_id, area_id, event_id, lat, lon, confidence,
             str(event.get("source", "offline")).strip(), status, distance, actor,
             str(event.get("details", "")).strip(), utcnow()),
        )
        self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
        return {"client_event_id": event_id, "status": "merged", "record_id": cur.lastrowid}

    def _merge_offline_assignment(self, conn: sqlite3.Connection, actor: str,
                                  event_id: str, event: dict[str, Any]) -> dict[str, Any]:
        existing = conn.execute("SELECT * FROM assignments WHERE client_event_id=?", (event_id,)).fetchone()
        if existing:
            return {"client_event_id": event_id, "status": "merged", "record_id": existing["id"], "idempotent": True}
        area_id, asset_id = int(event["area_id"]), int(event["asset_id"])
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        if not area or not asset:
            raise DomainError("搜索区域或资源不存在", 404)
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
        if not incident or incident["status"] not in ACTIVE_INCIDENT:
            raise DomainError("事件当前不可分配", 409)
        if area["assigned_asset_id"] is not None or self._active_order(conn, area_id):
            raise DomainError("搜索区域已经分配", 409)
        distance = self._validate_assignment(incident, area, asset)
        order = self._create_assignment(conn, actor, incident, area, asset, distance, client_event_id=event_id)
        self._audit(conn, incident["id"], actor, "assignment.issued",
                    {"assignment": order["code"], "area_id": area_id, "asset_id": asset_id,
                     "offline": True, "event_id": event_id})
        return {"client_event_id": event_id, "status": "merged", "record_id": order["id"]}

    def merge_offline_batch(self, actor: str, role: str, client_batch_id: str,
                            events: list[dict[str, Any]]) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"operator", "field", "coordinator"}, "合并离线记录")
        batch_id = client_batch_id.strip()
        if not batch_id or not isinstance(events, list):
            raise DomainError("批次编号和事件列表不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_id,)).fetchone()
            if existing:
                return {"batch_id": batch_id, "idempotent": True, "status": existing["status"], "summary": json.loads(existing["summary"])}
            results = []
            for event in events:
                event_id = str(event.get("client_event_id", "")).strip()
                try:
                    if not event_id:
                        raise DomainError("离线事件缺少 client_event_id")
                    event_type = event.get("type")
                    if event_type == "clue":
                        results.append(self._merge_offline_clue(conn, actor, batch_id, event_id, event))
                    elif event_type == "assignment":
                        results.append(self._merge_offline_assignment(conn, actor, event_id, event))
                    elif event_type == "timeline":
                        incident_id = int(event["incident_id"])
                        if not conn.execute("SELECT 1 FROM incidents WHERE id=?", (incident_id,)).fetchone():
                            raise DomainError("事件不存在", 404)
                        self._audit(conn, incident_id, actor, event.get("action", "offline.note"), event.get("details", {}))
                        results.append({"client_event_id": event_id, "status": "merged", "record_id": None})
                    else:
                        raise DomainError("不支持的离线事件类型")
                except (DomainError, KeyError, TypeError, ValueError) as exc:
                    results.append({"client_event_id": event_id, "status": "rejected", "error": str(exc)})
            summary = {
                "accepted": sum(1 for item in results if item["status"] == "merged"),
                "rejected": sum(1 for item in results if item["status"] == "rejected"),
                "conflicts": sum(1 for item in results if item["status"] == "conflict"),
                "events": results,
            }
            now = utcnow()
            try:
                conn.execute(
                    "INSERT INTO offline_batches(client_batch_id,actor,status,received_at,merged_at,summary) VALUES(?,?,?,?,?,?)",
                    (batch_id, actor, "merged", now, now, json_dump(summary)),
                )
            except sqlite3.IntegrityError:
                # 同一批次并发到达：回滚本次写入，以已存在的完整批次为准
                conn.rollback()
                existing = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_id,)).fetchone()
                return {"batch_id": batch_id, "idempotent": True, "status": existing["status"], "summary": json.loads(existing["summary"])}
            self._audit(conn, None, actor, "offline.batch_merged",
                        {"batch_id": batch_id, **{k: summary[k] for k in ("accepted", "rejected", "conflicts")}})
            return {"batch_id": batch_id, "idempotent": False, "status": "merged", "summary": summary}

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            assignments = [dict(r) for r in conn.execute("SELECT * FROM assignments ORDER BY id DESC").fetchall()]
            drafts = [dict(r) for r in conn.execute("SELECT * FROM assignment_drafts ORDER BY id DESC").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            conflicts = [dict(r) for r in conn.execute("SELECT * FROM clue_conflicts ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 300").fetchall()]
        return {"incidents": incidents, "assets": assets, "search_areas": areas, "assignments": assignments,
                "assignment_drafts": drafts, "clues": clues, "clue_conflicts": conflicts, "timeline": timeline}

    def incident_timeline(self, incident_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM timeline WHERE incident_id=? ORDER BY id", (incident_id,)).fetchall()
        return [dict(r) for r in rows]

    def seed_demo(self) -> dict[str, Any]:
        with self.connect() as conn:
            if conn.execute("SELECT COUNT(*) AS c FROM incidents").fetchone()["c"]:
                return {"seeded": False, "reason": "已有数据"}
        incident = self.create_incident("coord-demo", "coordinator", "SAR-2026-001", "远星号", 31.2, 122.5, 15.0, 3, "东海搜救中心", description="演示遇险事件")
        self.add_asset("coord-demo", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 22.0, 180.0, 6)
        self.add_asset("coord-demo", "coordinator", "救助直升机", "aircraft", ["air", "night"], 30.8, 122.1, 180.0, 260.0, 5)
        self.create_search_area("coord-demo", "coordinator", incident["id"], "AREA-A", "surface", 31.2, 122.5, 20.0, 1, "首要搜索区")
        return {"seeded": True, "incident_id": incident["id"]}


class ApiHandler(BaseHTTPRequestHandler):
    service: MaritimeSARService

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _actor(self) -> tuple[str, str]:
        return self.headers.get("X-User", ""), self.headers.get("X-Role", "viewer")

    def _json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise DomainError("Content-Length 无效") from exc
        if length > 2_000_000:
            raise DomainError("请求体过大", 413)
        if not length:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise DomainError("请求体不是有效 JSON") from exc
        if not isinstance(data, dict):
            raise DomainError("JSON 请求体必须是对象")
        return data

    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path in {"/", "/index.html"}:
                body = (ROOT / "static" / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/health":
                self._send(200, {"status": "ok", "service": "maritime-sar"})
                return
            if path == "/api/state":
                self._send(200, self.service.state(*self._actor()))
                return
            if path.startswith("/api/incidents/") and path.endswith("/timeline"):
                incident_id = int(path.split("/")[3])
                self._send(200, {"timeline": self.service.incident_timeline(incident_id)})
                return
            self._send(404, {"error": "接口不存在"})
        except (DomainError, ValueError) as exc:
            self._send(getattr(exc, "status", 400), {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            data, (actor, role) = self._json(), self._actor()
            if path == "/api/incidents":
                result = self.service.create_incident(actor, role, **data)
            elif path == "/api/assets":
                result = self.service.add_asset(actor, role, **data)
            elif path == "/api/areas":
                result = self.service.create_search_area(actor, role, **data)
            elif path == "/api/assignments":
                result = self.service.assign_area(actor, role, **data)
            elif path == "/api/assignments/reassign":
                result = self.service.reassign_area(actor, role, **data)
            elif path == "/api/assignments/start":
                result = self.service.start_assignment(actor, role, **data)
            elif path == "/api/assignments/review":
                result = self.service.review_assignment(actor, role, **data)
            elif path == "/api/assignments/drafts/submit":
                result = self.service.submit_assignment_draft(actor, role, **data)
            elif path == "/api/assignments/drafts/discard":
                result = self.service.discard_assignment_draft(actor, role, **data)
            elif path == "/api/clues":
                result = self.service.record_clue(actor, role, **data)
            elif path == "/api/clues/verify":
                result = self.service.verify_clue(actor, role, **data)
            elif path == "/api/assets/withdraw":
                result = self.service.withdraw_asset(actor, role, **data)
            elif path == "/api/areas/complete":
                result = self.service.complete_area(actor, role, **data)
            elif path == "/api/incidents/sea_state":
                result = self.service.update_sea_state(actor, role, **data)
            elif path == "/api/incidents/transfer":
                result = self.service.transfer_incident(actor, role, **data)
            elif path == "/api/incidents/close":
                result = self.service.close_incident(actor, role, **data)
            elif path == "/api/offline/batch":
                result = self.service.merge_offline_batch(actor, role, **data)
            else:
                raise DomainError("接口不存在", 404)
            self._send(201, result)
        except DomainError as exc:
            payload = {"error": str(exc)}
            payload.update(exc.extra)
            self._send(exc.status, payload)
        except (KeyError, TypeError, ValueError) as exc:
            self._send(400, {"error": "请求参数错误: %s" % exc})
        except Exception as exc:
            self._send(500, {"error": "服务器内部错误", "detail": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def serve(service: MaritimeSARService, host: str, port: int) -> None:
    ApiHandler.service = service
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print("Maritime SAR service listening on http://%s:%s" % (host, port))
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="海上搜救协调服务")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8206)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    service = MaritimeSARService(args.db)
    if args.init:
        print(json.dumps(service.seed_demo() if args.seed else {"initialized": True, "db": args.db}, ensure_ascii=False))
        return
    serve(service, args.host, args.port)


if __name__ == "__main__":
    main()
