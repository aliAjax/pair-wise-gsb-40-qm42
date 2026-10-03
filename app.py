"""海上搜救协调服务：事件、搜索区域、派单、资源占用与离线批次的可恢复协调流程。"""
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
OPEN_ASSIGNMENT = {"pending", "active"}
CLUE_CONFLICT_FIELDS = ("incident_id", "area_id", "latitude", "longitude", "confidence", "source", "details")


class DomainError(Exception):
    """业务错误；payload 会并入响应（如版本冲突时的最新版本与草稿）。"""

    def __init__(self, message: str, status: int = 400, payload: dict[str, Any] | None = None):
        super().__init__(message)
        self.status = status
        self.payload = payload or {}


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

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(%s)" % table)}
        if column not in cols:
            conn.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, column, definition))

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
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    area_id INTEGER NOT NULL REFERENCES search_areas(id),
                    asset_id INTEGER NOT NULL REFERENCES assets(id),
                    status TEXT NOT NULL DEFAULT 'pending',
                    basis TEXT NOT NULL DEFAULT '{}',
                    review_reasons TEXT NOT NULL DEFAULT '[]',
                    conflicts TEXT NOT NULL DEFAULT '[]',
                    client_event_id TEXT UNIQUE,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
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
                    conflicts TEXT NOT NULL DEFAULT '[]',
                    recorded_at TEXT NOT NULL,
                    merged_at TEXT
                );
                CREATE TABLE IF NOT EXISTS offline_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_batch_id TEXT NOT NULL UNIQUE,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL DEFAULT '[]',
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
                    client_event_id TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clues_incident ON clues(incident_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline(incident_id, id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_timeline_client_event
                    ON timeline(client_event_id) WHERE client_event_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_assignments_area ON assignments(area_id, status);
                CREATE INDEX IF NOT EXISTS idx_assignments_asset ON assignments(asset_id, status);
                """
            )
            self._ensure_column(conn, "clues", "conflicts", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(conn, "offline_batches", "payload", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(conn, "timeline", "client_event_id", "TEXT")

    def _audit(self, conn: sqlite3.Connection, incident_id: int | None, actor: str,
               action: str, details: dict[str, Any], client_event_id: str | None = None) -> None:
        conn.execute(
            "INSERT INTO timeline(incident_id,actor,action,details,client_event_id,created_at) VALUES(?,?,?,?,?,?)",
            (incident_id, actor, action, json_dump(details), client_event_id, utcnow()),
        )

    @staticmethod
    def _get(conn: sqlite3.Connection, table: str, row_id: Any, label: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM %s WHERE id=?" % table, (row_id,)).fetchone()
        if not row:
            raise DomainError("%s不存在" % label, 404)
        return row

    def _conflict(self, conn: sqlite3.Connection, message: str, *, draft: dict[str, Any] | None = None,
                  incident_id: int | None = None, area_id: int | None = None,
                  asset_id: int | None = None, assignment_id: int | None = None) -> None:
        """版本/状态冲突：返回最新版本与状态，调用方草稿随响应保留。"""
        current: dict[str, Any] = {}
        if incident_id is not None:
            row = conn.execute("SELECT id,version,status,sea_state FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if row:
                current["incident"] = dict(row)
        if area_id is not None:
            row = conn.execute("SELECT id,version,status,assigned_asset_id FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if row:
                current["area"] = dict(row)
        if asset_id is not None:
            row = conn.execute("SELECT id,version,status FROM assets WHERE id=?", (asset_id,)).fetchone()
            if row:
                current["asset"] = dict(row)
        if assignment_id is not None:
            row = conn.execute("SELECT id,version,status,review_reasons FROM assignments WHERE id=?", (assignment_id,)).fetchone()
            if row:
                data = dict(row)
                data["review_reasons"] = json.loads(data["review_reasons"] or "[]")
                current["assignment"] = data
        raise DomainError(message, 409, {"conflict": {"current": current, "draft": draft or {}}})

    # ---------- 遇险事件 ----------

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

    def transfer_incident(self, actor: str, role: str, incident_id: int, new_org: str,
                          expected_version: int, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "移交事件")
        new_org = new_org.strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = self._get(conn, "incidents", incident_id, "事件")
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能移交", 409)
            if incident["version"] != int(expected_version):
                self._conflict(conn, "事件已变化，请刷新后重试", incident_id=incident_id,
                               draft={"incident_id": incident_id, "new_org": new_org, "expected_version": expected_version})
            conn.execute(
                "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=? AND version=?",
                (new_org, utcnow(), incident_id, expected_version),
            )
            self._audit(conn, incident_id, actor, "incident.transferred", {"from": incident["lead_org"], "to": new_org, "note": note.strip()})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def update_sea_state(self, actor: str, role: str, incident_id: int, sea_state: int,
                         expected_version: int) -> dict[str, Any]:
        """海况更新：未执行派单失效重算，执行中派单保留原依据待复核。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "更新海况")
        try:
            sea_state = int(sea_state)
        except (TypeError, ValueError) as exc:
            raise DomainError("海况必须是数值") from exc
        if not 0 <= sea_state <= 9:
            raise DomainError("海况无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = self._get(conn, "incidents", incident_id, "事件")
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("已结束事件不能更新海况", 409)
            if incident["version"] != int(expected_version):
                self._conflict(conn, "事件版本已变化，请刷新后重试", incident_id=incident_id,
                               draft={"incident_id": incident_id, "sea_state": sea_state, "expected_version": expected_version})
            old = incident["sea_state"]
            conn.execute("UPDATE incidents SET sea_state=?,version=version+1,updated_at=? WHERE id=?", (sea_state, utcnow(), incident_id))
            self._audit(conn, incident_id, actor, "incident.sea_state_updated", {"from": old, "to": sea_state})
            self._reevaluate_assignments(conn, actor, incident_id=incident_id,
                                         reason="海况更新（%s→%s），派单依据失效" % (old, sea_state))
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def close_incident(self, actor: str, role: str, incident_id: int, outcome: str,
                       expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束事件")
        if outcome not in {"resolved", "cancelled", "false_alarm"}:
            raise DomainError("结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = self._get(conn, "incidents", incident_id, "事件")
            if incident["version"] != int(expected_version):
                self._conflict(conn, "事件已变化，请刷新后重试", incident_id=incident_id,
                               draft={"incident_id": incident_id, "outcome": outcome, "expected_version": expected_version})
            active_area = conn.execute(
                "SELECT COUNT(*) AS c FROM search_areas WHERE incident_id=? AND status IN ('planned','assigned','active')",
                (incident_id,),
            ).fetchone()["c"]
            if active_area and outcome != "false_alarm":
                raise DomainError("仍有未结束搜索区域，不能关闭事件", 409)
            status = "closed" if outcome == "resolved" else "cancelled"
            conn.execute("UPDATE incidents SET status=?,version=version+1,updated_at=? WHERE id=?", (status, utcnow(), incident_id))
            self._audit(conn, incident_id, actor, "incident.closed", {"outcome": outcome})
            self._reevaluate_assignments(conn, actor, incident_id=incident_id,
                                         reason="事件已结束（%s）" % outcome, area_release_status="abandoned")
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    # ---------- 资源 ----------

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

    def withdraw_asset(self, actor: str, role: str, asset_id: int, reason: str,
                       expected_asset_version: int) -> dict[str, Any]:
        """撤回资源：未执行派单失效重算；执行中派单保留原依据待复核，资源标记 withdrawn。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "撤回资源")
        reason = reason.strip()
        if not reason:
            raise DomainError("撤回原因不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            asset = self._get(conn, "assets", asset_id, "资源")
            if asset["version"] != int(expected_asset_version):
                self._conflict(conn, "资源状态已变化，请刷新后重试", asset_id=asset_id,
                               draft={"asset_id": asset_id, "reason": reason, "expected_asset_version": expected_asset_version})
            if asset["status"] == "available":
                raise DomainError("资源当前未分配", 409)
            self._reevaluate_assignments(conn, actor, asset_id=asset_id, reason="资源撤回：%s" % reason)
            still_active = conn.execute(
                "SELECT COUNT(*) AS c FROM assignments WHERE asset_id=? AND status='active'", (asset_id,)
            ).fetchone()["c"]
            now = utcnow()
            new_status = "withdrawn" if still_active else "available"
            conn.execute("UPDATE assets SET status=?,version=version+1,updated_at=? WHERE id=?", (new_status, now, asset_id))
            self._audit(conn, None, actor, "asset.withdrawn",
                        {"asset_id": asset_id, "reason": reason, "asset_status": new_status, "active_assignments": still_active})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone())

    # ---------- 搜索区域 ----------

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
            incident = self._get(conn, "incidents", incident_id, "事件")
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

    def complete_area(self, actor: str, role: str, area_id: int, outcome: str,
                      expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束搜索区域")
        if outcome not in {"completed", "abandoned"}:
            raise DomainError("区域结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = self._get(conn, "search_areas", area_id, "搜索区域")
            if area["status"] in {"completed", "abandoned"}:
                raise DomainError("搜索区域已经结束", 409)
            if expected_version is not None and area["version"] != int(expected_version):
                self._conflict(conn, "搜索区域已变化，请刷新后重试", area_id=area_id,
                               draft={"area_id": area_id, "outcome": outcome, "expected_version": expected_version})
            now = utcnow()
            assignment = conn.execute(
                "SELECT * FROM assignments WHERE area_id=? AND status IN ('pending','active') ORDER BY id DESC", (area_id,)
            ).fetchone()
            if assignment:
                conn.execute("UPDATE assignments SET status='completed',version=version+1,updated_at=? WHERE id=?", (now, assignment["id"]))
                self._audit(conn, area["incident_id"], actor, "assignment.completed",
                            {"assignment_id": assignment["id"], "area_id": area_id, "outcome": outcome})
            if area["assigned_asset_id"] is not None:
                conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, area["assigned_asset_id"]))
            conn.execute("UPDATE search_areas SET status=?,assigned_asset_id=NULL,version=version+1,updated_at=? WHERE id=?",
                         (outcome, now, area_id))
            self._audit(conn, area["incident_id"], actor, "area." + outcome, {"area_id": area_id})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    # ---------- 派单 ----------

    @staticmethod
    def _assignment_dict(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["basis"] = json.loads(data["basis"] or "{}")
        data["review_reasons"] = json.loads(data["review_reasons"] or "[]")
        data["conflicts"] = json.loads(data["conflicts"] or "[]")
        return data

    def get_assignment(self, assignment_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone()
        if not row:
            raise DomainError("派单不存在", 404)
        return self._assignment_dict(row)

    def _validate_dispatch(self, incident: sqlite3.Row, area: sqlite3.Row, asset: sqlite3.Row) -> float:
        """按能力、海况和航程确认有效占用，返回距离。"""
        capabilities = json.loads(asset["capabilities"])
        if area["kind"] not in capabilities:
            raise DomainError("资源不具备该搜索区域能力", 409)
        if incident["sea_state"] > asset["max_sea_state"]:
            raise DomainError("海况超出资源能力", 409)
        distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
        if distance > asset["range_km"]:
            raise DomainError("搜索区域超出资源航程", 409)
        return distance

    @staticmethod
    def _dispatch_basis(incident: sqlite3.Row, area: sqlite3.Row, asset: sqlite3.Row,
                        actor: str, distance: float, now: str) -> dict[str, Any]:
        return {
            "incident_version": incident["version"],
            "area_version": area["version"],
            "asset_version": asset["version"],
            "sea_state": incident["sea_state"],
            "area_kind": area["kind"],
            "distance_km": round(distance, 2),
            "asset_range_km": asset["range_km"],
            "asset_max_sea_state": asset["max_sea_state"],
            "asset_capabilities": json.loads(asset["capabilities"]),
            "assigned_by": actor,
            "assigned_at": now,
        }

    @staticmethod
    def _occupy_asset(conn: sqlite3.Connection, asset: sqlite3.Row, now: str) -> None:
        changed = conn.execute(
            "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=? AND status='available' AND version=?",
            (now, asset["id"], asset["version"]),
        )
        if changed.rowcount != 1:
            raise DomainError("资源已被其他任务占用", 409)

    def _insert_assignment(self, conn: sqlite3.Connection, incident: sqlite3.Row, area: sqlite3.Row,
                           asset: sqlite3.Row, actor: str, distance: float, now: str,
                           client_event_id: str | None = None, origin: str = "manual") -> int:
        """原子占用：资源置占用 + 区域置已派 + 生成派单，任一失败整体回滚。"""
        basis = self._dispatch_basis(incident, area, asset, actor, distance, now)
        self._occupy_asset(conn, asset, now)
        conn.execute(
            "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
            (asset["id"], now, area["id"]),
        )
        cur = conn.execute(
            """INSERT INTO assignments(incident_id,area_id,asset_id,status,basis,client_event_id,created_by,created_at,updated_at)
               VALUES(?,?,?,'pending',?,?,?,?,?)""",
            (incident["id"], area["id"], asset["id"], json_dump(basis), client_event_id, actor, now, now),
        )
        assignment_id = int(cur.lastrowid)
        self._audit(conn, incident["id"], actor, "assignment.created",
                    {"assignment_id": assignment_id, "area_id": area["id"], "asset_id": asset["id"],
                     "origin": origin, "basis": basis})
        return assignment_id

    def create_assignment(self, actor: str, role: str, area_id: int, asset_id: int,
                          expected_incident_version: int | None = None,
                          expected_area_version: int | None = None,
                          expected_asset_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "创建派单")
        draft = {"area_id": area_id, "asset_id": asset_id,
                 "expected_incident_version": expected_incident_version,
                 "expected_area_version": expected_area_version,
                 "expected_asset_version": expected_asset_version}
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = self._get(conn, "search_areas", area_id, "搜索区域")
            asset = self._get(conn, "assets", asset_id, "资源")
            incident = self._get(conn, "incidents", area["incident_id"], "事件")
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("事件当前不可派单", 409)
            if expected_incident_version is not None and incident["version"] != int(expected_incident_version):
                self._conflict(conn, "事件版本已变化，派单草稿已保留", draft=draft,
                               incident_id=incident["id"], area_id=area_id, asset_id=asset_id)
            if expected_area_version is not None and area["version"] != int(expected_area_version):
                self._conflict(conn, "搜索区域版本已变化，派单草稿已保留", draft=draft,
                               incident_id=incident["id"], area_id=area_id, asset_id=asset_id)
            if expected_asset_version is not None and asset["version"] != int(expected_asset_version):
                self._conflict(conn, "资源版本已变化，派单草稿已保留", draft=draft,
                               incident_id=incident["id"], area_id=area_id, asset_id=asset_id)
            if area["assigned_asset_id"] is not None or area["status"] != "planned":
                self._conflict(conn, "搜索区域已有派单，请刷新后重算", draft=draft,
                               incident_id=incident["id"], area_id=area_id, asset_id=asset_id)
            if asset["status"] != "available":
                self._conflict(conn, "资源当前不可用", draft=draft,
                               incident_id=incident["id"], area_id=area_id, asset_id=asset_id)
            distance = self._validate_dispatch(incident, area, asset)
            assignment_id = self._insert_assignment(conn, incident, area, asset, actor, distance, utcnow())
            row = conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone()
            return self._assignment_dict(row)

    def reassign_assignment(self, actor: str, role: str, assignment_id: int, new_asset_id: int,
                            expected_incident_version: int | None = None,
                            expected_area_version: int | None = None,
                            expected_asset_version: int | None = None) -> dict[str, Any]:
        """改派：先冻结事件与区域版本，再按航程、能力和海况确认新资源有效占用。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "改派")
        if expected_incident_version is None or expected_area_version is None:
            raise DomainError("改派必须先冻结事件与区域版本")
        draft = {"assignment_id": assignment_id, "new_asset_id": new_asset_id,
                 "expected_incident_version": expected_incident_version,
                 "expected_area_version": expected_area_version,
                 "expected_asset_version": expected_asset_version}
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            assignment = self._get(conn, "assignments", assignment_id, "派单")
            if assignment["status"] not in OPEN_ASSIGNMENT:
                raise DomainError("当前派单状态不能改派", 409)
            area = self._get(conn, "search_areas", assignment["area_id"], "搜索区域")
            incident = self._get(conn, "incidents", assignment["incident_id"], "事件")
            old_asset = self._get(conn, "assets", assignment["asset_id"], "资源")
            new_asset = self._get(conn, "assets", new_asset_id, "资源")
            if incident["version"] != int(expected_incident_version):
                self._conflict(conn, "事件版本已变化，改派草稿已保留", draft=draft,
                               incident_id=incident["id"], area_id=area["id"], assignment_id=assignment_id)
            if area["version"] != int(expected_area_version):
                self._conflict(conn, "搜索区域版本已变化，改派草稿已保留", draft=draft,
                               incident_id=incident["id"], area_id=area["id"], assignment_id=assignment_id)
            if expected_asset_version is not None and new_asset["version"] != int(expected_asset_version):
                self._conflict(conn, "资源版本已变化，改派草稿已保留", draft=draft,
                               incident_id=incident["id"], area_id=area["id"], asset_id=new_asset_id)
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("事件当前不可改派", 409)
            if new_asset["id"] == old_asset["id"]:
                raise DomainError("改派资源不能与原资源相同")
            if new_asset["status"] != "available":
                self._conflict(conn, "新资源当前不可用", draft=draft,
                               incident_id=incident["id"], area_id=area["id"], asset_id=new_asset_id)
            distance = self._validate_dispatch(incident, area, new_asset)
            now = utcnow()
            self._occupy_asset(conn, new_asset, now)
            conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, old_asset["id"]))
            conn.execute("UPDATE assignments SET status='superseded',version=version+1,updated_at=? WHERE id=?", (now, assignment_id))
            conn.execute("UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
                         (new_asset["id"], now, area["id"]))
            basis = self._dispatch_basis(incident, area, new_asset, actor, distance, now)
            cur = conn.execute(
                """INSERT INTO assignments(incident_id,area_id,asset_id,status,basis,created_by,created_at,updated_at)
                   VALUES(?,?,?,'pending',?,?,?,?)""",
                (incident["id"], area["id"], new_asset["id"], json_dump(basis), actor, now, now),
            )
            new_id = int(cur.lastrowid)
            self._audit(conn, incident["id"], actor, "assignment.reassigned",
                        {"old_assignment_id": assignment_id, "new_assignment_id": new_id,
                         "old_asset_id": old_asset["id"], "new_asset_id": new_asset["id"],
                         "area_id": area["id"], "basis": basis})
            row = conn.execute("SELECT * FROM assignments WHERE id=?", (new_id,)).fetchone()
            return self._assignment_dict(row)

    def start_assignment(self, actor: str, role: str, assignment_id: int,
                         expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "开始执行派单")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            assignment = self._get(conn, "assignments", assignment_id, "派单")
            if assignment["status"] != "pending":
                raise DomainError("仅未执行派单可以开始执行", 409)
            if expected_version is not None and assignment["version"] != int(expected_version):
                self._conflict(conn, "派单版本已变化，请刷新后重试", assignment_id=assignment_id,
                               draft={"assignment_id": assignment_id, "expected_version": expected_version})
            incident = self._get(conn, "incidents", assignment["incident_id"], "事件")
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("事件当前不可执行派单", 409)
            now = utcnow()
            conn.execute("UPDATE assignments SET status='active',version=version+1,updated_at=? WHERE id=?", (now, assignment_id))
            conn.execute("UPDATE search_areas SET status='active',version=version+1,updated_at=? WHERE id=?", (now, assignment["area_id"]))
            self._audit(conn, assignment["incident_id"], actor, "assignment.started",
                        {"assignment_id": assignment_id, "area_id": assignment["area_id"], "asset_id": assignment["asset_id"]})
            row = conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone()
            return self._assignment_dict(row)

    def review_assignment(self, actor: str, role: str, assignment_id: int, decision: str,
                          expected_version: int | None = None, note: str = "") -> dict[str, Any]:
        """复核待核派单：confirm 按当前条件重验并刷新依据；invalidate 失效重算并释放占用。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "复核派单")
        if decision not in {"confirm", "invalidate"}:
            raise DomainError("复核结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            assignment = self._get(conn, "assignments", assignment_id, "派单")
            reasons = json.loads(assignment["review_reasons"] or "[]")
            if not reasons:
                raise DomainError("派单没有待复核事项", 409)
            if expected_version is not None and assignment["version"] != int(expected_version):
                self._conflict(conn, "派单版本已变化，请刷新后重试", assignment_id=assignment_id,
                               draft={"assignment_id": assignment_id, "decision": decision, "expected_version": expected_version})
            if assignment["status"] not in OPEN_ASSIGNMENT:
                raise DomainError("当前派单状态不能复核", 409)
            area = self._get(conn, "search_areas", assignment["area_id"], "搜索区域")
            incident = self._get(conn, "incidents", assignment["incident_id"], "事件")
            asset = self._get(conn, "assets", assignment["asset_id"], "资源")
            now = utcnow()
            if decision == "confirm":
                if assignment["status"] != "active":
                    raise DomainError("仅执行中派单可复核确认", 409)
                if asset["status"] == "withdrawn":
                    raise DomainError("资源已撤回，请失效重算或改派", 409)
                distance = self._validate_dispatch(incident, area, asset)
                basis = self._dispatch_basis(incident, area, asset, actor, distance, now)
                basis["reviewed_by"] = actor
                conn.execute("UPDATE assignments SET review_reasons='[]',basis=?,version=version+1,updated_at=? WHERE id=?",
                             (json_dump(basis), now, assignment_id))
                self._audit(conn, assignment["incident_id"], actor, "assignment.reviewed",
                            {"assignment_id": assignment_id, "decision": "confirm",
                             "cleared_reasons": reasons, "note": note.strip()})
            else:
                conn.execute("UPDATE assignments SET status='invalidated',version=version+1,updated_at=? WHERE id=?", (now, assignment_id))
                conn.execute("UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
                             (now, area["id"]))
                conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, asset["id"]))
                self._audit(conn, assignment["incident_id"], actor, "assignment.invalidated",
                            {"assignment_id": assignment_id, "area_id": area["id"], "asset_id": asset["id"],
                             "reason": "复核失效：" + (note.strip() or "依据不再成立"), "pending_reasons": reasons})
            row = conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone()
            return self._assignment_dict(row)

    def _reevaluate_assignments(self, conn: sqlite3.Connection, actor: str, *, reason: str,
                                incident_id: int | None = None, asset_id: int | None = None,
                                area_release_status: str = "planned") -> None:
        """事件结束/资源撤回/海况更新后的统一重算：未执行失效，执行中保留依据待复核。"""
        clauses, params = [], []
        if incident_id is not None:
            clauses.append("incident_id=?")
            params.append(incident_id)
        if asset_id is not None:
            clauses.append("asset_id=?")
            params.append(asset_id)
        if not clauses:
            return
        rows = conn.execute(
            "SELECT * FROM assignments WHERE status IN ('pending','active') AND (%s)" % " OR ".join(clauses), params
        ).fetchall()
        now = utcnow()
        for assignment in rows:
            reasons = json.loads(assignment["review_reasons"] or "[]")
            if reason not in reasons:
                reasons.append(reason)
            if assignment["status"] == "pending":
                conn.execute("UPDATE assignments SET status='invalidated',review_reasons=?,version=version+1,updated_at=? WHERE id=?",
                             (json_dump(reasons), now, assignment["id"]))
                conn.execute("UPDATE search_areas SET assigned_asset_id=NULL,status=?,version=version+1,updated_at=? WHERE id=?",
                             (area_release_status, now, assignment["area_id"]))
                if asset_id is None:
                    conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=? AND status='assigned'",
                                 (now, assignment["asset_id"]))
                self._audit(conn, assignment["incident_id"], actor, "assignment.invalidated",
                            {"assignment_id": assignment["id"], "area_id": assignment["area_id"],
                             "asset_id": assignment["asset_id"], "reason": reason})
            else:
                conn.execute("UPDATE assignments SET review_reasons=?,version=version+1,updated_at=? WHERE id=?",
                             (json_dump(reasons), now, assignment["id"]))
                self._audit(conn, assignment["incident_id"], actor, "assignment.review_flagged",
                            {"assignment_id": assignment["id"], "area_id": assignment["area_id"],
                             "asset_id": assignment["asset_id"], "reason": reason,
                             "basis": json.loads(assignment["basis"] or "{}")})

    # ---------- 线索 ----------

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
            incident = self._get(conn, "incidents", incident_id, "事件")
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
            clue = self._get(conn, "clues", clue_id, "线索")
            if status == "verified" and clue["status"] == "invalid" and role != "coordinator":
                raise DomainError("异常位置线索只能由协调员确认", 403)
            conn.execute("UPDATE clues SET status=?,merged_at=? WHERE id=?", (status, utcnow(), clue_id))
            self._audit(conn, clue["incident_id"], actor, "clue.reviewed", {"clue_id": clue_id, "status": status})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone())

    # ---------- 离线批次 ----------

    def merge_offline_batch(self, actor: str, role: str, client_batch_id: str,
                            events: list[dict[str, Any]]) -> dict[str, Any]:
        """幂等合并离线记录。批次先落库完整载荷再处理；写失败保留完整批次，可按批次号恢复重试。"""
        actor = clean_actor(actor)
        require_role(role, {"operator", "field", "coordinator"}, "合并离线记录")
        batch_id = client_batch_id.strip()
        if not batch_id or not isinstance(events, list):
            raise DomainError("批次编号和事件列表不能为空")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_id,)).fetchone()
            if existing and existing["status"] == "merged":
                return {"batch_id": batch_id, "idempotent": True, "status": "merged",
                        "summary": json.loads(existing["summary"])}
            if existing:
                stored_events = json.loads(existing["payload"] or "[]")
            else:
                conn.execute(
                    "INSERT INTO offline_batches(client_batch_id,actor,status,payload,received_at,summary) VALUES(?,?,?,?,?,?)",
                    (batch_id, actor, "processing", json_dump(events), now, "{}"),
                )
                stored_events = events
        try:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                results = [self._merge_one_event(conn, actor, batch_id, event) for event in stored_events]
                summary = {
                    "accepted": sum(1 for r in results if r["status"] == "merged" and not r.get("idempotent")),
                    "duplicated": sum(1 for r in results if r["status"] == "merged" and r.get("idempotent")),
                    "conflicts": sum(1 for r in results if r["status"] == "conflict"),
                    "rejected": sum(1 for r in results if r["status"] == "rejected"),
                    "events": results,
                }
                conn.execute("UPDATE offline_batches SET status='merged',merged_at=?,summary=? WHERE client_batch_id=?",
                             (utcnow(), json_dump(summary), batch_id))
                self._audit(conn, None, actor, "offline.batch_merged",
                            {"batch_id": batch_id, **{k: summary[k] for k in ("accepted", "duplicated", "conflicts", "rejected")}})
                return {"batch_id": batch_id, "idempotent": False, "status": "merged", "summary": summary}
        except DomainError:
            raise
        except Exception as exc:
            with self.connect() as conn:
                conn.execute("UPDATE offline_batches SET status='failed',summary=? WHERE client_batch_id=?",
                             (json_dump({"error": str(exc)}), batch_id))
            raise DomainError("离线批次写入失败，已保留完整批次供恢复重试", 500) from exc

    def _merge_one_event(self, conn: sqlite3.Connection, actor: str, batch_id: str,
                         event: dict[str, Any]) -> dict[str, Any]:
        """单事件合并：SAVEPOINT 保证失败事件整体回滚，不留资源被占而区域未派单的状态。"""
        event_id = str(event.get("client_event_id", "")).strip() if isinstance(event, dict) else ""
        conn.execute("SAVEPOINT offline_event")
        try:
            if not event_id:
                raise DomainError("离线事件缺少 client_event_id")
            etype = event.get("type")
            if etype == "clue":
                result = self._merge_clue_event(conn, actor, batch_id, event_id, event)
            elif etype == "timeline":
                result = self._merge_timeline_event(conn, actor, event_id, event)
            elif etype == "assignment":
                result = self._merge_assignment_event(conn, actor, batch_id, event_id, event)
            else:
                raise DomainError("不支持的离线事件类型")
            conn.execute("RELEASE offline_event")
        except (DomainError, KeyError, TypeError, ValueError, sqlite3.Error) as exc:
            conn.execute("ROLLBACK TO offline_event")
            conn.execute("RELEASE offline_event")
            result = {"status": "rejected", "error": str(exc)}
        result.setdefault("client_event_id", event_id)
        return result

    def _merge_clue_event(self, conn: sqlite3.Connection, actor: str, batch_id: str,
                          event_id: str, event: dict[str, Any]) -> dict[str, Any]:
        existing = conn.execute("SELECT * FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
        if existing:
            diffs: dict[str, Any] = {}
            for field in CLUE_CONFLICT_FIELDS:
                if field not in event or event[field] is None:
                    continue
                incoming, kept = event[field], existing[field]
                if field in ("latitude", "longitude", "confidence"):
                    if abs(float(incoming) - float(kept)) > 1e-9:
                        diffs[field] = {"kept": kept, "incoming": incoming}
                elif field in ("incident_id", "area_id"):
                    if kept is None or int(incoming) != int(kept):
                        diffs[field] = {"kept": kept, "incoming": incoming}
                elif str(incoming).strip() != str(kept):
                    diffs[field] = {"kept": kept, "incoming": incoming}
            if not diffs:
                return {"status": "merged", "record_id": existing["id"], "idempotent": True}
            conflicts = json.loads(existing["conflicts"] or "[]")
            conflicts.append({"batch_id": batch_id, "actor": actor, "at": utcnow(), "fields": diffs})
            conn.execute("UPDATE clues SET conflicts=? WHERE id=?", (json_dump(conflicts), existing["id"]))
            self._audit(conn, existing["incident_id"], actor, "clue.conflict",
                        {"clue_id": existing["id"], "event_id": event_id, "batch_id": batch_id, "fields": sorted(diffs)})
            return {"status": "conflict", "record_id": existing["id"], "idempotent": True, "fields": diffs}
        incident_id = int(event["incident_id"])
        lat, lon = validate_position(event["latitude"], event["longitude"])
        confidence = float(event["confidence"])
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        incident = self._get(conn, "incidents", incident_id, "事件")
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
             str(event.get("source", "offline")).strip() or "offline", status, distance, actor,
             str(event.get("details", "")).strip(), utcnow()),
        )
        self._audit(conn, incident_id, actor, "clue.recorded",
                    {"clue_id": cur.lastrowid, "status": status, "event_id": event_id, "batch_id": batch_id})
        return {"status": "merged", "record_id": int(cur.lastrowid)}

    def _merge_timeline_event(self, conn: sqlite3.Connection, actor: str,
                              event_id: str, event: dict[str, Any]) -> dict[str, Any]:
        if conn.execute("SELECT 1 FROM timeline WHERE client_event_id=?", (event_id,)).fetchone():
            return {"status": "merged", "record_id": None, "idempotent": True}
        incident_id = int(event["incident_id"])
        if not conn.execute("SELECT 1 FROM incidents WHERE id=?", (incident_id,)).fetchone():
            raise DomainError("事件不存在", 404)
        details = event.get("details")
        self._audit(conn, incident_id, actor, str(event.get("action") or "offline.note"),
                    details if isinstance(details, dict) else {"note": details}, client_event_id=event_id)
        return {"status": "merged", "record_id": None}

    def _merge_assignment_event(self, conn: sqlite3.Connection, actor: str, batch_id: str,
                                event_id: str, event: dict[str, Any]) -> dict[str, Any]:
        area_id, asset_id = int(event["area_id"]), int(event["asset_id"])
        existing = conn.execute("SELECT * FROM assignments WHERE client_event_id=?", (event_id,)).fetchone()
        if existing:
            if existing["area_id"] == area_id and existing["asset_id"] == asset_id:
                return {"status": "merged", "record_id": existing["id"], "idempotent": True}
            diffs: dict[str, Any] = {}
            if existing["area_id"] != area_id:
                diffs["area_id"] = {"kept": existing["area_id"], "incoming": area_id}
            if existing["asset_id"] != asset_id:
                diffs["asset_id"] = {"kept": existing["asset_id"], "incoming": asset_id}
            conflicts = json.loads(existing["conflicts"] or "[]")
            conflicts.append({"batch_id": batch_id, "actor": actor, "at": utcnow(), "fields": diffs})
            conn.execute("UPDATE assignments SET conflicts=? WHERE id=?", (json_dump(conflicts), existing["id"]))
            self._audit(conn, existing["incident_id"], actor, "assignment.conflict",
                        {"assignment_id": existing["id"], "event_id": event_id, "batch_id": batch_id, "fields": sorted(diffs)})
            return {"status": "conflict", "record_id": existing["id"], "idempotent": True, "fields": diffs}
        area = self._get(conn, "search_areas", area_id, "搜索区域")
        asset = self._get(conn, "assets", asset_id, "资源")
        incident = self._get(conn, "incidents", area["incident_id"], "事件")
        if incident["status"] not in ACTIVE_INCIDENT:
            raise DomainError("事件当前不可派单", 409)
        if area["assigned_asset_id"] is not None or area["status"] != "planned":
            raise DomainError("搜索区域已有派单", 409)
        if asset["status"] != "available":
            raise DomainError("资源当前不可用", 409)
        distance = self._validate_dispatch(incident, area, asset)
        assignment_id = self._insert_assignment(conn, incident, area, asset, actor, distance, utcnow(),
                                                client_event_id=event_id, origin="offline")
        return {"status": "merged", "record_id": assignment_id}

    # ---------- 查询 ----------

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            assignments = [self._assignment_dict(r) for r in conn.execute("SELECT * FROM assignments ORDER BY id DESC").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            batches = [dict(r) for r in conn.execute(
                "SELECT id,client_batch_id,actor,status,received_at,merged_at FROM offline_batches ORDER BY id DESC LIMIT 50").fetchall()]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 300").fetchall()]
        return {"incidents": incidents, "assets": assets, "search_areas": areas, "assignments": assignments,
                "clues": clues, "offline_batches": batches, "timeline": timeline}

    def incident_timeline(self, incident_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM timeline WHERE incident_id=? ORDER BY id", (incident_id,)).fetchall()
        return [dict(r) for r in rows]

    def seed_demo(self) -> dict[str, Any]:
        with self.connect() as conn:
            if conn.execute("SELECT COUNT(*) AS c FROM incidents").fetchone()["c"]:
                return {"seeded": False, "reason": "已有数据"}
        incident = self.create_incident("coord-demo", "coordinator", "SAR-2026-001", "远星号", 31.2, 122.5, 15.0, 3, "东海搜救中心", description="演示遇险事件")
        vessel = self.add_asset("coord-demo", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 22.0, 180.0, 6)
        self.add_asset("coord-demo", "coordinator", "救助直升机", "aircraft", ["air", "night"], 30.8, 122.1, 180.0, 260.0, 5)
        area = self.create_search_area("coord-demo", "coordinator", incident["id"], "AREA-A", "surface", 31.2, 122.5, 20.0, 1, "首要搜索区")
        self.create_assignment("coord-demo", "coordinator", area["id"], vessel["id"])
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
                result = self.service.create_assignment(actor, role, **data)
            elif path == "/api/assignments/reassign":
                result = self.service.reassign_assignment(actor, role, **data)
            elif path == "/api/assignments/start":
                result = self.service.start_assignment(actor, role, **data)
            elif path == "/api/assignments/review":
                result = self.service.review_assignment(actor, role, **data)
            elif path == "/api/clues":
                result = self.service.record_clue(actor, role, **data)
            elif path == "/api/clues/verify":
                result = self.service.verify_clue(actor, role, **data)
            elif path == "/api/assets/withdraw":
                result = self.service.withdraw_asset(actor, role, **data)
            elif path == "/api/areas/complete":
                result = self.service.complete_area(actor, role, **data)
            elif path == "/api/incidents/sea-state":
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
            self._send(exc.status, {"error": str(exc), **exc.payload})
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
