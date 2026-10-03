import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import DomainError, MaritimeSARService  # noqa: E402


class MaritimeSARFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-001", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 20, 100, 5
        )
        self.asset2 = self.service.add_asset(
            "coord1", "coordinator", "海巡02", "vessel", ["surface"], 31.0, 122.0, 20, 100, 5
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _area(self, code="A-01", lat=31.1, lon=122.1):
        return self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], code, "surface", lat, lon, 8, 1
        )

    def _assignment(self, area, asset=None):
        asset = asset or self.asset
        return self.service.create_assignment(
            "coord1", "coordinator", area["id"], asset["id"],
            expected_incident_version=self.incident["version"],
            expected_area_version=area["version"],
            expected_asset_version=asset["version"],
        )

    def _state_assignment(self, assignment_id):
        return [a for a in self.service.state()["assignments"] if a["id"] == assignment_id][0]

    def _state_asset(self, asset_id):
        return [a for a in self.service.state()["assets"] if a["id"] == asset_id][0]

    def _state_area(self, area_id):
        return [a for a in self.service.state()["search_areas"] if a["id"] == area_id][0]

    # ---------- 完整派救流程 ----------

    def test_full_dispatch_lifecycle_and_close(self):
        area = self._area()
        asg = self._assignment(area)
        self.assertEqual("pending", asg["status"])
        self.assertEqual(self.incident["version"], asg["basis"]["incident_version"])
        self.assertEqual(3, asg["basis"]["sea_state"])
        self.assertEqual("assigned", self._state_asset(self.asset["id"])["status"])

        started = self.service.start_assignment("op1", "operator", asg["id"], asg["version"])
        self.assertEqual("active", started["status"])
        self.assertEqual("active", self._state_area(area["id"])["status"])

        clue = self.service.record_clue(
            "field1", "field", self.incident["id"], "evt-1", 31.1, 122.1, 0.9, "visual", area["id"]
        )
        self.assertEqual("verified", self.service.verify_clue("analyst1", "analyst", clue["id"], "verified")["status"])

        batch = self.service.merge_offline_batch(
            "field1", "field", "batch-1",
            [{"type": "clue", "client_event_id": "off-1", "incident_id": self.incident["id"],
              "latitude": 31.11, "longitude": 122.11, "confidence": 0.7, "source": "radio"}],
        )
        self.assertEqual(1, batch["summary"]["accepted"])
        self.assertTrue(self.service.merge_offline_batch("field1", "field", "batch-1", [])["idempotent"])

        current_area = self._state_area(area["id"])
        self.service.complete_area("coord1", "coordinator", area["id"], "completed", current_area["version"])
        self.assertEqual("completed", self._state_assignment(asg["id"])["status"])
        self.assertEqual("available", self._state_asset(self.asset["id"])["status"])

        current_incident = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        closed = self.service.close_incident("coord1", "coordinator", self.incident["id"], "resolved", current_incident["version"])
        self.assertEqual("closed", closed["status"])
        actions = [t["action"] for t in self.service.incident_timeline(self.incident["id"])]
        for expected in ("assignment.created", "assignment.started", "assignment.completed", "incident.closed"):
            self.assertIn(expected, actions)

    # ---------- 改派：冻结版本 + 有效占用 ----------

    def test_reassign_freezes_versions_and_validates_occupancy(self):
        area = self._area()
        asg = self._assignment(area)

        with self.assertRaises(DomainError) as ctx:
            self.service.reassign_assignment("coord1", "coordinator", asg["id"], self.asset2["id"],
                                             expected_incident_version=999,
                                             expected_area_version=area["version"])
        self.assertEqual(409, ctx.exception.status)
        conflict = ctx.exception.payload["conflict"]
        self.assertEqual(self.incident["version"], conflict["current"]["incident"]["version"])
        self.assertEqual(self.asset2["id"], conflict["draft"]["new_asset_id"])

        with self.assertRaises(DomainError):
            self.service.reassign_assignment("coord1", "coordinator", asg["id"], self.asset2["id"])

        area_now = self._state_area(area["id"])
        incident_now = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        far = self.service.add_asset("coord1", "coordinator", "远援船", "vessel", ["surface"], 40.0, 130.0, 20, 50, 5)
        with self.assertRaises(DomainError) as ctx2:
            self.service.reassign_assignment("coord1", "coordinator", asg["id"], far["id"],
                                             expected_incident_version=incident_now["version"],
                                             expected_area_version=area_now["version"])
        self.assertIn("航程", str(ctx2.exception))

        heli = self.service.add_asset("coord1", "coordinator", "直升机", "aircraft", ["air"], 31.0, 122.0, 150, 300, 5)
        with self.assertRaises(DomainError) as ctx3:
            self.service.reassign_assignment("coord1", "coordinator", asg["id"], heli["id"],
                                             expected_incident_version=incident_now["version"],
                                             expected_area_version=area_now["version"])
        self.assertIn("能力", str(ctx3.exception))

        new_asg = self.service.reassign_assignment(
            "coord1", "coordinator", asg["id"], self.asset2["id"],
            expected_incident_version=incident_now["version"], expected_area_version=area_now["version"],
        )
        self.assertEqual("pending", new_asg["status"])
        self.assertEqual(self.asset2["id"], new_asg["asset_id"])
        self.assertEqual("superseded", self._state_assignment(asg["id"])["status"])
        self.assertEqual("available", self._state_asset(self.asset["id"])["status"])
        self.assertEqual("assigned", self._state_asset(self.asset2["id"])["status"])
        self.assertEqual(self.asset2["id"], self._state_area(area["id"])["assigned_asset_id"])

    # ---------- 并发：后到者保留草稿并看到版本变化 ----------

    def test_concurrent_assignment_conflict_returns_current_and_draft(self):
        area = self._area()
        self._assignment(area)
        with self.assertRaises(DomainError) as ctx:
            self.service.create_assignment(
                "coord2", "coordinator", area["id"], self.asset2["id"],
                expected_incident_version=self.incident["version"],
                expected_area_version=area["version"],
                expected_asset_version=self.asset2["version"],
            )
        self.assertEqual(409, ctx.exception.status)
        conflict = ctx.exception.payload["conflict"]
        self.assertEqual("assigned", conflict["current"]["area"]["status"])
        self.assertEqual(self.asset["id"], conflict["current"]["area"]["assigned_asset_id"])
        self.assertEqual(area["id"], conflict["draft"]["area_id"])
        self.assertEqual(self.asset2["id"], conflict["draft"]["asset_id"])

        area2 = self._area("A-02", 31.2, 122.2)
        with self.assertRaises(DomainError) as ctx2:
            self.service.create_assignment(
                "coord2", "coordinator", area2["id"], self.asset2["id"],
                expected_incident_version=self.incident["version"],
                expected_area_version=area2["version"],
                expected_asset_version=999,
            )
        self.assertEqual(self.asset2["version"], ctx2.exception.payload["conflict"]["current"]["asset"]["version"])

    # ---------- 触发器：未执行失效重算，执行中保留依据待复核 ----------

    def test_sea_state_update_invalidates_pending_and_flags_active(self):
        area1, area2 = self._area("A-01"), self._area("A-02", 31.2, 122.2)
        asg_pending = self._assignment(area1, self.asset)
        asg_active = self._assignment(area2, self.asset2)
        self.service.start_assignment("coord1", "coordinator", asg_active["id"], asg_active["version"])

        incident_now = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        self.service.update_sea_state("coord1", "coordinator", self.incident["id"], 5, incident_now["version"])

        pending = self._state_assignment(asg_pending["id"])
        self.assertEqual("invalidated", pending["status"])
        self.assertIn("海况更新", pending["review_reasons"][0])
        self.assertEqual("planned", self._state_area(area1["id"])["status"])
        self.assertIsNone(self._state_area(area1["id"])["assigned_asset_id"])
        self.assertEqual("available", self._state_asset(self.asset["id"])["status"])

        active = self._state_assignment(asg_active["id"])
        self.assertEqual("active", active["status"])
        self.assertEqual(3, active["basis"]["sea_state"])
        self.assertTrue(any("海况更新" in r for r in active["review_reasons"]))
        self.assertEqual("assigned", self._state_asset(self.asset2["id"])["status"])

        confirmed = self.service.review_assignment("coord1", "coordinator", asg_active["id"], "confirm")
        self.assertEqual([], confirmed["review_reasons"])
        self.assertEqual(5, confirmed["basis"]["sea_state"])

        incident_now = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        self.service.update_sea_state("coord1", "coordinator", self.incident["id"], 7, incident_now["version"])
        flagged = self._state_assignment(asg_active["id"])
        self.assertTrue(flagged["review_reasons"])
        with self.assertRaises(DomainError) as ctx:
            self.service.review_assignment("coord1", "coordinator", asg_active["id"], "confirm")
        self.assertIn("海况", str(ctx.exception))
        reviewed = self.service.review_assignment("coord1", "coordinator", asg_active["id"], "invalidate", note="海况超限")
        self.assertEqual("invalidated", reviewed["status"])
        self.assertEqual("planned", self._state_area(area2["id"])["status"])
        self.assertEqual("available", self._state_asset(self.asset2["id"])["status"])

    def test_withdraw_invalidates_pending_and_flags_active(self):
        area1, area2 = self._area("A-01"), self._area("A-02", 31.2, 122.2)
        asg_pending = self._assignment(area1, self.asset)
        asg_active = self._assignment(area2, self.asset2)
        self.service.start_assignment("coord1", "coordinator", asg_active["id"], asg_active["version"])

        self.service.withdraw_asset("coord1", "coordinator", self.asset["id"], "机械故障", self._state_asset(self.asset["id"])["version"])
        self.assertEqual("invalidated", self._state_assignment(asg_pending["id"])["status"])
        self.assertEqual("planned", self._state_area(area1["id"])["status"])
        self.assertEqual("available", self._state_asset(self.asset["id"])["status"])

        self.service.withdraw_asset("coord1", "coordinator", self.asset2["id"], "奉命撤离", self._state_asset(self.asset2["id"])["version"])
        active = self._state_assignment(asg_active["id"])
        self.assertEqual("active", active["status"])
        self.assertTrue(any("奉命撤离" in r for r in active["review_reasons"]))
        self.assertEqual("withdrawn", self._state_asset(self.asset2["id"])["status"])
        self.assertEqual("active", self._state_area(area2["id"])["status"])

        with self.assertRaises(DomainError):
            self.service.review_assignment("coord1", "coordinator", asg_active["id"], "confirm")
        self.service.review_assignment("coord1", "coordinator", asg_active["id"], "invalidate", note="资源已撤")
        self.assertEqual("available", self._state_asset(self.asset2["id"])["status"])
        self.assertEqual("planned", self._state_area(area2["id"])["status"])

        actions = [t["action"] for t in self.service.incident_timeline(self.incident["id"])]
        self.assertIn("assignment.review_flagged", actions)
        self.assertIn("assignment.invalidated", actions)

    def test_close_false_alarm_invalidates_pending(self):
        area = self._area()
        asg = self._assignment(area)
        incident_now = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        closed = self.service.close_incident("coord1", "coordinator", self.incident["id"], "false_alarm", incident_now["version"])
        self.assertEqual("cancelled", closed["status"])
        self.assertEqual("invalidated", self._state_assignment(asg["id"])["status"])
        self.assertEqual("abandoned", self._state_area(area["id"])["status"])
        self.assertEqual("available", self._state_asset(self.asset["id"])["status"])

    # ---------- 离线批次：幂等、冲突并列、原子占用、失败恢复 ----------

    def test_offline_conflicts_kept_side_by_side(self):
        self.service.merge_offline_batch(
            "field1", "field", "b-1",
            [{"type": "clue", "client_event_id": "c-1", "incident_id": self.incident["id"],
              "latitude": 31.1, "longitude": 122.1, "confidence": 0.5, "source": "radio"}],
        )
        result = self.service.merge_offline_batch(
            "field2", "field", "b-2",
            [{"type": "clue", "client_event_id": "c-1", "incident_id": self.incident["id"],
              "latitude": 31.1, "longitude": 122.1, "confidence": 0.9, "source": "radar"}],
        )
        self.assertEqual(1, result["summary"]["conflicts"])
        clue = [c for c in self.service.state()["clues"] if c["client_event_id"] == "c-1"][0]
        self.assertEqual(0.5, clue["confidence"])
        self.assertEqual("radio", clue["source"])
        conflicts = json.loads(clue["conflicts"])
        self.assertEqual({"kept": 0.5, "incoming": 0.9}, conflicts[0]["fields"]["confidence"])
        self.assertEqual({"kept": "radio", "incoming": "radar"}, conflicts[0]["fields"]["source"])

    def test_offline_assignment_atomic_and_idempotent(self):
        area1, area2 = self._area("A-01"), self._area("A-02", 31.2, 122.2)
        batch = self.service.merge_offline_batch(
            "field1", "field", "b-asg",
            [{"type": "assignment", "client_event_id": "asg-1", "area_id": area1["id"], "asset_id": self.asset["id"]},
             {"type": "assignment", "client_event_id": "asg-2", "area_id": area2["id"], "asset_id": self.asset["id"]}],
        )
        events = {e["client_event_id"]: e for e in batch["summary"]["events"]}
        self.assertEqual("merged", events["asg-1"]["status"])
        self.assertEqual("rejected", events["asg-2"]["status"])
        self.assertEqual("assigned", self._state_area(area1["id"])["status"])
        self.assertEqual("planned", self._state_area(area2["id"])["status"])
        self.assertIsNone(self._state_area(area2["id"])["assigned_asset_id"])
        self.assertEqual(1, len([a for a in self.service.state()["assignments"] if a["status"] == "pending"]))

        replay = self.service.merge_offline_batch(
            "field1", "field", "b-asg-2",
            [{"type": "assignment", "client_event_id": "asg-1", "area_id": area1["id"], "asset_id": self.asset["id"]}],
        )
        self.assertTrue(replay["summary"]["events"][0]["idempotent"])
        conflict = self.service.merge_offline_batch(
            "field1", "field", "b-asg-3",
            [{"type": "assignment", "client_event_id": "asg-1", "area_id": area1["id"], "asset_id": self.asset2["id"]}],
        )
        self.assertEqual("conflict", conflict["summary"]["events"][0]["status"])
        self.assertEqual("available", self._state_asset(self.asset2["id"])["status"])

    def test_offline_batch_recovers_from_stored_payload_after_failure(self):
        events = [
            {"type": "clue", "client_event_id": "r-1", "incident_id": self.incident["id"],
             "latitude": 31.1, "longitude": 122.1, "confidence": 0.6, "source": "radio"},
            {"type": "timeline", "client_event_id": "r-2", "incident_id": self.incident["id"],
             "action": "offline.note", "details": {"note": "现场风浪大"}},
        ]
        original = self.service._merge_one_event
        self.service._merge_one_event = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("模拟写入失败"))
        with self.assertRaises(DomainError) as ctx:
            self.service.merge_offline_batch("field1", "field", "b-fail", events)
        self.assertEqual(500, ctx.exception.status)
        self.service._merge_one_event = original

        self.assertEqual([], self.service.state()["clues"])
        batch_row = [b for b in self.service.state()["offline_batches"] if b["client_batch_id"] == "b-fail"][0]
        self.assertEqual("failed", batch_row["status"])

        recovered = self.service.merge_offline_batch("field1", "field", "b-fail", [])
        self.assertFalse(recovered["idempotent"])
        self.assertEqual(2, recovered["summary"]["accepted"])
        self.assertEqual(1, len(self.service.state()["clues"]))
        notes = [t for t in self.service.incident_timeline(self.incident["id"]) if t["action"] == "offline.note"]
        self.assertEqual(1, len(notes))

        again = self.service.merge_offline_batch("field1", "field", "b-fail", [])
        self.assertTrue(again["idempotent"])
        self.assertEqual(1, len(self.service.state()["clues"]))

    # ---------- 既有控制 ----------

    def test_duplicate_alarm_and_invalid_position_are_controlled(self):
        duplicate = self.service.create_incident(
            "op1", "operator", "SAR-002", "海燕号", 31.01, 122.01, 5.0, 3, "东海中心"
        )
        self.assertEqual("duplicate", duplicate["status"])
        self.assertEqual(self.incident["id"], duplicate["duplicate_of"])
        invalid = self.service.record_clue(
            "field1", "field", self.incident["id"], "evt-far", 45.0, 130.0, 0.8, "radio"
        )
        self.assertEqual("invalid", invalid["status"])
        with self.assertRaises(DomainError):
            self.service.verify_clue("field1", "field", invalid["id"], "verified")

    def test_permissions(self):
        area = self._area()
        with self.assertRaises(DomainError) as ctx:
            self.service.create_assignment("field1", "field", area["id"], self.asset["id"])
        self.assertEqual(403, ctx.exception.status)
        with self.assertRaises(DomainError) as ctx2:
            self.service.create_search_area("field1", "field", self.incident["id"], "A-09", "surface", 31, 122, 5)
        self.assertEqual(403, ctx2.exception.status)
        with self.assertRaises(DomainError) as ctx3:
            self.service.update_sea_state("op1", "operator", self.incident["id"], 4, 1)
        self.assertEqual(403, ctx3.exception.status)


if __name__ == "__main__":
    unittest.main()
