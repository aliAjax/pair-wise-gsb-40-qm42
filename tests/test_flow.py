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

    def tearDown(self):
        self.tmp.cleanup()

    def test_complete_assignment_clue_offline_and_close_flow(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-01", "surface", 31.1, 122.1, 8, 1
        )
        assigned = self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"])
        self.assertEqual("assigned", assigned["status"])
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
        updated_asset = self.service.list_assets()[0]
        self.service.withdraw_asset("coord1", "coordinator", self.asset["id"], "任务移交", updated_asset["version"])
        current_area = self.service.state()["search_areas"][0]
        self.service.complete_area("coord1", "coordinator", area["id"], "abandoned", current_area["version"])
        current_incident = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        closed = self.service.close_incident("coord1", "coordinator", self.incident["id"], "resolved", current_incident["version"])
        self.assertEqual("closed", closed["status"])
        self.assertGreaterEqual(len(self.service.incident_timeline(self.incident["id"])), 6)

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

    def test_assignment_conflict_and_permission(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-02", "surface", 31.1, 122.1, 5
        )
        self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"])
        area2 = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-03", "surface", 31.2, 122.2, 5
        )
        with self.assertRaises(DomainError) as ctx:
            self.service.assign_area("coord1", "coordinator", area2["id"], self.asset["id"], self.asset["version"])
        self.assertEqual(409, ctx.exception.status)
        with self.assertRaises(DomainError) as ctx2:
            self.service.create_search_area("field1", "field", self.incident["id"], "A-04", "surface", 31, 122, 5)
        self.assertEqual(403, ctx2.exception.status)


class ReassignmentFlowTest(unittest.TestCase):
    """改派冻结版本、失效重算与离线恢复。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-100", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset1 = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface"], 31.0, 122.0, 20, 200, 5
        )
        self.asset2 = self.service.add_asset(
            "coord1", "coordinator", "海巡02", "vessel", ["surface"], 31.05, 122.05, 20, 200, 5
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _area(self, code="A-1"):
        return self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], code, "surface", 31.1, 122.1, 5, 1
        )

    def _state(self):
        return self.service.state()

    def _asset_status(self, asset_id):
        return [a for a in self._state()["assets"] if a["id"] == asset_id][0]["status"]

    def _orders(self, status=None):
        orders = self._state()["assignments"]
        return [o for o in orders if status is None or o["status"] == status]

    def test_reassign_freezes_versions_and_keeps_draft_for_late_submitter(self):
        area = self._area()
        self.service.assign_area("coord1", "coordinator", area["id"], self.asset1["id"])
        # 后到者用过期的区域版本提交：保留草稿并看到当前版本
        with self.assertRaises(DomainError) as ctx:
            self.service.reassign_area(
                "coord2", "coordinator", area["id"], self.asset2["id"],
                expected_incident_version=1, expected_area_version=999,
            )
        self.assertEqual(409, ctx.exception.status)
        self.assertEqual(2, ctx.exception.extra["current_versions"]["area_version"])
        draft_id = ctx.exception.extra["draft_id"]
        draft = self._state()["assignment_drafts"][0]
        self.assertEqual("open", draft["status"])
        self.assertEqual("reassign", draft["action"])
        # 版本一致后改派成功：旧单作废、旧船释放、新船占用，全部原子完成
        result = self.service.reassign_area(
            "coord1", "coordinator", area["id"], self.asset2["id"],
            expected_incident_version=1, expected_area_version=2, note="海巡01机械故障",
        )
        self.assertEqual("assigned", result["status"])
        self.assertEqual(self.asset2["id"], result["assigned_asset_id"])
        self.assertEqual("available", self._asset_status(self.asset1["id"]))
        self.assertEqual("assigned", self._asset_status(self.asset2["id"]))
        self.assertEqual(1, len(self._orders("superseded")))
        self.assertEqual(1, len(self._orders("issued")))
        # 草稿可放弃；时间线记录了同一派单编号
        self.assertEqual("discarded", self.service.discard_assignment_draft("coord1", "coordinator", draft_id)["status"])
        timeline = self.service.incident_timeline(self.incident["id"])
        reassigned = [t for t in timeline if t["action"] == "assignment.reassigned"][0]
        self.assertIn(result["assignment"]["code"], reassigned["details"])

    def test_draft_can_be_submitted_after_refresh(self):
        area = self._area()
        with self.assertRaises(DomainError) as ctx:
            self.service.assign_area(
                "coord1", "coordinator", area["id"], self.asset1["id"], expected_area_version=999
            )
        draft_id = ctx.exception.extra["draft_id"]
        result = self.service.submit_assignment_draft("coord1", "coordinator", draft_id)
        self.assertEqual("assigned", result["status"])
        self.assertEqual(draft_id, result["draft_id"])
        self.assertEqual("assigned", self._asset_status(self.asset1["id"]))

    def test_sea_state_update_invalidates_unexecuted_and_flags_executing(self):
        area1, area2 = self._area("A-1"), self._area("A-2")
        self.service.assign_area("coord1", "coordinator", area1["id"], self.asset1["id"])
        self.service.assign_area("coord1", "coordinator", area2["id"], self.asset2["id"])
        executing = self._orders("issued")[0]
        self.service.start_assignment("coord1", "coordinator", executing["id"])
        incident = [i for i in self._state()["incidents"] if i["id"] == self.incident["id"]][0]
        self.service.update_sea_state("coord1", "coordinator", self.incident["id"], 7, incident["version"])
        # 未执行派单失效：资源释放、区域回到待派
        invalidated = self._orders("invalidated")
        self.assertEqual(1, len(invalidated))
        self.assertIn("海况", invalidated[0]["invalid_reason"])
        # 执行中派单保留原依据待复核
        flagged = [o for o in self._orders("executing") if o["review_pending"]]
        self.assertEqual(1, len(flagged))
        self.assertIn("海况", flagged[0]["review_reasons"])
        self.assertEqual(3, __import__("json").loads(flagged[0]["basis"])["sea_state"])
        invalidated_area = [a for a in self._state()["search_areas"] if a["id"] == invalidated[0]["area_id"]][0]
        self.assertEqual("planned", invalidated_area["status"])
        self.assertIsNone(invalidated_area["assigned_asset_id"])
        self.assertEqual("available", self._asset_status(invalidated[0]["asset_id"]))
        # 复核作废执行中的派单
        self.service.review_assignment("coord1", "coordinator", flagged[0]["id"], "invalidate", "海况超限")
        self.assertEqual("available", self._asset_status(flagged[0]["asset_id"]))
        # 时间线包含同一派单与待核原因
        timeline = self.service.incident_timeline(self.incident["id"])
        review_entries = [t for t in timeline if t["action"] == "assignment.review_pending"]
        self.assertTrue(review_entries)
        self.assertIn(flagged[0]["code"], review_entries[0]["details"])
        self.assertIn("海况", review_entries[0]["details"])

    def test_withdraw_releases_unexecuted_and_keeps_executing_for_review(self):
        area1, area2 = self._area("A-1"), self._area("A-2")
        self.service.assign_area("coord1", "coordinator", area1["id"], self.asset1["id"])
        self.service.assign_area("coord1", "coordinator", area2["id"], self.asset2["id"])
        executing = [o for o in self._orders("issued") if o["asset_id"] == self.asset2["id"]][0]
        self.service.start_assignment("coord1", "coordinator", executing["id"])
        asset1 = [a for a in self._state()["assets"] if a["id"] == self.asset1["id"]][0]
        self.service.withdraw_asset("coord1", "coordinator", self.asset1["id"], "机械故障", asset1["version"])
        # 未执行派单失效，区域不再挂着已撤回的船
        area1_now = [a for a in self._state()["search_areas"] if a["id"] == area1["id"]][0]
        self.assertEqual("planned", area1_now["status"])
        self.assertIsNone(area1_now["assigned_asset_id"])
        self.assertEqual("available", self._asset_status(self.asset1["id"]))
        # 执行中的保留原依据待复核
        asset2 = [a for a in self._state()["assets"] if a["id"] == self.asset2["id"]][0]
        self.service.withdraw_asset("coord1", "coordinator", self.asset2["id"], "召回休整", asset2["version"])
        flagged = [o for o in self._orders("executing") if o["review_pending"]]
        self.assertEqual(1, len(flagged))
        self.assertIn("撤回", flagged[0]["review_reasons"])
        confirmed = self.service.review_assignment("coord1", "coordinator", flagged[0]["id"], "confirm")
        self.assertEqual(0, confirmed["review_pending"])

    def test_close_false_alarm_invalidates_unexecuted_orders(self):
        area = self._area()
        self.service.assign_area("coord1", "coordinator", area["id"], self.asset1["id"])
        incident = [i for i in self._state()["incidents"] if i["id"] == self.incident["id"]][0]
        closed = self.service.close_incident("coord1", "coordinator", self.incident["id"], "false_alarm", incident["version"])
        self.assertEqual("cancelled", closed["status"])
        self.assertEqual(1, len(self._orders("invalidated")))
        self.assertEqual("available", self._asset_status(self.asset1["id"]))
        area_now = [a for a in self._state()["search_areas"] if a["id"] == area["id"]][0]
        self.assertEqual("abandoned", area_now["status"])

    def test_offline_conflicts_kept_side_by_side_and_batch_recovers(self):
        area = self._area()
        batch1 = self.service.merge_offline_batch(
            "field1", "field", "b-1",
            [{"type": "clue", "client_event_id": "evt-a", "incident_id": self.incident["id"],
              "latitude": 31.1, "longitude": 122.1, "confidence": 0.5, "source": "radio"}],
        )
        self.assertEqual(1, batch1["summary"]["accepted"])
        # 同一客户端事件编号、字段不同：冲突并列保留，不覆盖已存记录
        batch2 = self.service.merge_offline_batch(
            "field1", "field", "b-2",
            [{"type": "clue", "client_event_id": "evt-a", "incident_id": self.incident["id"],
              "latitude": 31.1, "longitude": 122.1, "confidence": 0.9, "source": "radio"}],
        )
        self.assertEqual(1, batch2["summary"]["conflicts"])
        conflicts = self._state()["clue_conflicts"]
        self.assertEqual(1, len(conflicts))
        self.assertEqual("confidence", conflicts[0]["field"])
        self.assertEqual("0.5", conflicts[0]["existing_value"])
        self.assertEqual("0.9", conflicts[0]["incoming_value"])
        clue = [c for c in self._state()["clues"] if c["client_event_id"] == "evt-a"][0]
        self.assertEqual(0.5, clue["confidence"])
        # 批次重放幂等
        self.assertTrue(self.service.merge_offline_batch("field1", "field", "b-1", [])["idempotent"])
        # 离线派单事件：旧批次重放不会再次占船
        batch3 = self.service.merge_offline_batch(
            "op1", "operator", "b-3",
            [{"type": "assignment", "client_event_id": "evt-asg-1", "area_id": area["id"], "asset_id": self.asset1["id"]}],
        )
        self.assertEqual(1, batch3["summary"]["accepted"])
        replay = self.service.merge_offline_batch(
            "op1", "operator", "b-4",
            [{"type": "assignment", "client_event_id": "evt-asg-1", "area_id": area["id"], "asset_id": self.asset1["id"]}],
        )
        self.assertTrue(replay["summary"]["events"][0]["idempotent"])
        self.assertEqual(1, len(self._orders()))
        # 写入失败的批次事件不消耗幂等编号，修正后可从完整批次恢复
        bad = self.service.merge_offline_batch(
            "op1", "operator", "b-5",
            [{"type": "assignment", "client_event_id": "evt-asg-2", "area_id": area["id"]}],
        )
        self.assertEqual(1, bad["summary"]["rejected"])
        area2 = self._area("A-2")
        fixed = self.service.merge_offline_batch(
            "op1", "operator", "b-6",
            [{"type": "assignment", "client_event_id": "evt-asg-2", "area_id": area2["id"], "asset_id": self.asset2["id"]}],
        )
        self.assertEqual(1, fixed["summary"]["accepted"])
        self.assertEqual(2, len(self._orders()))


if __name__ == "__main__":
    unittest.main()
