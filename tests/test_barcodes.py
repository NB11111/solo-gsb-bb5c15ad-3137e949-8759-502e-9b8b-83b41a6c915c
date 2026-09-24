"""样品条码整批换绑模块端到端测试:

- 单事务首绑、解绑(null)、两样品互换、三样品循环、批内解绑后同批再占用
- 逐项核对样品存在、旧绑定与预期一致、目标条码未被批外样品占用
- 任一冲突整批回滚、冲突一次列全,失败批次不留任何审计痕迹
- 同批样品号重复 / 非空目标条码重复 -> 422;多项解绑(null 目标)允许
- 按批次号同内容重放返回首次结果且不重复审计,异内容 409
- 成功写不可变换绑审计(头表/明细触发器禁止 UPDATE/DELETE)
- 按条码查询当前绑定样品;未绑定条码 bound=false
- 换绑不动余量与账本;并发由 BEGIN IMMEDIATE 串行化;旧库启动自动建表
"""
from __future__ import annotations

import threading

import pytest
import sqlite3
from fastapi.testclient import TestClient

from app import barcodes as bc
from app.db import connect, init_db
from app.main import app, get_conn


@pytest.fixture()
def client():
    mem = connect(":memory:")
    init_db(mem)

    def _override():
        yield mem

    app.dependency_overrides[get_conn] = _override
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
    mem.close()


def _sample(client, op_no, sid, amount=100, unit="mL"):
    r = client.post("/api/v1/operations/sampling", json={
        "type": "sampling", "op_no": op_no, "sample_id": sid,
        "material": "血清", "amount": amount, "unit": unit,
    })
    assert r.status_code == 200, r.text


def _reassign(client, no, items, **extra):
    return client.post("/api/v1/barcodes/reassignments", json={
        "reassignment_no": no, "items": items, **extra,
    })


def _bound(client, barcode):
    return client.get(f"/api/v1/barcodes/{barcode}").json()


def _all_bindings(conn):
    return {
        r["sample_id"]: r["barcode"]
        for r in conn.execute(
            "SELECT sample_id, barcode FROM sample_barcodes"
        ).fetchall()
    }


# ---------------------------------------------------------------- 成功路径

def test_initial_bind_from_unbound_with_null_expected(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-2", "S2")
    r = _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": "B2"},
    ], operator="甲", note="首绑")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["reassignment_no"] == "BR-1"
    assert body["operator"] == "甲"
    assert body["note"] == "首绑"
    assert body["replayed"] is False
    assert body["reassigned_at"] == body["created_at"]
    assert body["items"] == [
        {"sample_id": "S1", "expected_old_barcode": None,
         "target_barcode": "B1", "from_barcode": None},
        {"sample_id": "S2", "expected_old_barcode": None,
         "target_barcode": "B2", "from_barcode": None},
    ]
    assert _bound(client, "B1")["sample_id"] == "S1"
    assert _bound(client, "B2")["sample_id"] == "S2"
    assert _bound(client, "B1")["reassignment_no"] == "BR-1"


def test_swap_two_samples_in_one_batch(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-2", "S2")
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": "B2"},
    ])
    r = _reassign(client, "BR-2", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B2"},
        {"sample_id": "S2", "expected_old_barcode": "B2", "target_barcode": "B1"},
    ])
    assert r.status_code == 200, r.text
    assert [i["from_barcode"] for i in r.json()["items"]] == ["B1", "B2"]
    assert _bound(client, "B1")["sample_id"] == "S2"
    assert _bound(client, "B2")["sample_id"] == "S1"


def test_three_sample_cycle(client):
    for sid in ("S1", "S2", "S3"):
        _sample(client, f"OP-{sid}", sid)
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": "B2"},
        {"sample_id": "S3", "expected_old_barcode": None, "target_barcode": "B3"},
    ])
    # S1->B2, S2->B3, S3->B1(循环)
    r = _reassign(client, "BR-2", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B2"},
        {"sample_id": "S2", "expected_old_barcode": "B2", "target_barcode": "B3"},
        {"sample_id": "S3", "expected_old_barcode": "B3", "target_barcode": "B1"},
    ])
    assert r.status_code == 200, r.text
    assert _bound(client, "B1")["sample_id"] == "S3"
    assert _bound(client, "B2")["sample_id"] == "S1"
    assert _bound(client, "B3")["sample_id"] == "S2"


def test_unbind_sets_sample_and_barcode_free(client):
    _sample(client, "OP-1", "S1")
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
    ])
    r = _reassign(client, "BR-2", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": None},
    ])
    assert r.status_code == 200, r.text
    item = r.json()["items"][0]
    assert item["target_barcode"] is None and item["from_barcode"] == "B1"
    b = _bound(client, "B1")
    assert b == {"barcode": "B1", "bound": False,
                "sample_id": None, "reassignment_no": None}


def test_unbind_and_reuse_freed_barcode_within_same_batch(client):
    """批内 S1 解绑 B3 与 S2 占用 B3 同时提交:先统一解绑再写入,不触发唯一冲突。"""
    for sid in ("S1", "S2"):
        _sample(client, f"OP-{sid}", sid)
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B3"},
    ])
    r = _reassign(client, "BR-2", [
        {"sample_id": "S1", "expected_old_barcode": "B3", "target_barcode": None},
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": "B3"},
    ])
    assert r.status_code == 200, r.text
    assert _bound(client, "B3")["sample_id"] == "S2"
    assert _bound(client, "B3")["reassignment_no"] == "BR-2"


def test_rebind_to_same_barcode_is_a_noop_move(client):
    _sample(client, "OP-1", "S1")
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
    ])
    r = _reassign(client, "BR-2", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B1"},
    ])
    assert r.status_code == 200, r.text
    assert _bound(client, "B1")["sample_id"] == "S1"


def test_reassignment_does_not_touch_remaining_or_ledger(client):
    _sample(client, "OP-1", "S1")
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
    ])
    _reassign(client, "BR-2", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": None},
    ])
    bal = client.get("/api/v1/checks/balances").json()
    assert bal["consistent"] is True
    s1 = next(b for b in bal["samples"] if b["sample_id"] == "S1")
    assert s1["stored_remaining"] == 100.0
    # 版本不递增(换绑不是余量操作)
    conn = next(app.dependency_overrides[get_conn]())
    assert conn.execute(
        "SELECT version FROM samples WHERE sample_id = 'S1'"
    ).fetchone()["version"] == 0


# ---------------------------------------------------------------- 校验与整批回滚

def test_old_binding_mismatch_conflicts_and_changes_nothing(client):
    _sample(client, "OP-1", "S1")
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
    ])
    r = _reassign(client, "BR-BAD", [
        {"sample_id": "S1", "expected_old_barcode": "B9", "target_barcode": "B2"},
    ])
    assert r.status_code == 409
    c = r.json()["conflicts"][0]
    assert c["code"] == "barcode_mismatch"
    assert c["sample_id"] == "S1"
    assert c["op_no"] == "BR-BAD"
    assert c["batch_index"] == 0
    assert c["requested_barcode"] == "B9"
    assert c["available_barcode"] == "B1"
    assert "B9" in c["message"] and "B1" in c["message"]
    # 整批不变:仍绑 B1,B2 未出现绑定
    assert _bound(client, "B1")["sample_id"] == "S1"
    assert _bound(client, "B2")["bound"] is False


def test_expected_null_but_already_bound_conflicts(client):
    _sample(client, "OP-1", "S1")
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
    ])
    r = _reassign(client, "BR-BAD", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B2"},
    ])
    assert r.status_code == 409
    c = r.json()["conflicts"][0]
    assert c["code"] == "barcode_mismatch"
    assert c["requested_barcode"] is None
    assert c["available_barcode"] == "B1"
    assert "(未绑定)" in c["message"]


def test_target_barcode_held_by_sample_outside_batch_conflicts(client):
    for sid in ("S1", "S2"):
        _sample(client, f"OP-{sid}", sid)
    _reassign(client, "BR-1", [
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": "B2"},
    ])
    # S1 想占用 B2,但持有者 S2 不在本批
    r = _reassign(client, "BR-BAD", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B2"},
    ])
    assert r.status_code == 409
    c = r.json()["conflicts"][0]
    assert c["code"] == "barcode_already_bound"
    assert c["sample_id"] == "S1"
    assert c["requested_barcode"] == "B2"
    assert "S2" in c["message"]
    assert _bound(client, "B2")["sample_id"] == "S2"


def test_target_held_by_in_batch_sample_is_allowed_as_swap(client):
    for sid in ("S1", "S2"):
        _sample(client, f"OP-{sid}", sid)
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": "B2"},
    ])
    r = _reassign(client, "BR-2", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B2"},
        {"sample_id": "S2", "expected_old_barcode": "B2", "target_barcode": "B1"},
    ])
    assert r.status_code == 200, r.text


def test_sample_not_found_conflicts(client):
    r = _reassign(client, "BR-BAD", [
        {"sample_id": "GHOST", "expected_old_barcode": None,
         "target_barcode": "B1"},
    ])
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "sample_not_found"
    assert _bound(client, "B1")["bound"] is False


def test_all_conflicts_collected_in_one_response(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-4", "S4")
    _sample(client, "OP-5", "S5")
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
        {"sample_id": "S5", "expected_old_barcode": None, "target_barcode": "B5"},
    ])
    r = _reassign(client, "BR-BAD", [
        {"sample_id": "S1", "expected_old_barcode": "WRONG",
         "target_barcode": "B9"},                                        # 旧绑定不符
        {"sample_id": "GHOST", "expected_old_barcode": None,
         "target_barcode": "B10"},                                       # 样品不存在
        {"sample_id": "S4", "expected_old_barcode": None,
         "target_barcode": "B5"},                                        # B5 被批外 S5 占用
    ])
    assert r.status_code == 409
    assert [c["code"] for c in r.json()["conflicts"]] == [
        "barcode_mismatch", "sample_not_found", "barcode_already_bound"
    ]
    # 整批不变
    assert _bound(client, "B1")["sample_id"] == "S1"
    assert _bound(client, "B5")["sample_id"] == "S5"
    # 失败批次不留审计痕迹
    conn = next(app.dependency_overrides[get_conn]())
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM barcode_reassignments"
        " WHERE reassignment_no = 'BR-BAD'"
    ).fetchone()["c"] == 0


def test_single_conflict_rolls_back_other_valid_items(client):
    """批内一项冲突时,其余合法项的换绑也不能生效。"""
    for sid in ("S1", "S2"):
        _sample(client, f"OP-{sid}", sid)
    r = _reassign(client, "BR-BAD", [
        {"sample_id": "S1", "expected_old_barcode": None,
         "target_barcode": "B1"},
        {"sample_id": "S2", "expected_old_barcode": "B-X",
         "target_barcode": "B2"},
    ])
    assert r.status_code == 409
    assert _bound(client, "B1")["bound"] is False
    conn = next(app.dependency_overrides[get_conn]())
    assert _all_bindings(conn) == {}


# ---------------------------------------------------------------- 请求体校验(422)

def test_duplicate_sample_id_in_batch_is_422(client):
    _sample(client, "OP-1", "S1")
    r = _reassign(client, "BR-X", [
        {"sample_id": "S1", "expected_old_barcode": None,
         "target_barcode": "B1"},
        {"sample_id": "S1", "expected_old_barcode": "B1",
         "target_barcode": "B2"},
    ])
    assert r.status_code == 422


def test_duplicate_nonnull_target_barcode_is_422(client):
    for sid in ("S1", "S2"):
        _sample(client, f"OP-{sid}", sid)
    r = _reassign(client, "BR-X", [
        {"sample_id": "S1", "expected_old_barcode": None,
         "target_barcode": "B1"},
        {"sample_id": "S2", "expected_old_barcode": None,
         "target_barcode": "B1"},
    ])
    assert r.status_code == 422
    assert "重复" in r.text


def test_multiple_null_targets_allowed_but_empty_items_422(client):
    for sid in ("S1", "S2"):
        _sample(client, f"OP-{sid}", sid)
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None,
         "target_barcode": "B1"},
        {"sample_id": "S2", "expected_old_barcode": None,
         "target_barcode": "B2"},
    ])
    r = _reassign(client, "BR-2", [
        {"sample_id": "S1", "expected_old_barcode": "B1",
         "target_barcode": None},
        {"sample_id": "S2", "expected_old_barcode": "B2",
         "target_barcode": None},
    ])
    assert r.status_code == 200, r.text
    assert client.post("/api/v1/barcodes/reassignments", json={
        "reassignment_no": "BR-3", "items": []
    }).status_code == 422


# ---------------------------------------------------------------- 幂等

def test_replay_same_payload_returns_first_result_without_new_audit(client):
    _sample(client, "OP-1", "S1")
    body = {
        "reassignment_no": "BR-ID", "operator": "甲",
        "items": [{"sample_id": "S1", "expected_old_barcode": None,
                   "target_barcode": "B1"}],
    }
    r1 = client.post("/api/v1/barcodes/reassignments", json=body)
    assert r1.status_code == 200
    assert r1.json()["replayed"] is False
    first_at = r1.json()["reassigned_at"]

    # 此后样品状态已变(再绑到别的码),同批次号同内容重放仍只回放首次结果
    _reassign(client, "BR-MOVE", [
        {"sample_id": "S1", "expected_old_barcode": "B1",
         "target_barcode": "B9"},
    ])
    r2 = client.post("/api/v1/barcodes/reassignments", json=body)
    assert r2.status_code == 200
    assert r2.json()["replayed"] is True
    assert r2.json()["reassigned_at"] == first_at
    # 重放不会再次移动:样品仍绑 B9
    assert _bound(client, "B9")["sample_id"] == "S1"
    assert _bound(client, "B1")["bound"] is False

    conn = next(app.dependency_overrides[get_conn]())
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM barcode_reassignments"
        " WHERE reassignment_no = 'BR-ID'"
    ).fetchone()["c"] == 1
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM barcode_reassignment_items"
        " WHERE reassignment_no = 'BR-ID'"
    ).fetchone()["c"] == 1


def test_same_no_different_payload_conflicts(client):
    _sample(client, "OP-1", "S1")
    body1 = {"reassignment_no": "BR-C", "items": [
        {"sample_id": "S1", "expected_old_barcode": None,
         "target_barcode": "B1"}]}
    assert client.post(
        "/api/v1/barcodes/reassignments", json=body1
    ).status_code == 200
    body2 = {"reassignment_no": "BR-C", "items": [
        {"sample_id": "S1", "expected_old_barcode": None,
         "target_barcode": "B2"}]}
    r = client.post("/api/v1/barcodes/reassignments", json=body2)
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "reassignment_no_conflict"


# ---------------------------------------------------------------- 查询

def test_get_reassignment_returns_first_result(client):
    _sample(client, "OP-1", "S1")
    _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None,
         "target_barcode": "B1"},
    ], operator="甲", note="n")
    r = client.get("/api/v1/barcodes/reassignments/BR-1")
    assert r.status_code == 200
    body = r.json()
    assert body["reassignment_no"] == "BR-1"
    assert body["operator"] == "甲"
    assert body["replayed"] is False
    assert body["items"][0]["target_barcode"] == "B1"


def test_get_unknown_reassignment_is_404(client):
    r = client.get("/api/v1/barcodes/reassignments/NOPE")
    assert r.status_code == 404
    assert r.json()["error"] == "reassignment_not_found"


def test_lookup_unbound_barcode_reports_bound_false(client):
    r = client.get("/api/v1/barcodes/NEVER")
    assert r.status_code == 200
    assert r.json() == {
        "barcode": "NEVER", "bound": False,
        "sample_id": None, "reassignment_no": None,
    }


# ---------------------------------------------------------------- 审计不可变

def test_audit_tables_are_immutable(client):
    _sample(client, "OP-1", "S1")
    assert _reassign(client, "BR-1", [
        {"sample_id": "S1", "expected_old_barcode": None,
         "target_barcode": "B1"},
    ]).status_code == 200

    conn = next(app.dependency_overrides[get_conn]())
    for sql in (
        "UPDATE barcode_reassignments SET note = 'z'",
        "DELETE FROM barcode_reassignments",
        "UPDATE barcode_reassignment_items SET target_barcode = 'z'",
        "DELETE FROM barcode_reassignment_items",
    ):
        with pytest.raises(sqlite3.Error):
            conn.execute(sql)


# ---------------------------------------------------------------- 并发

def test_concurrent_reassignments_serialize(tmp_path):
    """两笔并发换绑都要把 S1 绑到各自的新条码(都声称旧绑定为 null):
    BEGIN IMMEDIATE 串行化,后者因旧绑定已变(barcode_mismatch)失败。"""
    db_path = str(tmp_path / "t.db")
    setup = connect(db_path)
    init_db(setup)
    setup.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, version, is_root) VALUES ('S1', 'M', 'mL', 100, 100, 0, 1)"
    )
    setup.commit()
    setup.close()

    from app.schemas import BarcodeReassignmentRequest, BarcodeReassignmentItemIn

    winners: list[str] = []
    losers: list[tuple[str, str]] = []
    barrier = threading.Barrier(2)

    def worker(no: str, barcode: str) -> None:
        c = connect(db_path)
        barrier.wait()
        try:
            bc.create_reassignment(c, BarcodeReassignmentRequest(
                reassignment_no=no,
                items=[BarcodeReassignmentItemIn(
                    sample_id="S1", expected_old_barcode=None,
                    target_barcode=barcode)],
            ))
            winners.append(no)
        except bc.BarcodeError as exc:
            losers.append((no, exc.conflicts[0].code))
        finally:
            c.close()

    t1 = threading.Thread(target=worker, args=("BR-C1", "B1"))
    t2 = threading.Thread(target=worker, args=("BR-C2", "B2"))
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert len(winners) == 1 and len(losers) == 1
    assert losers[0][1] == "barcode_mismatch"

    check = connect(db_path)
    rows = list(check.execute(
        "SELECT barcode, reassignment_no FROM sample_barcodes WHERE sample_id = 'S1'"
    ))
    assert len(rows) == 1
    assert rows[0]["reassignment_no"] == winners[0]
    # 只有成功批次写了审计
    assert check.execute(
        "SELECT COUNT(*) AS c FROM barcode_reassignments"
    ).fetchone()["c"] == 1
    check.close()


# ---------------------------------------------------------------- 旧库迁移

def test_init_db_creates_barcode_tables_on_legacy_database(tmp_path):
    """旧版本数据库(仅有老 samples 表)启动初始化后,条码表自动补齐且全流程可用。"""
    db_path = str(tmp_path / "legacy.db")
    raw = sqlite3.connect(db_path)
    raw.execute(
        "CREATE TABLE samples (sample_id TEXT PRIMARY KEY, material TEXT NOT NULL,"
        " unit TEXT NOT NULL, initial_amount REAL NOT NULL, remaining REAL NOT NULL,"
        " location TEXT, is_root INTEGER NOT NULL DEFAULT 0, created_at TEXT)"
    )
    raw.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, location, is_root) VALUES"
        " ('S1', 'M', 'mL', 100, 100, NULL, 1)"
    )
    raw.commit()
    raw.close()

    conn = connect(db_path)
    init_db(conn)
    init_db(conn)  # 再执行一次也不重复创建

    tables = {
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    assert {"sample_barcodes", "barcode_reassignments",
            "barcode_reassignment_items"} <= tables

    from app.schemas import BarcodeReassignmentRequest, BarcodeReassignmentItemIn

    res = bc.create_reassignment(conn, BarcodeReassignmentRequest(
        reassignment_no="BR-M",
        items=[BarcodeReassignmentItemIn(
            sample_id="S1", expected_old_barcode=None, target_barcode="B1")],
    ))
    assert res.items[0].target_barcode == "B1"
    got = bc.lookup_barcode(conn, "B1")
    assert got.bound is True and got.sample_id == "S1"
    conn.close()
