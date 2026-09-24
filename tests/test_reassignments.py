"""条码整批换绑模块端到端测试:

- 一个换绑批次号对多个样品给出预期旧条码/目标条码,单事务先解绑再绑新
- 两样品互换、多样品循环、解绑(null 目标)与从未绑定样品(null 预期)均可
- 样品不存在、旧绑定与预期不符、目标条码被批外样品占用:冲突一次列全、整批不变
- 同批样品号/非空目标条码重复在请求模型层拒绝
- 同批次号同内容幂等重放(不重复审计),异内容 409
- 不可变审计表触发器禁止 UPDATE/DELETE;换绑不影响余量账本
- 按条码查询当前样品,未绑定返回 bound=false
- 并发换绑同一组样品由 BEGIN IMMEDIATE 串行化
- 旧版本数据库启动后自动建表(条码换绑表随初始化幂等创建)
"""
from __future__ import annotations

import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app import reassignments as ra
from app import schemas as s
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


def _sample(client, op_no, sid, amount=100):
    r = client.post("/api/v1/operations/sampling", json={
        "type": "sampling", "op_no": op_no, "sample_id": sid,
        "material": "血清", "amount": amount, "unit": "mL",
    })
    assert r.status_code == 200, r.text


def _bind_direct(client, sample_id, barcode):
    """绕过 API 直接造初始绑定(模拟历史数据)。"""
    db = next(app.dependency_overrides[get_conn]())
    db.execute(
        "INSERT INTO sample_barcodes (sample_id, barcode) VALUES (?, ?)",
        (sample_id, barcode),
    )


def _barcodes(client):
    db = next(app.dependency_overrides[get_conn]())
    return {
        r["sample_id"]: r["barcode"]
        for r in db.execute("SELECT sample_id, barcode FROM sample_barcodes")
    }


def _reassign(client, no, items, operator="条码管理员", **extra):
    return client.post("/api/v1/barcodes/reassignments", json={
        "reassignment_no": no, "operator": operator, "items": items, **extra,
    })


# ---------------------------------------------------------------- 成功路径

def test_reassign_swap_two_samples(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-2", "S2")
    _bind_direct(client, "S1", "B1")
    _bind_direct(client, "S2", "B2")

    r = _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B2"},
        {"sample_id": "S2", "expected_old_barcode": "B2", "target_barcode": "B1"},
    ], note="贴反纠正")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["reassignment_no"] == "RA-1"
    assert body["operator"] == "条码管理员"
    assert body["note"] == "贴反纠正"
    assert body["replayed"] is False
    assert body["rebound_at"] == body["created_at"]
    assert body["items"] == [
        {"sample_id": "S1", "old_barcode": "B1", "new_barcode": "B2"},
        {"sample_id": "S2", "old_barcode": "B2", "new_barcode": "B1"},
    ]
    assert _barcodes(client) == {"S1": "B2", "S2": "B1"}


def test_reassign_three_sample_cycle_and_unbind_and_bind_fresh(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-2", "S2")
    _sample(client, "OP-3", "S3")
    _bind_direct(client, "S1", "B1")
    _bind_direct(client, "S2", "B2")
    # S3 未绑定;目标:B1->S2, B2->S3, S1 解绑(三样品循环 + 解绑 + 新绑)
    r = _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": None},
        {"sample_id": "S2", "expected_old_barcode": "B2", "target_barcode": "B1"},
        {"sample_id": "S3", "expected_old_barcode": None, "target_barcode": "B2"},
    ])
    assert r.status_code == 200, r.text
    assert r.json()["items"] == [
        {"sample_id": "S1", "old_barcode": "B1", "new_barcode": None},
        {"sample_id": "S2", "old_barcode": "B2", "new_barcode": "B1"},
        {"sample_id": "S3", "old_barcode": None, "new_barcode": "B2"},
    ]
    assert _barcodes(client) == {"S2": "B1", "S3": "B2"}


def test_reassign_does_not_touch_balance_ledger(client):
    _sample(client, "OP-1", "S1", amount=100)
    _bind_direct(client, "S1", "B1")
    assert _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B9"},
    ]).status_code == 200
    bal = client.get("/api/v1/checks/balances").json()
    assert bal["consistent"] is True
    assert bal["samples"][0]["stored_remaining"] == 100.0


# ---------------------------------------------------------------- 请求级校验

def test_duplicate_sample_id_in_batch_rejected(client):
    r = _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B1"},
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B2"},
    ])
    assert r.status_code == 422
    assert "重复 sample_id" in r.text


def test_duplicate_non_null_target_barcode_rejected(client):
    r = _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "BX"},
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": "BX"},
    ])
    assert r.status_code == 422
    assert "重复的非空 target_barcode" in r.text


def test_multiple_null_target_barcodes_allowed(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-2", "S2")
    r = _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": None},
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": None},
    ])
    assert r.status_code == 200, r.text
    assert _barcodes(client) == {}


# ---------------------------------------------------------------- 冲突与整批回滚

def test_conflicts_collected_per_item_and_whole_batch_unchanged(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-2", "S2")
    _sample(client, "OP-3", "S3")
    _sample(client, "OP-4", "S4")
    _sample(client, "OP-5", "S5")
    _bind_direct(client, "S1", "B1")
    _bind_direct(client, "S2", "B2")
    # S3、S4 未绑定;S5 不在批次内且持有 BX(批外占用)
    _bind_direct(client, "S5", "BX")
    r = _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": "B9", "target_barcode": "B9"},
        {"sample_id": "S2", "expected_old_barcode": "B2", "target_barcode": "B1"},
        {"sample_id": "S3", "expected_old_barcode": None, "target_barcode": "B2"},
        {"sample_id": "S4", "expected_old_barcode": None, "target_barcode": "BX"},
        {"sample_id": "NOPE", "expected_old_barcode": None, "target_barcode": "B3"},
    ])
    assert r.status_code == 409
    conflicts = r.json()["conflicts"]
    by_sample = {c["sample_id"]: c for c in conflicts}
    # S1 旧绑定不符:预期 B9、实际 B1
    assert by_sample["S1"]["code"] == "barcode_mismatch"
    assert by_sample["S1"]["expected_barcode"] == "B9"
    assert by_sample["S1"]["actual_barcode"] == "B1"
    # S2/S3 是批内互换链条(B1、B2 的占用者都在批内),不构成冲突
    assert "S2" not in by_sample
    assert "S3" not in by_sample
    # S4 目标 BX 被批外样品 S5 占用
    assert by_sample["S4"]["code"] == "barcode_already_bound"
    assert by_sample["S4"]["bound_sample_id"] == "S5"
    # 样品不存在
    assert by_sample["NOPE"]["code"] == "sample_not_found"

    # 整批不变:绑定维持原状,失败批次不留任何记录
    assert _barcodes(client) == {"S1": "B1", "S2": "B2", "S5": "BX"}
    db = next(app.dependency_overrides[get_conn]())
    assert db.execute(
        "SELECT 1 FROM barcode_reassignments WHERE reassignment_no = 'RA-1'"
    ).fetchone() is None
    assert db.execute(
        "SELECT COUNT(*) AS c FROM barcode_reassignment_items"
    ).fetchone()["c"] == 0


def test_target_barcode_held_by_sample_outside_batch_conflicts(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-2", "S2")
    _bind_direct(client, "S1", "B1")  # S1 不在批次内
    r = _reassign(client, "RA-1", [
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": "B1"},
    ])
    assert r.status_code == 409
    c = r.json()["conflicts"][0]
    assert c["code"] == "barcode_already_bound"
    assert c["bound_sample_id"] == "S1"
    assert _barcodes(client) == {"S1": "B1"}


def test_target_barcode_held_within_batch_is_allowed_for_swap(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-2", "S2")
    _bind_direct(client, "S1", "B1")
    _bind_direct(client, "S2", "B2")
    # 即使两项各自的目标当前被同批另一样品占用,也允许(互换)
    r = _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B2"},
        {"sample_id": "S2", "expected_old_barcode": "B2", "target_barcode": "B1"},
    ])
    assert r.status_code == 200, r.text


def test_expected_unbound_but_actually_bound_conflicts(client):
    _sample(client, "OP-1", "S1")
    _bind_direct(client, "S1", "B1")
    r = _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": None, "target_barcode": "B2"},
    ])
    assert r.status_code == 409
    c = r.json()["conflicts"][0]
    assert c["code"] == "barcode_mismatch"
    assert c["expected_barcode"] is None
    assert c["actual_barcode"] == "B1"
    assert _barcodes(client) == {"S1": "B1"}


# ---------------------------------------------------------------- 幂等

def test_same_batch_same_payload_replays_without_rewriting_audit(client):
    _sample(client, "OP-1", "S1")
    _bind_direct(client, "S1", "B1")
    payload = [{"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B2"}]

    first = _reassign(client, "RA-1", payload)
    assert first.status_code == 200
    assert first.json()["replayed"] is False
    first_time = first.json()["rebound_at"]

    # 重放前先把现状改乱,证明重放不会再次换绑
    db = next(app.dependency_overrides[get_conn]())
    db.execute("UPDATE samples SET location = 'X' WHERE sample_id = 'S1'")  # 无影响字段
    db.execute("DELETE FROM sample_barcodes WHERE sample_id = 'S1'")

    second = _reassign(client, "RA-1", payload)
    assert second.status_code == 200
    body = second.json()
    assert body["replayed"] is True
    assert body["rebound_at"] == first_time
    assert body["items"] == first.json()["items"]

    # 审计只有一份;S1 当前仍为解绑态(重放不重新绑定)
    assert db.execute(
        "SELECT COUNT(*) AS c FROM barcode_reassignment_items"
        " WHERE reassignment_no = 'RA-1'"
    ).fetchone()["c"] == 1
    assert db.execute(
        "SELECT barcode FROM sample_barcodes WHERE sample_id = 'S1'"
    ).fetchone() is None


def test_same_batch_different_payload_conflicts(client):
    _sample(client, "OP-1", "S1")
    _bind_direct(client, "S1", "B1")
    assert _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B2"},
    ]).status_code == 200
    r = _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": "B2", "target_barcode": "B3"},
    ])
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "reassignment_no_conflict"
    assert _barcodes(client) == {"S1": "B2"}


# ---------------------------------------------------------------- 不可变审计

def test_audit_tables_are_immutable(client):
    _sample(client, "OP-1", "S1")
    _bind_direct(client, "S1", "B1")
    assert _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": "B2"},
    ]).status_code == 200
    db = next(app.dependency_overrides[get_conn]())
    for sql in (
        "UPDATE barcode_reassignments SET note = 'x'",
        "DELETE FROM barcode_reassignments WHERE reassignment_no = 'RA-1'",
        "UPDATE barcode_reassignment_items SET new_barcode = 'z'",
        "DELETE FROM barcode_reassignment_items WHERE reassignment_no = 'RA-1'",
    ):
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(sql)


# ---------------------------------------------------------------- 按条码查询

def test_lookup_barcode_returns_current_sample(client):
    _sample(client, "OP-1", "S1")
    _sample(client, "OP-2", "S2")
    _bind_direct(client, "S1", "B1")
    r = client.get("/api/v1/barcodes/B1")
    assert r.status_code == 200
    body = r.json()
    assert body["barcode"] == "B1"
    assert body["bound"] is True
    assert body["sample_id"] == "S1"
    assert body["bound_at"] is not None  # 绑定时间(直接造数取 updated_at)
    assert body["reassignment_no"] is None

    # 经换绑接口换给 S2 后,查询反映最新绑定并带来源批次
    assert _reassign(client, "RA-1", [
        {"sample_id": "S1", "expected_old_barcode": "B1", "target_barcode": None},
        {"sample_id": "S2", "expected_old_barcode": None, "target_barcode": "B1"},
    ]).status_code == 200
    body = client.get("/api/v1/barcodes/B1").json()
    assert body["bound"] is True
    assert body["sample_id"] == "S2"
    assert body["reassignment_no"] == "RA-1"
    assert body["bound_at"] is not None

    # 未绑定条码
    body = client.get("/api/v1/barcodes/UNUSED").json()
    assert body == {
        "barcode": "UNUSED", "bound": False, "sample_id": None,
        "bound_at": None, "reassignment_no": None,
    }


# ---------------------------------------------------------------- 并发串行化

def test_concurrent_reassignments_serialize(tmp_path):
    db_path = str(tmp_path / "t.db")
    setup = connect(db_path)
    init_db(setup)
    setup.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, is_root) VALUES"
        " ('S1', 'M', 'mL', 100, 100, 1), ('S2', 'M', 'mL', 100, 100, 1)"
    )
    setup.execute("INSERT INTO sample_barcodes (sample_id, barcode) VALUES ('S1', 'B1')")
    setup.commit()
    setup.close()

    winners: list[str] = []
    losers: list[tuple[str, str]] = []
    barrier = threading.Barrier(2)

    def worker(no: str, target: str) -> None:
        c = connect(db_path)
        barrier.wait()
        try:
            ra.create_reassignment(c, s.BarcodeReassignmentRequest(
                reassignment_no=no,
                items=[s.BarcodeReassignmentItemIn(
                    sample_id="S1", expected_old_barcode="B1", target_barcode=target)],
            ))
            winners.append(no)
        except ra.ReassignmentError as exc:
            losers.append((no, exc.conflicts[0].code))
        finally:
            c.close()

    t1 = threading.Thread(target=worker, args=("RA-C1", "B9"))
    t2 = threading.Thread(target=worker, args=("RA-C2", "B9"))
    t1.start(); t2.start()
    t1.join(); t2.join()

    # 两个并发批次都想把 B1 换成 B9:恰好一成一败(败者看到旧绑定已是 null)
    assert len(winners) == 1 and len(losers) == 1
    assert losers[0][1] == "barcode_mismatch"
    check = connect(db_path)
    assert check.execute(
        "SELECT barcode FROM sample_barcodes WHERE sample_id = 'S1'"
    ).fetchone()["barcode"] == "B9"
    assert check.execute(
        "SELECT COUNT(*) AS c FROM barcode_reassignment_items"
    ).fetchone()["c"] == 1
    check.close()


# ---------------------------------------------------------------- 旧库迁移

def test_init_db_creates_reassignment_tables_on_legacy_database(tmp_path):
    import sqlite3 as _sqlite3

    db_path = str(tmp_path / "legacy.db")
    raw = _sqlite3.connect(db_path)
    raw.execute(
        "CREATE TABLE samples (sample_id TEXT PRIMARY KEY, material TEXT NOT NULL,"
        " unit TEXT NOT NULL, initial_amount REAL NOT NULL, remaining REAL NOT NULL,"
        " location TEXT, is_root INTEGER NOT NULL DEFAULT 0, created_at TEXT)"
    )
    raw.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, is_root) VALUES ('S1', 'M', 'mL', 100, 100, 1)"
    )
    raw.commit()
    raw.close()

    conn = connect(db_path)
    init_db(conn)
    init_db(conn)  # 重复执行幂等

    tables = {
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    assert {
        "barcode_reassignments", "barcode_reassignment_items", "sample_barcodes",
    } <= tables

    res = ra.create_reassignment(conn, s.BarcodeReassignmentRequest(
        reassignment_no="RA-M", operator="迁移人",
        items=[s.BarcodeReassignmentItemIn(
            sample_id="S1", expected_old_barcode=None, target_barcode="B1")],
    ))
    assert res.items[0].new_barcode == "B1"
    assert ra.lookup_barcode(conn, "B1").sample_id == "S1"
    conn.close()
