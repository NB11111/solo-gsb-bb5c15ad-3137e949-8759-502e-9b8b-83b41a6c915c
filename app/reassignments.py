"""样品条码整批换绑服务:一个换绑批次号对多个样品做条码解绑/重绑。

规则
----
- 请求逐项给出样品号、**预期旧条码** `expected_old_barcode` 与**目标条码**
  `target_barcode`,二者均可为 null:预期旧条码 null 表示样品当前未绑定;
  目标条码 null 表示本次仅解绑。同批样品号不得重复、非空目标条码不得重复
  (请求模型校验)。
- 服务在单个 `BEGIN IMMEDIATE` 事务内逐项核对:
    1. 样品存在;
    2. 样品当前绑定与预期旧条码完全一致(均为 null 表示"未绑定");
    3. 非空目标条码未被**批外**样品占用(批内样品之间互换/循环允许)。
  任一项冲突即**整批不变**,响应 `conflicts[]` 一次返回全部逐项原因。
- 全部通过后在同一事务内**先统一删除本批样品的当前绑定行(解除旧绑定),
  再统一插入新绑定行**;两阶段写入使两样品互换与多样品循环换绑不会触发
  `sample_barcodes.barcode` 的唯一约束。
- 换绑成功逐项写不可变审计明细(换绑前实际条码 -> 换绑后条码,触发器禁止
  UPDATE/DELETE),并写换绑单头;换绑不改动余量,不产生谱系边与流水。

幂等
----
同一换绑批次号 + 同一请求体重放返回首次结果(`replayed: true`),
不重复换绑、不重复写审计;同号不同内容返回 `409 reassignment_no_conflict`。
另提供按条码查询当前绑定样品的接口(未绑定返回 bound=false)。
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from . import schemas as s


class ReassignmentError(Exception):
    """条码整批换绑业务冲突,携带结构化冲突列表。"""

    def __init__(self, conflicts: list[s.Conflict]):
        self.conflicts = conflicts
        super().__init__(conflicts[0].message if conflicts else "reassignment conflict")


def _conflict(
    code: str,
    message: str,
    *,
    reassignment_no: Optional[str] = None,
    sample_id: Optional[str] = None,
    expected_barcode: Optional[str] = None,
    actual_barcode: Optional[str] = None,
    requested_barcode: Optional[str] = None,
    bound_sample_id: Optional[str] = None,
) -> s.Conflict:
    return s.Conflict(
        code=code,
        message=message,
        op_no=reassignment_no,
        sample_id=sample_id,
        expected_barcode=expected_barcode,
        actual_barcode=actual_barcode,
        requested_barcode=requested_barcode,
        bound_sample_id=bound_sample_id,
    )


# ---------------------------------------------------------------- 查询/组装

def _get_header(
    conn: sqlite3.Connection, reassignment_no: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM barcode_reassignments WHERE reassignment_no = ?",
        (reassignment_no,),
    ).fetchone()


def _load_items(
    conn: sqlite3.Connection, reassignment_no: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM barcode_reassignment_items WHERE reassignment_no = ?"
        " ORDER BY seq_index, item_id",
        (reassignment_no,),
    ).fetchall()


def _build_result(
    header: sqlite3.Row, items: list[sqlite3.Row], *, replayed: bool
) -> s.BarcodeReassignmentResult:
    return s.BarcodeReassignmentResult(
        reassignment_no=header["reassignment_no"],
        operator=header["operator"],
        note=header["note"],
        rebound_at=header["created_at"],
        created_at=header["created_at"],
        replayed=replayed,
        items=[
            s.BarcodeReassignmentItemOut(
                sample_id=r["sample_id"],
                old_barcode=r["old_barcode"],
                new_barcode=r["new_barcode"],
            )
            for r in items
        ],
    )


# ---------------------------------------------------------------- 提交换绑

def create_reassignment(
    conn: sqlite3.Connection, req: s.BarcodeReassignmentRequest
) -> s.BarcodeReassignmentResult:
    """单事务逐项核对样品/旧绑定/目标条码占用;全部通过先解绑再绑新并写审计。"""
    payload = req.model_dump_json()

    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = _get_header(conn, req.reassignment_no)
        if existing is not None:
            if existing["payload"] != payload:
                raise ReassignmentError([
                    _conflict(
                        "reassignment_no_conflict",
                        f"换绑批次号 {req.reassignment_no} 已存在但请求内容不同,"
                        "同一换绑批次号必须对应同一次条码换绑(幂等键冲突)",
                        reassignment_no=req.reassignment_no,
                    )
                ])
            result = _build_result(
                existing, _load_items(conn, req.reassignment_no), replayed=True
            )
            conn.execute("COMMIT")
            return result

        ids = list(dict.fromkeys(i.sample_id for i in req.items))  # model 已保证不重复
        qmarks = ",".join("?" * len(ids))
        sample_rows = {
            r["sample_id"]: r
            for r in conn.execute(
                f"SELECT sample_id FROM samples WHERE sample_id IN ({qmarks})", ids
            ).fetchall()
        }
        # 本批样品的当前绑定(样品 -> 条码);无行表示未绑定
        current: dict[str, Optional[str]] = {
            r["sample_id"]: r["barcode"]
            for r in conn.execute(
                f"SELECT sample_id, barcode FROM sample_barcodes"
                f" WHERE sample_id IN ({qmarks})",
                ids,
            ).fetchall()
        }
        # 非空目标条码的全局占用情况(条码 -> 占用样品),用于批外占用校验
        targets = list(dict.fromkeys(
            i.target_barcode for i in req.items if i.target_barcode is not None
        ))
        tmarks = ",".join("?" * len(targets))
        occupied: dict[str, str] = {}
        if targets:
            occupied = {
                r["barcode"]: r["sample_id"]
                for r in conn.execute(
                    f"SELECT barcode, sample_id FROM sample_barcodes"
                    f" WHERE barcode IN ({tmarks})",
                    targets,
                ).fetchall()
            }

        in_batch = set(ids)

        # 先做全部静态校验,收集逐项冲突后一次性抛出;任一冲突整批不变
        conflicts: list[s.Conflict] = []
        for item in req.items:
            if item.sample_id not in sample_rows:
                conflicts.append(_conflict(
                    "sample_not_found",
                    f"样品 {item.sample_id} 不存在,无法换绑条码",
                    reassignment_no=req.reassignment_no,
                    sample_id=item.sample_id,
                    requested_barcode=item.target_barcode,
                ))
                continue
            actual = current.get(item.sample_id)
            if actual != item.expected_old_barcode:
                conflicts.append(_conflict(
                    "barcode_mismatch",
                    f"样品 {item.sample_id} 当前实际条码"
                    f"「{actual if actual is not None else '(未绑定)'}」与预期旧条码"
                    f"「{item.expected_old_barcode if item.expected_old_barcode is not None else '(未绑定)'}」"
                    "不符,整批拒绝换绑",
                    reassignment_no=req.reassignment_no,
                    sample_id=item.sample_id,
                    expected_barcode=item.expected_old_barcode,
                    actual_barcode=actual,
                    requested_barcode=item.target_barcode,
                ))
            if item.target_barcode is not None:
                holder = occupied.get(item.target_barcode)
                if holder is not None and holder not in in_batch:
                    conflicts.append(_conflict(
                        "barcode_already_bound",
                        f"目标条码 {item.target_barcode} 已被批外样品 {holder} 占用,"
                        "不能再绑定到本批样品(整批拒绝换绑)",
                        reassignment_no=req.reassignment_no,
                        sample_id=item.sample_id,
                        expected_barcode=item.expected_old_barcode,
                        actual_barcode=actual,
                        requested_barcode=item.target_barcode,
                        bound_sample_id=holder,
                    ))
        if conflicts:
            raise ReassignmentError(conflicts)

        # 全部通过:写换绑单头;先统一解除本批样品旧绑定,再统一写入新绑定。
        # 两阶段删除/插入保证批内互换(A<->B)与多样品循环不违反条码唯一约束。
        conn.execute(
            "INSERT INTO barcode_reassignments (reassignment_no, operator, payload, note)"
            " VALUES (?, ?, ?, ?)",
            (req.reassignment_no, req.operator, payload, req.note),
        )
        conn.execute(
            f"DELETE FROM sample_barcodes WHERE sample_id IN ({qmarks})", ids
        )
        for idx, item in enumerate(req.items):
            old_barcode = current.get(item.sample_id)
            conn.execute(
                "INSERT INTO barcode_reassignment_items"
                " (reassignment_no, sample_id, seq_index, old_barcode, new_barcode,"
                " operator) VALUES (?, ?, ?, ?, ?, ?)",
                (req.reassignment_no, item.sample_id, idx,
                 old_barcode, item.target_barcode, req.operator),
            )
            if item.target_barcode is not None:
                conn.execute(
                    "INSERT INTO sample_barcodes"
                    " (sample_id, barcode, reassignment_no) VALUES (?, ?, ?)",
                    (item.sample_id, item.target_barcode, req.reassignment_no),
                )

        header = _get_header(conn, req.reassignment_no)
        result = _build_result(
            header, _load_items(conn, req.reassignment_no), replayed=False  # type: ignore[arg-type]
        )
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------- 按条码查询

def lookup_barcode(conn: sqlite3.Connection, barcode: str) -> s.BarcodeLookupResponse:
    """按条码查询当前绑定样品;无绑定行返回 bound=false。"""
    row = conn.execute(
        "SELECT b.sample_id AS sample_id, b.updated_at AS updated_at,"
        " b.reassignment_no AS reassignment_no, r.created_at AS bound_at"
        " FROM sample_barcodes b"
        " LEFT JOIN barcode_reassignments r"
        " ON r.reassignment_no = b.reassignment_no"
        " WHERE b.barcode = ?",
        (barcode,),
    ).fetchone()
    if row is None:
        return s.BarcodeLookupResponse(barcode=barcode, bound=False)
    return s.BarcodeLookupResponse(
        barcode=barcode,
        bound=True,
        sample_id=row["sample_id"],
        bound_at=row["bound_at"] or row["updated_at"],
        reassignment_no=row["reassignment_no"],
    )
