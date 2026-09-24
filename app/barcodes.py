"""样品条码整批换绑服务:一个换绑批次号对应多项样品的条码重绑。

规则
----
- 请求逐项给出样品号、**预期旧条码** `expected_old_barcode` 与**目标条码**
  `target_barcode`(两者为 `null` 分别表示「预期当前未绑定」与「解绑为未绑定」)。
  同批样品号不得重复、同批非空目标条码不得重复(请求体模型直接 422 拒绝)。
- 单个 `BEGIN IMMEDIATE` 事务内逐项核对:
    1. 样品存在;
    2. 样品当前绑定与 `expected_old_barcode` 完全一致(含 null=未绑定);
    3. 非空目标条码未被**本批之外**的样品占用(批内样品之间互换/循环允许)。
  任一项不符即**整批回滚**,响应 `conflicts[]` 一次返回全部不符明细。
- 全部通过后先**删除本批样品的全部旧绑定**,再统一写入新绑定
  (解绑项不写),因此两样品互换、多样品循环、本批内解绑再占用均不会触发
  唯一约束冲突。成功后逐项写入不可变换绑审计明细(触发器禁止 UPDATE/DELETE)。
- 换绑只改条码绑定,不改动余量、版本,不产生谱系边与出入库流水。

幂等
----
同一换绑批次号 + 同一请求体重放返回首次结果(`replayed: true`),
不重复换绑、不重复写审计;同号不同内容返回 `409 reassignment_no_conflict`。
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from . import schemas as s


class BarcodeError(Exception):
    """条码换绑业务冲突,携带结构化冲突列表。"""

    def __init__(self, conflicts: list[s.Conflict]):
        self.conflicts = conflicts
        super().__init__(conflicts[0].message if conflicts else "barcode conflict")


def _conflict(
    code: str,
    message: str,
    *,
    reassignment_no: Optional[str] = None,
    sample_id: Optional[str] = None,
    batch_index: Optional[int] = None,
    requested_barcode: Optional[str] = None,
    available_barcode: Optional[str] = None,
) -> s.Conflict:
    return s.Conflict(
        code=code,
        message=message,
        op_no=reassignment_no,
        sample_id=sample_id,
        batch_index=batch_index,
        requested_barcode=requested_barcode,
        available_barcode=available_barcode,
    )


def _fmt(barcode: Optional[str]) -> str:
    return "「%s」" % barcode if barcode is not None else "(未绑定)"


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
        reassigned_at=header["created_at"],
        created_at=header["created_at"],
        replayed=replayed,
        items=[
            s.BarcodeReassignmentItemOut(
                sample_id=r["sample_id"],
                expected_old_barcode=r["expected_old_barcode"],
                target_barcode=r["target_barcode"],
                from_barcode=r["from_barcode"],
            )
            for r in items
        ],
    )


# ---------------------------------------------------------------- 提交换绑

def create_reassignment(
    conn: sqlite3.Connection, req: s.BarcodeReassignmentRequest
) -> s.BarcodeReassignmentResult:
    """单事务核对存在性/旧绑定/目标占用;全部通过先解绑再统一写入并写审计。"""
    payload = req.model_dump_json()

    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = _get_header(conn, req.reassignment_no)
        if existing is not None:
            if existing["payload"] != payload:
                raise BarcodeError([
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
        id_marks = ",".join("?" * len(ids))
        sample_rows = {
            r["sample_id"]: r
            for r in conn.execute(
                f"SELECT sample_id FROM samples WHERE sample_id IN ({id_marks})",
                ids,
            ).fetchall()
        }
        targets = [i.target_barcode for i in req.items
                   if i.target_barcode is not None]
        if targets:
            target_marks = ",".join("?" * len(targets))
            bind_rows = conn.execute(
                f"SELECT barcode, sample_id FROM sample_barcodes"
                f" WHERE sample_id IN ({id_marks})"
                f" OR barcode IN ({target_marks})",
                ids + targets,
            ).fetchall()
        else:
            bind_rows = conn.execute(
                f"SELECT barcode, sample_id FROM sample_barcodes"
                f" WHERE sample_id IN ({id_marks})",
                ids,
            ).fetchall()
        # 当前绑定:barcode -> 行(含持有者 sample_id)
        bindings: dict[str, sqlite3.Row] = {r["barcode"]: r for r in bind_rows}
        current_by_sample: dict[str, str] = {
            r["sample_id"]: r["barcode"] for r in bind_rows
        }

        # 逐项静态校验,收集全部冲突后一次抛出;任一不符整批不变
        conflicts: list[s.Conflict] = []
        for idx, item in enumerate(req.items):
            if item.sample_id not in sample_rows:
                conflicts.append(_conflict(
                    "sample_not_found",
                    f"样品 {item.sample_id} 不存在,无法换绑条码",
                    reassignment_no=req.reassignment_no,
                    sample_id=item.sample_id,
                    batch_index=idx,
                    requested_barcode=item.expected_old_barcode,
                ))
                continue
            actual = current_by_sample.get(item.sample_id)
            if actual != item.expected_old_barcode:
                conflicts.append(_conflict(
                    "barcode_mismatch",
                    f"样品 {item.sample_id} 当前绑定条码 {_fmt(actual)} 与预期旧条码"
                    f" {_fmt(item.expected_old_barcode)} 不符,整批拒绝换绑",
                    reassignment_no=req.reassignment_no,
                    sample_id=item.sample_id,
                    batch_index=idx,
                    requested_barcode=item.expected_old_barcode,
                    available_barcode=actual,
                ))
            target = item.target_barcode
            if target is not None:
                holder = bindings.get(target)
                if holder is not None and holder["sample_id"] not in sample_rows:
                    conflicts.append(_conflict(
                        "barcode_already_bound",
                        f"目标条码 {target} 已被本批之外的样品"
                        f" {holder['sample_id']} 占用,整批拒绝换绑",
                        reassignment_no=req.reassignment_no,
                        sample_id=item.sample_id,
                        batch_index=idx,
                        requested_barcode=target,
                    ))
        if conflicts:
            raise BarcodeError(conflicts)

        # 全部通过:先解除本批样品的旧绑定(含本批内互换/循环),再统一写入新绑定;
        # target_barcode 为 null 的解绑项不产生新绑定
        conn.execute(
            f"DELETE FROM sample_barcodes WHERE sample_id IN ({id_marks})", ids
        )
        conn.execute(
            "INSERT INTO barcode_reassignments (reassignment_no, operator,"
            " payload, note) VALUES (?, ?, ?, ?)",
            (req.reassignment_no, req.operator, payload, req.note),
        )
        for idx, item in enumerate(req.items):
            if item.target_barcode is not None:
                conn.execute(
                    "INSERT INTO sample_barcodes (barcode, sample_id,"
                    " reassignment_no) VALUES (?, ?, ?)",
                    (item.target_barcode, item.sample_id, req.reassignment_no),
                )
            conn.execute(
                "INSERT INTO barcode_reassignment_items (reassignment_no,"
                " sample_id, seq_index, expected_old_barcode, target_barcode,"
                " from_barcode, operator)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (req.reassignment_no, item.sample_id, idx,
                 item.expected_old_barcode, item.target_barcode,
                 item.expected_old_barcode, req.operator),
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


# ---------------------------------------------------------------- 查询

def get_reassignment(
    conn: sqlite3.Connection, reassignment_no: str
) -> s.BarcodeReassignmentResult:
    """按换绑批次号查询首次换绑结果;不存在抛 BarcodeError(reassignment_not_found)。"""
    header = _get_header(conn, reassignment_no)
    if header is None:
        raise BarcodeError([
            _conflict(
                "reassignment_not_found",
                f"换绑批次号 {reassignment_no} 不存在",
                reassignment_no=reassignment_no,
            )
        ])
    return _build_result(header, _load_items(conn, reassignment_no), replayed=False)


def lookup_barcode(
    conn: sqlite3.Connection, barcode: str
) -> s.BarcodeLookupResponse:
    """按条码查询当前绑定的样品;条码未被任何样品绑定时 bound=False。"""
    row = conn.execute(
        "SELECT barcode, sample_id, reassignment_no FROM sample_barcodes"
        " WHERE barcode = ?",
        (barcode,),
    ).fetchone()
    if row is None:
        return s.BarcodeLookupResponse(
            barcode=barcode, bound=False, sample_id=None, reassignment_no=None
        )
    return s.BarcodeLookupResponse(
        barcode=row["barcode"],
        bound=True,
        sample_id=row["sample_id"],
        reassignment_no=row["reassignment_no"],
    )
