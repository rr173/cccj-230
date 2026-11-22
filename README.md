# 每日结算批次闭合服务

根据声明式到货合同判断批次何时可进入处理、缺件状态、迟到文件如何形成新修订，
并产出可重放的闭合证据。纯 Python 标准库 + SQLite 实现，无第三方依赖。

## 1. 领域规则

* **批次身份**：合同摘要 + 批次键（含业务日）决定 `batch_id`，同一身份的到货归并到同一批次。
* **闭合时机**：截止时间之前，无论到了多少文件都只能是 `WAITING`，显式评估也不会提前闭合；
  截止之后才产生首个闭合决定。
* **缺件语义**：
  * 截止前缺件 → `WAITING`（等待，不拒收）；
  * 截止后、迟到窗口内仍缺必需件 → `reject` 策略继续等待，`publish_with_missing`
    策略立即产生 `CLOSED_PARTIAL`；
  * 迟到窗口结束后仍缺 → `reject` 产出 `REJECTED`（不可领取），
    `publish_with_missing` 产出/保持 `CLOSED_PARTIAL`。
* **迟到修订**：窗口内同名字段内容变化（blob sha256 不同）不覆盖旧内容，
  而是保留旧 blob、记录 `duplicate_member` 审计并在下次评估时生成修订号 +1，
  并给出影响范围（added / removed / changed）。窗口之后的到货一律隔离为
  `late_arrival`，不能改写任何既有修订。
* **幂等**：同名同内容重送只新增一条指向同一 blob 的到货记录，不改变到达顺序、
  绑定摘要或闭合结果。
* **失败分类**（各自独立的审计码，互不混淆）：
  `manifest_missing`、`digest_mismatch`、`duplicate_member`、`size_exceeded`、
  `unsupported_compression`、`compression_mismatch`、`compression_corrupt`、
  `path_escape`、`unknown_member`、`manifest_malformed`、`manifest_duplicate`、
  `summary_malformed`、`late_arrival`、`rejected_missing`。
* **路径安全**：成员路径必须词法位于到货根目录内，绝对路径、`..` 穿越、NUL 字节
  在触碰文件系统前拒绝；清单内声明的成员路径同样校验。
* **领取约束**：只有 `CLOSED` / `CLOSED_PARTIAL` 修订可领取；令牌用 HMAC 绑定
  合同摘要与成员绑定摘要；同一修订只能被领取一次；旧修订仍在处理时，新修订
  不能被领取（旧输入 blob 与旧绑定物理不可变）。
* **可恢复**：SQLite WAL + `synchronous=FULL`，每次接收/决定独立提交；
  blob 按内容寻址、只写一次。进程退出后重新打开同一目录即可恢复全部状态。

## 2. 模块结构

```
batch_closure/
  contract.py      合同编译器：校验、规范化、规范 JSON、合同摘要、截止/窗口计算
  hashing.py       分块流式哈希（64KiB）、大小上限中断、gzip 解压流哈希、压缩嗅探
  paths.py         路径越界检查（词法 + resolve 双重）
  store.py         SQLite 模式、事件日志、到货/校验/闭合/领取表、内容寻址 blob 存储
  service.py       接收、状态查询、闭合/修订评估、领取/令牌、证据导出、重放
  state_machine.py 独立参考状态机（独立重写规则，仅共享纯工具），用于交叉核对
  evidence.py      证据包独立校验器（blob 重哈希、哈希链、合同摘要重导）
  cli.py           独立进程驱动入口（python -I cli.py --args args.json）
  errors.py        异常类型
tests/             合同/场景/流式/真实多进程测试
demo.py            全流程演示
smoke_test.py      冒烟脚本
```

## 3. 合同声明（示例）

```json
{
  "contract_id": "daily-settlement",
  "version": "2026-11",
  "batch_key": ["business_date"],
  "business_date_field": "business_date",
  "max_file_size": 67108864,
  "deadline": {"type": "relative", "offset_days": 1, "hour": 12, "minute": 0},
  "late_window_minutes": 60,
  "on_missing_required": "publish_with_missing",
  "supported_compression": ["gzip"],
  "members": {
    "manifest.json": {"required": true, "role": "manifest",
                      "digest_algorithms": ["sha256"]},
    "detail.csv":    {"required": true, "role": "data",
                      "digest_algorithms": ["sha256"], "compression": "gzip"},
    "control.json":  {"required": false, "role": "summary"}
  }
}
```

截止也支持绝对时间：`{"type": "absolute", "at": "2026-12-01T08:30:00Z"}`。
编译结果的 `digest` 是规范化合同的 sha256，所有闭合绑定与领取令牌都绑定该摘要；
合同任何字段（含版本号）变化都会产生不同摘要。

## 4. 服务接口

```python
from batch_closure import BatchClosureService, compile_contract

contract = svc.compile_contract(declaration)          # 合同编译并持久化
svc.load_contract(contract_id, version)               # 恢复已存合同

svc.receive_file(contract, key, member, source_path,  # 文件接收（流式）
                 expected_digests={"sha256": "..."})
# -> {"accepted": bool, "idempotent": bool, "revision_candidate": bool,
#     "quarantine_reason": str|None, "findings": [...], "blob_sha256": ...}

svc.get_status(contract, key)                         # 状态/等待/缺件/隔离/闭合历史
svc.evaluate_closure(contract, key)                   # 闭合或修订决定（幂等评估）
svc.claim(contract, key, processor="worker-1")        # 领取最新已闭合修订
svc.verify_token(token)                               # 校验令牌绑定
svc.complete_claim(token)                             # 完成/失败处理
svc.export_evidence(contract, key, out_dir)           # 证据包导出
svc.replay(contract, key)                             # 独立状态机重放核对
```

业务校验失败不抛异常，而是作为隔离到货 + 校验事件持久化并在返回值中体现；
编程错误（未知合同、领取未闭合修订等）才抛 `errors.py` 中的异常。

## 5. 闭合证据

`export_evidence` 产出的目录：

```
contract.json            规范化合同（可重新编译出相同摘要）
events.json              完整事件日志（batch_opened/file_received/
                         file_quarantined/closure_decided/claim_*）
closures.json            每个修订的绑定、签名、影响范围、证据哈希链
arrivals.json            全部到货尝试（含隔离行）
validation.json          分类校验事件
claims.json              领取记录（不含令牌原文以外的敏感字段）
blobs/<前2位>/<sha256>   所有被引用文件的内容寻址副本
evidence_manifest.json   清单（含每个 blob 校验结果、链完整性）
```

每个修订含 `prev_evidence_hash`，形成只增哈希链；`evidence.verify_bundle`
可在没有服务、没有数据库的情况下独立校验整个包。篡改任何 blob 或闭合记录
都会被检出（测试 `test_evidence_chain_breaks_under_tampering` 覆盖）。

## 6. 独立状态机交叉核对

`state_machine.py` 不调用服务的任何转换代码，只读取事件日志并按事件时刻
（外加截止时刻、窗口结束时刻）独立推演状态序列与修订集合；`service.replay`
用同一 HMAC 密钥比较两边的修订号、状态与绑定签名，并检查证据哈希链。
所有场景测试都强制要求交叉核对一致。

## 7. 运行测试

```bash
# 全部以隔离模式运行（-I 忽略环境变量与用户站点包）
python3 -I tests/test_contract.py
python3 -I tests/test_scenarios.py      # 18 个手工场景 + 状态机逐项核对
python3 -I tests/test_streaming.py      # 流式哈希/内存上限
python3 -I tests/test_multiprocess.py   # 真实多进程交错接收/领取
python3 -I demo.py
```

`test_multiprocess.py` 的每个 CLI 调用都是独立 `python -I` 进程，共享同一
SQLite 库与 blob 目录，覆盖：并发接收串行化、未闭合不可领取、领取竞争只有
一个赢家、旧修订处理中新修订被阻塞、旧令牌对旧绑定持续有效、完成后才能
领取新修订、崩溃后已提交数据不丢。

## 8. 手工场景与需求对照

| 场景 | 测试 |
| --- | --- |
| 必需文件先后到达、不提前闭合 | `test_s1_required_sequence_no_early_closure` |
| 可选文件迟到生成修订与影响范围 | `test_s2_optional_late_creates_revision_with_impact` |
| 摘要错误隔离且缺件保持 | `test_s3_digest_mismatch_recorded_separately` |
| 同名内容变化形成修订不覆盖 | `test_s4_content_change_forms_revision_not_overwrite` |
| 截止前后缺件（等待→拒收） | `test_s5_missing_before_deadline_waiting_after_window_rejected` |
| 允许带缺件发布合同 | `test_s6_publish_with_missing_contract` |
| gzip 正常/格式不符/损坏/不支持格式 | `test_s7_compression_rules` |
| 路径穿越/绝对路径 | `test_s8_path_escape_rejected` |
| 清单内重复成员 | `test_s8b_duplicate_manifest_entries_recorded` |
| 大小超限（流式中断） | `test_s9_size_limit_enforced_while_streaming` |
| 旧修订处理中收到新修订 | `test_s10_new_revision_while_old_is_processing` |
| 同内容重送幂等 | `test_idempotent_reception_same_content` |
| 摘要成员驱动摘要校验 | `test_summary_member_drives_digest_verification` |
| 证据自校验与篡改检出 | `test_evidence_bundle_is_self_verifying` / `..._tampering` |
| 进程退出恢复 | `test_recovery_after_reopen_preserves_decisions` / 多进程崩溃测试 |
