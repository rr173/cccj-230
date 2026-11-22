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
netting/           逐笔核销事实表（只读闭合绑定，独立 SQLite 库，见第 9 节）
  engine.py        纯账户内配对引擎：过期、部分抵扣、entry_id 次序、重放
  csv_detail.py    detail.csv 严格校验（UTF-8、表头、ID/kind/金额、日内唯一）
  source.py        按指定 (business_date, revision) 只读 CLOSED 绑定的 detail.csv
  store.py         独立 SQLite：选版、日快照、检查点、事实表、事实修订边界标记、审计日志（WAL）
  service.py       应用指定版本、受影响账户增量重算、事实读取、全量重放核对
  cli.py           独立进程入口（python -I netting/cli.py --args args.json）
  errors.py        核销侧异常类型
posting/           核销过账交接（与核销库同一 SQLite 文件，见第 11 节）
  store.py         交接表、facts 变更触发器、批次/流水/预览/在途正向流水（WAL）
  service.py       预览、确认（作废判定/幂等）、按批次号读取、首次启用引导
  cli.py           独立进程入口（python -I posting/cli.py --args args.json）
  errors.py        交接侧异常类型
tests/             合同/场景/流式/真实多进程/核销/过账测试
demo.py            全流程演示
netting_demo.py    核销事实表端到端演示（回填、更正、坏版隔离、前缀/旁路）
posting_demo.py    过账交接端到端演示（冲回/新登记、预览作废、幂等、不扫历史）
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

## 9. 逐笔核销事实表（`netting/`）

在闭合服务之上维护一张可持续更新、可更正的逐笔核销事实表。它**只读**闭合
库与内容寻址 blob，绝不修改原闭合库、到货文件或领取状态；全部自有状态写入
一个独立的持久 SQLite 库。

### 9.1 取数约定（不猜版本、不找同名文件）

每次应用都由调用方明确给出业务日和修订号：

1. 用闭合服务同一套批次身份函数解析该合同下该 `business_date` 的批次；
2. 只读取该批次**指定修订号**、状态恰为 `CLOSED` 的闭合行（`CLOSED_PARTIAL`、
   不存在的修订号一律拒绝）；
3. 从该行的**绑定摘要**取 `detail.csv` 的 blob sha256，再读内容寻址 blob 并
   重新哈希校验；不去到货目录找同名文件，也不默认读最新版；
4. 绑定为压缩 payload（如 gzip）或 blob 缺失/哈希不符时拒绝应用。

明细只接受未压缩 UTF-8 CSV，表头固定为 `entry_id,account_id,kind,cents`；
日内 `entry_id` 唯一，两个 ID 为非空 ASCII 字母数字串，`kind` 仅 `bill`/
`payment`，`cents` 为不超过 10^12 的正整数。只有表头表示当天无记录，不代表
文件缺失。任何不符都在写库**之前**拒绝，已发布结果不变。事件身份为
`业务日 + entry_id`，行序不代表处理顺序。

### 9.2 核销规则

* 只在同一 `account_id` 内核销，允许部分抵扣；单笔累计核销不超过原金额。
* 按业务日推进，每日顺序：先丢弃已过期的未用到账余额 → 纳入当天两类记录 →
  从最早未清账单与最早可用到账开始配对。
* 到账可用于到账日至其后两个自然日（含两端），也可抵更早的欠款；余额过期
  不撤销已完成的核销，未清账单不过期。
* 同日先后按各自 `entry_id` 的 ASCII 顺序，绝不使用 CSV 物理行序。
* 事实表保存每次配对的账户、核销日、账单事件身份、到账事件身份与抵扣金额（分）。

### 9.3 更正安全性（核心）

各日可任意次序加入。同一天新版应用成功后旧版不能回压（抛 `StaleRevision`），
同版重试幂等不产生重复。对日期 D 的每次应用，结果与“把所有已选日版本从头按
日期重算”一致，但只重算 D 日行级增删改**实际涉及的账户**（账户迁移算旧、新
两边；快照逐字节相同的旁路账户完全不触碰），且：

* 从该账户 D 日之前**最新的未清状态检查点**恢复，绝不重放 D 日以前的配对；
* 只重放已持久化的逐日快照，不重新打开闭合服务、不遍历其他日期的源文件；
* 只删除并重写该账户 `match_day >= D` 的事实与检查点，未受影响账户的事实
  连 rowid 都不变；
* 被删账单的旧配对消失，原来占用的到账按规则改抵之后的账单（见演示第 2 节）。

### 9.4 持久化与并发

独立 SQLite 库（WAL、`synchronous=FULL`、写连接 `BEGIN IMMEDIATE`）含：
`applied_days`（每选用版）、`entries`（不可变逐日快照）、`facts`（事实表）、
`checkpoints`（可恢复未清状态）、`apply_log`（含拒绝的审计）、
`fact_revisions`（每次真正重写事实的应用边界标记，供本地过账交接使用，
不参与任何核销决策）。写连接负责事务内
读写（能见到本事务未提交改动），另开只读连接服务已发布查询（不长期钉住 WAL
读位点）。一次应用是单事务：并发应用串行化、旧版压不回新版、不会丢掉另一日已
提交变更，读者不可能看到“新版号配旧事实”。重开库即可继续查询与更新。

### 9.5 接口与示例

```python
from netting import NettingService

# 复用已打开的闭合服务句柄，或传 closure_data_dir 让其自建只读句柄
net = NettingService("/path/to/netting.db", closure=closure_service)

# 明确指定业务日 + 修订号；坏绑定/坏 CSV/旧版会抛 netting.errors.* 且不改已发布结果
net.apply_revision(business_day="2026-11-03", revision=2,
                   contract_id="daily-settle", contract_version="1")

net.read_facts()                          # 全部事实（含 bill_event/payment_event）
net.read_facts(account_id="A")           # 按账户过滤
net.read_facts(business_day="2026-11-03")  # 按核销日过滤
net.selected_revisions()                 # 各业务日当前选用的修订号
net.application_log()                    # 应用/幂等/拒绝审计
net.full_replay()                        # 独立全量重放，用于核对增量结果
net.open_state_checkpoint("A", "2026-11-03")  # 查看可恢复未清状态
```

命令行（参数走 JSON 文件，适合多进程/调度器）：

```bash
python3 -I netting/cli.py --args apply.json     # apply / read / revisions / log / replay
```

## 10. 核销演示与测试

```bash
python3 -I netting_demo.py                 # 回填→更正→坏版隔离→前缀/旁路 全流程
python3 -I tests/test_netting_engine.py   # 纯引擎：部分抵扣/到期/次序/累计上限
python3 -I tests/test_netting_csv.py      # detail.csv 全部合法性校验
python3 -I tests/test_netting_service.py  # 更正重定向、删账单、账户迁移、重开恢复
python3 -I tests/test_netting_multiprocess.py  # 真实多进程并发与读者一致性
```

测试数据全部运行时通过闭合服务临时生成（临时目录），不提交大型数据文件。

### 10.1 核销场景与需求对照

| 场景 | 测试 |
| --- | --- |
| 后导入的账先入、再补前面日批次，结果同从头重算 | `test_out_of_order_apply_matches_from_scratch_replay` |
| 跨日部分抵扣、到账到期、删账单旧配对消失、到账改抵后续账单 | `test_correction_redirects_payment_and_drops_deleted_bill_facts` |
| 表头-only 修订删除整日及其配对 | `test_header_only_correction_deletes_whole_day_and_its_pairings` |
| 账户迁移旧、新两边都重算 | `test_account_move_recomputes_both_old_and_new_account` |
| 同版重试幂等、旧版不能回压新版 | `test_same_revision_retry_is_idempotent_and_older_revision_rejected` |
| 坏修订（未知修订/未闭合/坏 CSV/压缩/坏日期）不改已发布结果 | `test_bad_revisions_do_not_change_published_results` |
| `CLOSED_PARTIAL` 绑定被拒绝 | `test_closed_partial_revision_is_rejected` |
| 长旧前缀不重放、旁路账户事实不重写 | `test_old_prefix_is_not_replayed_and_bypass_account_is_untouched` |
| 重开库可查询并继续更新 | `test_reopen_database_preserves_and_continues_state` |
| 并发不同日互不丢变更、竞态下旧版压不回、读者一致性 | `test_netting_multiprocess.py` 三个用例 |
| 到账两日窗口、到期不撤销已核销、ASCII 次序、累计不超额 | `test_netting_engine.py` |

## 11. 核销过账交接（`posting/`）

在核销库之上做**本地**过账交接：财务先预览一批应交付流水，再确认发布；
发布后的批次按批次号反复读取。它不连接任何外部总账、不改任何核销配对规则，
所有表与触发器都建在核销库**同一个 SQLite 文件**里，因此一次核销应用与其
变更记录、一次批次确认与其流水/水位都在同一事务内原子提交。

### 11.1 配对标识与流水语义

配对标识为「账户 + 核销业务日 + 账单事件身份 + 到账事件身份」，事件身份是
「原业务日 + `entry_id`」；金额一律整数分，数据库 rowid 不是业务身份。
预览比较当前核销事实与上次交接结果：

* 新出现的配对 → 正向流水（`forward`）；
* 消失的配对 → 全额冲回（`reversal`），冲回行显式指向仍有效的那条正向
  流水编号（外键保证目标存在且未被冲过，不能指向已冲销版本）；
* 同一配对金额变化 → 先全额冲回旧金额、再登记新金额，**不记净差**；
* 配对移到另一核销日或换掉任一事件身份 → 按旧配对消失 + 新配对出现处理，
  即使合计金额不变；
* 两次交接之间出现又消失、最终没有留下变化的配对 → 不出流水。

过账日独立于核销业务日：可与上一批相同，但不得早于上一批的过账日；更正
登记在本次过账日上，每条流水仍保留原配对的业务日期。

### 11.2 增量边界与「业务变更」判定（不扫历史）

`posting` 在核销 `facts` 表上挂两个只追加的触发器，把每次物理删/插逐行
写入 `fact_change_log`；核销服务在每个真正重写事实的应用事务内写一条
`fact_revisions` 边界标记（同版重试与被拒绝的应用走不到重写路径，没有
标记）。每个标记的变更行按多重集差分化简：

* 完全相同的「删除 + 插入」两两抵消（只重写数据库行、配对与金额没变，
  即便修订号变了）→ 业务变更时钟 `fact_clock` **不动**，预览不作废；
* 有非空差分的应用（新配对/消失/改金额）→ 时钟 +1；每个应用单独评估，
  所以「改了又改回」仍是两次业务变更，夹在预览与确认之间照样作废旧预览；
* 失败应用整体回滚，触发器副作用一起回滚，时钟不动。

除首次启用的一次性引导外，预览/确认只读 `fact_change_log` 中超过水位的
行，并对涉及身份做索引点查，从不扫描未受本轮变化影响的历史配对或已发布
流水；按批次号读取不受此限。首次启用可直接在已有核销库上从现存事实建立
初始待交接集合，无需重新导入原文件。

### 11.3 预览/确认协议与并发

* 预览只追加 `posting_previews` 行，不移动任何交接水位；没有待交接流水时
  抛 `NothingToPost`，绝不发布空批。
* 确认只能发布该预览的内容。确认时若时钟较预览基址变化（任何一次真正
  改变事实的应用，即使随后改回），或已有另一个预览先确认（批次号基址
  变化），该预览作废（标记 `superseded`）并抛 `PreviewStale`，提示重新
  预览；纯行重写、仅修订号变化、失败应用都不会作废预览。
* 已确认预览的重试幂等：返回同一批次、同一批流水号，不再写任何流水；
  之后的更正不影响它，只属于下一批。
* 批次号稳定递增，批内 `entry_no` 稳定，同一配对的冲回恒在新登记之前；
  批次一经确认不可改写，读者只看见整批已确认流水，并能凭冲回行追溯原
  正向流水与原配对身份。
* WAL + 写连接 `BEGIN IMMEDIATE` 串行化并发应用与确认：旧预览不会被当成
  新结果发布，并发确认只有一个赢家、不会重复出账。关闭进程重开后已发布
  批次可读、未确认预览仍按同一规则确认或拒绝。

### 11.4 接口与示例

```python
from netting import NettingService
from posting import PostingService, PreviewStale, NothingToPost

# 与核销库同一个 SQLite 文件；首次打开即在现存事实上完成引导准备
net = NettingService("/path/to/netting.db", closure=closure_service)
post = PostingService("/path/to/netting.db")

preview = post.preview(posting_day="2026-11-03")   # 不改交接状态
# preview["entries"]: forward/reversal 行，reversal 带 reverses_flow_id
batch = post.confirm(preview["preview_id"])        # 发布不可变批次
post.read_batch(batch["batch_no"])                 # 按批次号反复读取
post.list_batches()                                # 批次头列表
post.handoff_state()                               # 交接水位/时钟快照
```

命令行（参数走 JSON 文件，便于多进程/调度器）：

```bash
python3 -I posting/cli.py --args preview.json      # preview/confirm/read_batch/
python3 -I posting/cli.py --args confirm.json      # batches/state
```

### 11.5 演示与测试

```bash
python3 -I posting_demo.py                  # 60/40→30/70、先改后交接、预览作废、
                                            # 重试幂等、无关历史不扫描
python3 -I tests/test_posting_service.py    # 17 个用例（含真实多进程并发）
```

| 场景 | 测试 |
| --- | --- |
| 总额不变的 30/70 更正仍交付两冲回两新登记，冲回指向原流水 | `test_equal_total_correction_delivers_two_reversals_and_two_forwards` |
| 先改再首次交接只登记当前两笔 | `test_correction_before_first_handoff_registers_current_pairings_only` |
| 更正夹在预览/确认之间（含改回）→ 旧预览拒绝、重预览正确、重试不重复写 | `test_correction_between_preview_and_confirm_rejects_then_repreview` |
| 无关历史配对不被后续交接扫描 | `test_unrelated_history_is_not_scanned_on_later_handoffs` |
| 消失全额冲回、身份/核销日迁移按消失+出现处理、闪烁不留痕 | `DeltaSemanticsTests` |
| 纯行重写/失败应用/同版重试不作废预览 | `test_row_only_rewrite_and_failed_application_do_not_stale_preview` |
| 过账日次序、空批不发布、重开恢复、现存库引导 | `ProtocolTests` |
| 并发确认唯一赢家、跨进程应用与确认不发布旧事实 | `ConcurrencyTests` |


