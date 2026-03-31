# Siyu 批量自动上传研发拆解清单

> 对应产品方案：`docs/QUEUE_UPLOAD_PRODUCT_DESIGN.md`  
> 当前负责人拍板流程：**选择多个文件 → 文件列表确认/可追加/去重 → 点击解析 → 查看解析结果 → 点击上传**  
> 目标：将产品方案拆解成可执行的研发任务，指导后续代码改造、联调和验收

---

# 0. 拆解原则

本次拆解遵循 4 个原则：

1. **优先保证用户体验闭环**
   - 先让用户能完成“选文件 → 解析 → 上传 → 看结果”完整流程

2. **严格隔离“本次任务”和“历史任务”**
   - 上传按钮必须只作用于本次 session

3. **优先做文件级视图，不把底层 upload_tasks 直接暴露给 UI**
   - UI 看文件状态和汇总进度
   - 底层继续沿用现有 upload_tasks 承载明细任务

4. **边做体验，边补最必要的持久化抽象**
   - 不是纯 UI 包装
   - 要补 session/file 层，否则后面必然返工

---

# 1. 总体实施分期

建议按 3 个阶段推进。

## Phase 1：打通主流程骨架（必须先做）
目标：让系统从“工具台”升级成“批量自动上传工作台”的最小可用版本。

包含：
- 本次文件池
- 多文件添加/追加/去重
- 开始解析
- 解析结果展示
- 开始上传
- 上传结果展示
- 本次 session 隔离

---

## Phase 2：补强状态、恢复与结果能力
目标：解决大文件、长耗时、失败恢复体验。

包含：
- 恢复上次未完成任务
- 文件级重试
- 更细粒度进度
- 更完善结果页

---

## Phase 3：优化与运维增强
目标：提高可维护性、可审计性和交付稳定性。

包含：
- 历史任务列表
- 导出结果报告
- 管理员模式
- 更强调试/审计能力

---

# 2. 模块级研发拆解

---

## 模块 A：数据模型与数据库改造

### 目标
在不推翻现有 `upload_tasks` 的前提下，补出“本次批量任务”和“文件级结果”的持久化基础。

---

### A1. 新增 `batch_sessions` 表

#### 目标
记录一次批量上传任务。

#### 建议字段
- `session_id` TEXT PRIMARY KEY
- `mode` TEXT  -- `parse_then_upload` / `parse_only`
- `status` TEXT -- `CREATED` / `PARSING` / `PARSED` / `UPLOADING` / `COMPLETED` / `PARTIAL_FAILED` / `STOPPED` / `FAILED`
- `total_files` INTEGER
- `parsed_files` INTEGER
- `uploaded_files` INTEGER
- `failed_files` INTEGER
- `total_rows` INTEGER
- `uploaded_rows` INTEGER
- `created_at`
- `started_at`
- `finished_at`
- `last_error`

#### 交付要求
- 增加建表 SQL
- 接入 `init_db()` 的迁移逻辑
- 提供 CRUD / update 方法

---

### A2. 新增 `batch_files` 表

#### 目标
记录 session 中的单个文件。

#### 建议字段
- `file_id` TEXT PRIMARY KEY
- `session_id` TEXT
- `file_path` TEXT
- `file_name` TEXT
- `file_size` INTEGER
- `file_mtime` TEXT
- `file_hash` TEXT NULL
- `file_type` TEXT
- `status` TEXT  -- `PENDING_PARSE` / `PARSING` / `PARSE_SUCCESS` / `PARSE_FAILED` / `READY_TO_UPLOAD` / `UPLOADING` / `UPLOAD_SUCCESS` / `UPLOAD_FAILED` / `STOPPED`
- `parse_rows` INTEGER
- `uploaded_rows` INTEGER
- `parse_error` TEXT
- `upload_error` TEXT
- `current_stage` TEXT
- `created_at`
- `updated_at`

#### 交付要求
- 增加建表 SQL
- 建立 `(session_id)` 索引
- 提供文件状态更新和汇总查询方法

---

### A3. 改造 `upload_tasks`，增加文件归属字段

#### 目标
让底层行任务可以追溯到“本次 session 的哪个文件”。

#### 建议新增字段
- `session_id` TEXT
- `file_id` TEXT
- `source_file_name` TEXT
- `source_file_path` TEXT

#### 交付要求
- 数据库迁移脚本
- 旧数据兼容（旧记录允许为空）
- `insert_task()` 增加写入参数

---

### A4. 去重策略抽象

#### 目标
支持“本次文件池去重”，不是只靠 upload_tasks 去重。

#### 建议规则
第一层：路径相同直接视为重复  
第二层：`file_name + file_size + mtime` 相同视为重复  
第三层（可选）：内容 hash

#### 交付要求
- 新增文件去重工具函数
- UI 添加文件时调用
- 对重复文件返回“跳过原因”

---

## 模块 B：应用层对象与状态聚合层

### 目标
在 UI 和 DB 之间增加“批量任务模型”和“文件状态聚合模型”，避免 UI 直接拼底层表。

---

### B1. 新增 Batch Session Service

#### 职责
- 创建 session
- 追加文件到 session
- 查询 session 摘要
- 汇总成功/失败文件数
- 汇总总解析条数、总上传条数
- 控制 session 状态流转

#### 建议位置
- `siyu_etl/session_service.py`
- 或 `siyu_etl/batch_service.py`

---

### B2. 新增 File Progress Aggregator

#### 职责
把底层 upload_tasks 聚合成文件级结果：
- 一个文件总解析多少条
- 一个文件当前上传多少条
- 文件是否可上传
- 文件当前属于解析中 / 上传中 / 成功 / 失败

#### 交付要求
- 按 `file_id` 聚合
- 能被 UI 定时读取

---

### B3. 新增 Result Summary Builder

#### 职责
生成页面上需要的摘要指标：
- 本次文件总数
- 成功文件数
- 失败文件数
- 总解析条数
- 已上传条数
- 当前正在处理哪个文件

---

## 模块 C：UI 改版

### 目标
把当前 `ui/app.py` 从“操作台 + 日志台”改造成“批量自动上传工作台”。

---

### C1. 重构主页面结构

#### 当前问题
- 已选择文件只是简单文本框
- 日志权重过高
- 主操作语义是工程术语

#### 改造目标
页面结构调整为：
1. 顶部状态条
2. 文件添加区
3. 本次文件列表区（核心）
4. 本次结果摘要区
5. 详细记录区（折叠或降级）

#### 交付要求
- 重构 `_build_ui()`
- 不追求视觉华丽，优先信息清晰

---

### C2. 文件列表组件化

#### 目标
不要再用简单 `Text` 展示文件路径，要有文件级条目。

#### 每个文件项至少展示
- 文件名
- 文件大小
- 文件类型（解析后）
- 当前状态
- 解析条数
- 上传条数
- 错误原因
- 操作按钮：移除 / 查看详情（MVP 可先不做详情弹窗）

#### 交付要求
- Tkinter 内可用 Frame + Label + Button 列表实现
- 支持刷新状态

---

### C3. 调整主按钮逻辑

#### 按钮设计
阶段 1：
- `选择文件`
- `开始解析`

阶段 2：
- `继续添加文件`
- `开始上传`

阶段 3：
- `重试失败文件`
- `重新选择文件`

#### 交付要求
- 删除/弱化“开始处理”“仅推送待上传”旧心智
- 普通用户不再直接面对 `push_only` 这个概念

---

### C4. 增加顶部状态条

#### 展示内容
动态展示：
- 已添加 X 个文件
- 正在解析第 Y/X 个文件
- 正在上传第 Y/X 个文件
- 当前文件较大，请勿关闭窗口
- 本次处理完成：成功 X 个，失败 Y 个

#### 交付要求
- 独立 `StringVar`
- 状态由 session service 驱动

---

### C5. 结果摘要区改造

#### 固定显示指标
- 本次文件数
- 成功文件数
- 失败文件数
- 总解析条数
- 已上传条数
- 当前阶段

#### 交付要求
- 从 session summary 获取数据
- 替代当前 parse_stats / push_stats 的简单文案

---

### C6. 日志区降级

#### 目标
日志保留，但不再是页面核心。

#### 交付要求
- 标题改为“详细记录”或“处理过程”
- 允许默认折叠（若实现成本高，MVP 至少放到页面下部）

---

## 模块 D：文件池与去重逻辑

### 目标
支持一次选择多个文件，也支持解析前继续追加文件，并对重复文件去重。

---

### D1. 引入“本次文件池”内存态模型

#### 目标
替代当前单纯的 `_selected_files: list[Path]`

#### 建议结构
每个文件项包含：
- `file_path`
- `file_name`
- `file_size`
- `mtime`
- `status`
- `dedup_key`

#### 交付要求
- 支持追加
- 支持删除
- 支持清空
- 支持重复检测

---

### D2. 文件添加交互改造

#### 行为要求
- 第一次选择文件：创建本次 session + 加入文件
- 第二次继续添加：追加到当前 session 的文件池
- 遇到重复文件：跳过，并在页面或日志给出提示

#### 提示文案建议
- 已添加 4 个文件
- 已跳过 1 个重复文件

---

## 模块 E：解析流程改造

### 目标
把现有 `_parse_files()` 从“直接面向全局任务表”升级成“面向 session / file 执行解析”。

---

### E1. 新增 `parse_session_files(session_id, ...)`

#### 职责
- 遍历当前 session 下的文件
- 逐个更新文件状态
- 调用现有解析链路
- 写入 upload_tasks 时绑定 `session_id/file_id`
- 汇总 parse_rows

#### 建议调用链
`UI -> app service -> parse_session_files -> detect_sheet/read_rows/identify_row/insert_task`

---

### E2. 解析状态流转

#### 文件级状态
- `PENDING_PARSE`
- `PARSING`
- `PARSE_SUCCESS`
- `PARSE_FAILED`

#### session 级状态
- `CREATED`
- `PARSING`
- `PARSED`
- `PARTIAL_FAILED`

---

### E3. 解析完成后的结果判断

#### 文件级判断
- 若识别失败/结构错误 -> `PARSE_FAILED`
- 若成功解析且条数 > 0 -> `READY_TO_UPLOAD`
- 若解析成功但无有效行 -> 可定义为 `PARSE_FAILED` 或 `READY_TO_UPLOAD(0)`，需统一规则

负责人建议：
- **0 条有效数据默认视为解析失败，并给“文件没有可上传数据”提示**

---

### E4. 解析阶段进度上报

#### 需要上报
- 当前第几个文件 / 共几个文件
- 当前文件已读取多少行
- 当前文件类型识别结果
- 当前阶段（识别中/读取中/清洗中）

#### 交付要求
- 扩展 progress callback 结构
- 不再只传简单文本

---

## 模块 F：上传流程改造

### 目标
上传只作用于“本次 session 中解析成功的文件”，不再默认上传全局 pending。

---

### F1. 新增 `upload_session_files(session_id, ...)`

#### 职责
- 只查询当前 session 下、状态允许上传的任务
- 按 file_id 聚合文件上传进度
- 逐个文件执行上传
- 汇总 uploaded_rows

---

### F2. 改造 `fetch_pending_tasks()`

#### 新增过滤能力
支持按以下维度过滤：
- `session_id`
- `file_id`
- `file_type`

#### 交付要求
- 不破坏现有旧调用
- 新增可选参数

---

### F3. 文件级上传状态流转

#### 文件状态
- `READY_TO_UPLOAD`
- `UPLOADING`
- `UPLOAD_SUCCESS`
- `UPLOAD_FAILED`

#### session 状态
- `PARSED`
- `UPLOADING`
- `COMPLETED`
- `PARTIAL_FAILED`
- `FAILED`
- `STOPPED`

---

### F4. 上传阶段进度上报

#### 页面要能看到
- 正在上传第几个文件
- 当前文件上传了多少条
- 当前文件发送了多少批
- 累计上传多少条

#### 交付要求
- 扩展 uploader -> processor -> UI 之间的进度事件

---

### F5. 严格禁止“上传本次按钮误发历史 pending”

#### 这是验收红线
必须通过测试验证：
- session A 解析的文件，点击上传，只上传 A
- session B 的遗留 pending 不应被误上传

---

## 模块 G：失败重试与恢复

### 目标
支持大文件长耗时场景下的失败恢复，避免全部重来。

---

### G1. 重试失败文件

#### 目标
按钮“重试失败文件”只作用于：
- 本次 session 中解析失败或上传失败的文件

#### 交付要求
- 解析失败文件：支持重新解析
- 上传失败文件：支持重新上传
- 已成功文件不重复处理

---

### G2. 启动时恢复未完成 session

#### 目标
如果上次处理过程中关闭程序，下次打开时提示：
- 检测到上次有未完成任务，是否继续？

#### 交付要求
- 查询最新未完成 session
- 恢复其文件列表和状态

---

### G3. 停止逻辑

#### 目标
停止按钮语义改为：
- **停止本次任务**

#### 交付要求
- 正在解析时可停止
- 正在上传时可停止
- 停止后 session/file 状态正确落库

---

## 模块 H：文案与错误信息改造

### 目标
统一说人话，避免技术术语直接暴露给普通用户。

---

### H1. 主按钮文案替换
旧：
- 开始处理
- 仅推送待上传

新：
- 选择文件
- 开始解析
- 开始上传
- 重试失败文件
- 继续添加文件

---

### H2. 错误文案替换
例如：
- “webhook missing” -> “未配置上传地址，请联系管理员”
- “无法识别文件类型” -> “这个文件不是系统支持的报表，请重新导出后再试”
- “NoResponseStopError” -> “上传接口暂时没有响应，系统已停止后续上传，请稍后重试”

---

### H3. 长耗时提示文案
建议新增：
- 当前文件较大，处理中可能需要较长时间
- 请勿关闭窗口
- 系统会自动继续处理后续文件

---

## 模块 I：测试与验收

### 目标
确保这次改造不是 UI 假象，而是流程和数据边界都正确。

---

### I1. 单元测试
建议新增/补充：
- session/file 表建表与迁移测试
- 文件去重测试
- session 过滤查询测试
- 按 session 上传隔离测试
- 文件级状态聚合测试

---

### I2. 集成测试
关键场景：
1. 一次添加多个文件并解析成功
2. 二次追加文件且重复文件被跳过
3. 解析后只上传本次 session
4. 某个文件解析失败，不影响其他文件继续上传
5. 某个文件上传失败，可单独重试
6. 系统中断后可恢复上次 session

---

### I3. 验收标准（必须）

#### 验收项 1：文件池体验
- 可一次选择多个 Excel
- 可继续追加文件
- 可识别并跳过重复文件

#### 验收项 2：解析体验
- 点击“开始解析”后，有整体进度与当前文件进度
- 解析后能看到文件类型、总解析条数、是否可上传

#### 验收项 3：上传边界
- 点击“开始上传”只上传本次解析成功文件
- 不误发历史 pending

#### 验收项 4：结果清晰
- 页面能看到成功/失败文件
- 页面能看到总解析条数、总上传条数
- 页面能看到当前上传进度

#### 验收项 5：恢复与重试
- 失败文件可以重试
- 已成功文件不会重复上传

---

# 3. 建议的代码改造落点

## 优先修改文件
1. `siyu_etl/db.py`
2. `siyu_etl/processor.py`
3. `siyu_etl/scheduler.py`
4. `siyu_etl/uploader.py`
5. `siyu_etl/ui/app.py`

## 建议新增文件
1. `siyu_etl/batch_service.py`
2. `siyu_etl/session_models.py`（可选）
3. `siyu_etl/file_dedup.py`（可选）
4. `tests/test_batch_session_flow.py`
5. `tests/test_file_dedup.py`

---

# 4. 实施顺序建议（负责人拍板）

## 第一步：先做数据层和服务层
- batch_sessions
- batch_files
- upload_tasks 关联字段
- batch service

## 第二步：再改 UI 主流程
- 文件池
- 文件列表
- 开始解析 / 开始上传
- 状态条 / 结果摘要

## 第三步：接入解析和上传流程
- parse_session_files
- upload_session_files
- session 过滤上传

## 第四步：补恢复与重试
- 恢复未完成 session
- 失败文件重试

## 第五步：补测试与文案
- 集成测试
- 用户文案收敛

---

# 5. 一句话总结

这次研发改造不是在当前页面上“多加一个多选按钮”，而是要把项目升级成一个：

# 以“本次文件”为中心、支持多文件追加与去重、先解析后上传、能承接大文件长耗时场景的批量自动上传工作台。

实现上最关键的 3 件事是：
1. 补 `session/file` 持久化抽象
2. 上传严格按 session 隔离
3. UI 改成文件级进度与结果视图

这 3 件事做对了，这次升级才算真正完成。
