# Siyu ETL Client

一个本地单机的 ETL + 推送工具：解析大众点评导出的 Excel 数据，严格清洗、业务主键去重，按门店分组分片（100 条/包）推送到各自 webhook，并支持重试/跳过/熔断。

---

## 系统要求

- **Python**: 3.10 或更高版本
- **操作系统**: 
  - Windows 10/11
  - macOS 10.15 或更高版本
  - Linux (Ubuntu 20.04+, CentOS 7+)
- **内存**: 建议 4GB 以上
- **磁盘空间**: 至少 100MB（用于数据库和临时文件）

---

## 功能概览

- **支持 6 张表**：
  - 会员交易明细
  - 店内订单明细(已结账)
  - 收入优惠统计
  - 优惠券统计表
  - 会员储值消费分析表
  - 会员卡导出

- **智能表头识别**：根据文件名自动识别表类型，无需手动配置
- **严格数据清洗**：空值与零值区分、Excel 序列日期转 `YYYY-MM-DD HH:mm:ss`、百分比转小数
- **本地 SQLite 去重**：基于业务主键（fingerprint）去重，避免重复推送
- **智能分组推送**：按门店分组 + 时间升序 + 100 条/包
- **容错机制**：2s/5s/10s 重试 3 次、失败跳过、连续 5 个 batch 失败熔断（可重置）
- **友好 UI**：Tkinter + ttkbootstrap（拖拽可选，缺依赖自动降级为"选择文件"）

---

## 快速开始

### 1. 安装依赖

```bash
# 创建虚拟环境
python -m venv .venv

# 激活虚拟环境
# Linux/macOS:
source .venv/bin/activate
# Windows:
.venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置环境变量（生产环境必须）

⚠️ **重要**：生产环境必须通过环境变量设置 `SIYU_PLATFORM_KEY`，不要使用代码中的默认值。

**Linux/macOS**:
```bash
export SIYU_PLATFORM_KEY="your-platform-key-here"
```

**Windows (PowerShell)**:
```powershell
$env:SIYU_PLATFORM_KEY="your-platform-key-here"
```

**Windows (CMD)**:
```cmd
set SIYU_PLATFORM_KEY=your-platform-key-here
```

### 3. 启动应用

```bash
python main.py
```

### 4. 使用步骤

1. **选择推送模式**：
   - **预演模式（推荐首次使用）**：不发送请求，仅预览推送内容
   - **真实推送**：真实发送数据到 webhook

2. **添加文件**：
   - 拖拽 Excel 文件到拖拽区（如果系统支持）
   - 或点击"选择 Excel 文件"按钮

3. **开始处理**：
   - 点击"开始处理"：解析文件、清洗数据、去重入库（不推送）
   - 点击"仅推送待上传"：推送数据库中待推送的数据

4. **查看结果**：
   - 查看"本次结果"面板了解处理统计（解析行数、插入数、重复数等）
   - 查看日志框了解详细处理过程

5. **配置 Webhook（可选）**：
   - 如果需要修改 webhook URL，编辑 `siyu_etl/config.py` 中的 `Webhooks` 类
   - 或通过 UI 中的"保存配置"按钮保存 platformKey

---

## 使用示例

### 示例 1：首次使用（预演模式）

1. 启动应用
2. 选择"预演（不发送请求）"模式
3. 拖拽或选择 Excel 文件
4. 点击"开始处理"
5. 查看"本次结果"面板，确认解析结果
6. 点击"仅推送待上传"查看推送预览（前 3 个批次）
7. 确认无误后，切换到"真实推送"模式重新操作

### 示例 2：批量处理多个文件

1. 选择多个 Excel 文件（支持批量选择）
2. 点击"开始处理"
3. 等待所有文件处理完成
4. 查看统计信息（解析行数、插入数、重复数）
5. 点击"仅推送待上传"推送所有待推送数据

### 示例 3：断点续传

1. 如果推送过程中断（网络问题、熔断等）
2. 修复问题后，直接点击"仅推送待上传"
3. 系统会自动从上次中断的地方继续推送（只推送 PENDING 状态的数据）

---

## 配置说明

### 环境变量配置

**生产环境必须设置**：

```bash
export SIYU_PLATFORM_KEY="your-platform-key-here"
```

如果不设置环境变量，将使用代码中的默认值（**仅用于开发/测试**）。

### Webhook URL 配置

如果需要修改 webhook URL，编辑 `siyu_etl/config.py` 文件：

```python
@dataclass(frozen=True)
class Webhooks:
    member_trade_detail: str = "https://your-webhook-url-here"
    income_discount_stat: str = "https://your-webhook-url-here"
    # ... 其他 webhook
```

### 配置文件位置

- 用户配置：`siyu_etl_config.json`（自动保存用户设置）
- 数据库：`siyu_etl.sqlite3`（存储任务数据）

---

## Windows 一键运行（免安装）

若需在 Windows 上**无需安装 Python、双击即用**，可使用自动构建好的安装包。

### 获取 Windows 包

1. 打开本仓库的 **Actions** 页面，进入最近一次 **Build Windows** 工作流运行。
2. 在 **Artifacts** 中下载 **siyu-etl-windows**（zip）。
3. 解压到任意目录，得到文件夹 **SiyuETL**，内含 `SiyuETL.exe` 和 `_internal/`。

### 使用方式

- **双击 `SiyuETL.exe`** 即可启动客户端，无需安装 Python 或任何依赖。
- 若需自定义配置，将 **siyu_etl_config.json** 放在与 `SiyuETL.exe` 同一目录；首次运行也可在界面中配置并保存。
- 配置与数据库（`siyu_etl.sqlite3`）会保存在 exe 所在目录。

### 说明

- 未签名的 exe 可能被 Windows SmartScreen 拦截，需选择「更多信息」→「仍要运行」。
- 构建由 GitHub Actions 在 push 到 `main` 时自动触发，也可在 Actions 页手动运行 **Build Windows** workflow。

---

## 故障排查

### 常见问题

#### 1. 文件被占用错误

**错误信息**：
```
无法读取文件 xxx.xlsx：文件可能正在被其他程序（如 Excel）打开。
```

**解决方法**：
- 关闭 Excel 或其他正在打开该文件的程序
- 确保文件没有被其他进程锁定
- 重新尝试

#### 2. 网络超时/连接失败

**错误信息**：
```
REQUEST_TIMEOUT: ...
CONNECTION_ERROR: ...
```

**解决方法**：
- 检查网络连接
- 检查 webhook URL 是否正确（在 `config.py` 中）
- 检查防火墙设置
- 如果持续失败，会触发熔断器（连续 5 次失败后）

#### 3. 熔断器触发

**错误信息**：
```
已熔断: 会员交易明细 / 山禾田·日料小屋（龙华店）
```

**解决方法**：
- 点击"重置"按钮清除熔断状态
- 检查网络和服务端状态
- 确认 webhook URL 正确

#### 4. 数据库锁定

**错误信息**：
```
database is locked
```

**解决方法**：
- 确保没有多个实例同时运行
- 关闭应用后重新启动
- 如果问题持续，删除 `siyu_etl.sqlite3` 重新开始（会丢失历史数据）

#### 5. 文件格式不支持

**错误信息**：
```
无法识别表类型: xxx.xlsx
```

**解决方法**：
- 确认文件是支持的 6 种类型之一
- 检查文件名是否包含关键词（如"会员交易明细"、"店内订单明细"等）
- 确认文件是 `.xlsx` 格式（不是 `.xls`）

### 获取帮助

如果问题仍未解决，请：
1. 查看日志框中的详细错误信息
2. 检查 `siyu_etl.sqlite3` 数据库中的任务状态
3. 查看 `docs/TROUBLESHOOTING.md` 获取更多故障排查信息

---

## 目录结构

```
siyu/                      # 项目根目录
├── main.py                 # 程序入口
├── requirements.txt        # Python 依赖
├── siyu.spec               # PyInstaller 打包配置（Windows）
├── siyu_etl_config.json    # 用户配置（自动生成）
├── README.md
│
├── siyu_etl/               # 核心 ETL 包
│   ├── __init__.py
│   ├── archive.py          # 文件归档
│   ├── circuit_breaker.py  # 熔断器
│   ├── cleaner.py          # 数据清洗
│   ├── config.py           # 配置定义
│   ├── constants.py        # 常量定义
│   ├── db.py               # 数据库操作
│   ├── excel_detect.py     # Excel 文件类型检测
│   ├── excel_read.py       # Excel 数据读取
│   ├── fingerprint.py      # 指纹生成
│   ├── processor.py        # 处理流程
│   ├── scheduler.py        # 任务调度
│   ├── settings.py         # 配置管理
│   ├── uploader.py         # 数据上传
│   └── ui/                 # 用户界面
│       ├── app.py          # 主应用窗口
│       ├── config_dialog.py
│       └── dnd.py          # 拖拽支持
│
├── data/                   # 样例/测试用 Excel 文件
├── docs/                   # 设计文档、评审、故障排查等
├── tests/                  # 单元测试与集成测试（pytest）
│   ├── conftest.py         # 测试配置与路径
│   └── test_*.py
├── scripts/                # 调试与一次性脚本
│   ├── debug_actual_data.py
│   └── debug_store_id_issue.py
```

---

## 开发说明

### 运行测试

```bash
pytest tests/
```

### 代码规范

- 使用 `black` 格式化代码（如果配置）
- 使用类型注解
- 遵循 PEP 8 规范

---

## 许可证

[根据实际情况填写]

---

## 更新日志

### v1.0 (2026-01-28)

- 初始版本发布
- 支持 6 种数据源
- 完整的 ETL 流程
- 友好的图形界面
- 容错和重试机制

---

## 联系方式

[根据实际情况填写]
