"""
图形用户界面应用程序模块

该模块实现了基于 tkinter/ttkbootstrap 的图形用户界面，提供：
1. 文件选择和拖拽功能
2. 解析和推送控制
3. 进度显示和日志输出
4. 配置管理
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import tkinter as tk
import ttkbootstrap as tb
from ttkbootstrap.constants import BOTH, BOTTOM, END, LEFT, RIGHT, TOP, X, Y
from ttkbootstrap.dialogs import Messagebox

from siyu_etl.circuit_breaker import CircuitBreaker
from siyu_etl.config import DEFAULT_CONFIG, AppConfig
from siyu_etl.constants import DEFAULT_CIRCUIT_BREAKER_THRESHOLD
from siyu_etl.db import clear_all_tasks
from siyu_etl.excel_detect import (
    FILETYPE_COUPON_STAT,
    FILETYPE_INCOME_DISCOUNT,
    FILETYPE_INSTORE_ORDER,
    FILETYPE_MEMBER_CARD_EXPORT,
    FILETYPE_MEMBER_STORAGE,
    FILETYPE_MEMBER_TRADE,
)
from siyu_etl.processor import parse_only, push_only
from siyu_etl.settings import load_config, save_config
from siyu_etl.ui.config_dialog import WebhookConfigDialog
from siyu_etl.ui.dnd import create_dnd_window_or_none, detect_dnd_support, register_drop_target


@dataclass(frozen=True)
class ProgressSnapshot:
    """
    进度快照数据类
    
    Attributes:
        current: 当前进度
        total: 总进度
        message: 进度消息
    """
    current: int
    total: int
    message: str = ""


class UiBus:
    """
    UI 消息总线
    
    用于在工作线程和主 UI 线程之间传递日志和进度信息。
    使用队列实现线程安全的消息传递。
    """
    
    def __init__(self) -> None:
        """初始化消息总线，创建日志和进度队列"""
        self._log_q: "queue.Queue[str]" = queue.Queue()
        self._progress_q: "queue.Queue[ProgressSnapshot]" = queue.Queue()

    def log(self, msg: str) -> None:
        """
        发送日志消息
        
        Args:
            msg: 日志消息（会自动添加时间戳）
        """
        ts = time.strftime("%H:%M:%S")
        self._log_q.put(f"[{ts}] {msg}")

    def progress(self, current: int, total: int, message: str = "") -> None:
        """
        发送进度更新
        
        Args:
            current: 当前进度
            total: 总进度
            message: 进度消息（可选）
        """
        self._progress_q.put(ProgressSnapshot(current=current, total=total, message=message))

    def drain_logs(self, limit: int = 200) -> list[str]:
        """
        清空并获取日志队列中的所有消息
        
        Args:
            limit: 最大获取数量
            
        Returns:
            日志消息列表
        """
        out: list[str] = []
        for _ in range(limit):
            try:
                out.append(self._log_q.get_nowait())
            except queue.Empty:
                break
        return out

    def drain_progress(self, limit: int = 50) -> list[ProgressSnapshot]:
        """
        清空并获取进度队列中的所有快照
        
        Args:
            limit: 最大获取数量
            
        Returns:
            进度快照列表
        """
        out: list[ProgressSnapshot] = []
        for _ in range(limit):
            try:
                out.append(self._progress_q.get_nowait())
            except queue.Empty:
                break
        return out


# Dynamically choose base class based on tkinterdnd2 availability
_dnd_tk_class = create_dnd_window_or_none()
if _dnd_tk_class is not None:
    # Use TkinterDnD.Tk as base, apply ttkbootstrap theme manually
    _AppBase = _dnd_tk_class
else:
    # Use standard ttkbootstrap.Window
    _AppBase = tb.Window


class App(_AppBase):
    """
    主应用程序窗口类
    
    继承自 ttkbootstrap.Window 或 TkinterDnD.Tk（如果支持拖拽）。
    提供完整的用户界面和业务逻辑。
    """
    
    def __init__(self, config: Optional[AppConfig] = None) -> None:
        """
        初始化应用程序窗口
        
        Args:
            config: 应用配置，如果为 None 则从文件加载
        """
        self.config_obj = config or load_config()

        # Initialize based on base class
        if _dnd_tk_class is not None:
            # Initialize TkinterDnD.Tk
            super().__init__()
            self.title("Dianping ETL Uploader")
            # Apply ttkbootstrap theme manually
            self.style = tb.Style(theme="flatly")
        else:
            # Initialize ttkbootstrap.Window
            super().__init__(themename="flatly")
            self.title("Dianping ETL Uploader")

        self.geometry("520x760")
        self.resizable(True, True)

        self.bus = UiBus()
        self._worker: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._selected_files: list[Path] = []
        self.breaker = CircuitBreaker(threshold=DEFAULT_CIRCUIT_BREAKER_THRESHOLD)

        # UI variables
        # Push mode (no ambiguous "dry-run" wording in UI)
        # preview: 不发送请求，仅预览；real: 真实发送 webhook
        self.var_push_mode = tk.StringVar(
            master=self, value=("preview" if bool(self.config_obj.dry_run) else "real")
        )
        self.var_archive = tb.BooleanVar(value=bool(self.config_obj.archive_to_processed_dir))
        self.var_platform_key = tb.StringVar(value=str(self.config_obj.platform_key))
        self.var_mode_text = tb.StringVar(value="")
        self.var_parse_stats = tb.StringVar(value="解析：-")
        self.var_push_stats = tb.StringVar(value="推送：-")
        self.var_last_status = tb.StringVar(value="状态：就绪")
        # 文件类型过滤选择（用于推送时过滤）
        self.var_file_type_filter = tk.StringVar(master=self, value="全部")

        # Keep config in sync with UI mode even if widget 'command' doesn't fire.
        def _on_push_mode_trace(*_args):
            try:
                self._on_change_push_mode()
            except Exception:
                pass

        try:
            self.var_push_mode.trace_add("write", _on_push_mode_trace)
        except Exception:
            pass

        self._build_ui()
        self._setup_keyboard_shortcuts()
        self._tick()
        self._refresh_mode_banner()

        dnd = detect_dnd_support()
        if dnd.enabled:
            self.bus.log("拖拽支持已启用（tkinterdnd2）")
        else:
            self.bus.log("拖拽未启用，已降级为“选择文件”按钮")
            self.bus.log(dnd.reason)

    def _build_ui(self) -> None:
        root = tb.Frame(self, padding=12)
        root.pack(fill=BOTH, expand=True)

        # Mode banner (most important clarity)
        self.mode_banner = tb.Labelframe(root, text="运行模式（最重要）", padding=8)
        self.mode_banner.pack(side=TOP, fill=X, pady=(0, 8))
        tb.Label(self.mode_banner, textvariable=self.var_mode_text).pack(side=LEFT)

        header = tb.Frame(root)
        header.pack(side=TOP, fill=X)
        tb.Label(header, text="Dianping ETL Uploader", font=("Helvetica", 16, "bold")).pack(
            side=LEFT
        )

        # Controls
        controls = tb.Frame(root, padding=(0, 10, 0, 6))
        controls.pack(side=TOP, fill=X)

        # Push mode selector (clear wording)
        tb.Label(controls, text="推送模式：").pack(side=LEFT)
        tb.Radiobutton(
            controls,
            text="预演（不发送请求）",
            variable=self.var_push_mode,
            value="preview",
        ).pack(side=LEFT)
        tb.Radiobutton(
            controls,
            text="真实推送（发送 webhook）",
            variable=self.var_push_mode,
            value="real",
        ).pack(side=LEFT, padx=(8, 0))

        tb.Checkbutton(
            controls,
            text="归档到 processed/",
            variable=self.var_archive,
            command=self._on_toggle_archive,
        ).pack(side=LEFT, padx=(10, 0))

        tb.Button(
            controls,
            text="选择 Excel 文件",
            command=self._on_pick_files,
        ).pack(side=RIGHT)

        # Platform Key
        key_row = tb.Frame(root, padding=(0, 0, 0, 6))
        key_row.pack(side=TOP, fill=X)
        tb.Label(key_row, text="platformKey:").pack(side=LEFT)
        self.platform_key_entry = tb.Entry(key_row, textvariable=self.var_platform_key)
        self.platform_key_entry.pack(side=LEFT, fill=X, expand=True, padx=(8, 8))
        tb.Button(key_row, text="保存配置", command=self._on_save_config).pack(side=RIGHT)
        tb.Button(key_row, text="Webhook 配置", command=self._on_webhook_config).pack(side=RIGHT, padx=(8, 0))

        # KPI summary
        kpi = tb.Labelframe(root, text="本次结果（不用看日志）", padding=8)
        kpi.pack(side=TOP, fill=X, pady=(0, 8))
        tb.Label(kpi, textvariable=self.var_parse_stats).pack(side=TOP, anchor="w")
        tb.Label(kpi, textvariable=self.var_push_stats).pack(side=TOP, anchor="w")
        tb.Label(kpi, textvariable=self.var_last_status).pack(side=TOP, anchor="w")

        # Drop zone
        self.drop_zone = tb.Labelframe(root, text="拖拽区", padding=10)
        self.drop_zone.pack(side=TOP, fill=X, pady=(0, 10))

        dz_label = tb.Label(
            self.drop_zone,
            text="将 .xlsx 文件拖拽到此处（可多选）\n或点击右上角按钮选择文件",
            justify="center",
        )
        dz_label.pack(fill=X, pady=8)

        # Register drop target if possible
        _enabled = register_drop_target(
            self.drop_zone,
            on_files=self._on_files_dropped,
            on_error=lambda m: self.bus.log(m),
        )
        if not _enabled:
            # not a failure; we have file dialog fallback
            pass

        # Selected files
        files_frame = tb.Labelframe(root, text="已选择文件", padding=8)
        files_frame.pack(side=TOP, fill=X)
        self.files_list = tb.Text(files_frame, height=5, wrap="none")
        self.files_list.pack(fill=BOTH, expand=True)
        self.files_list.configure(state="disabled")

        # Actions
        actions = tb.Frame(root, padding=(0, 10, 0, 6))
        actions.pack(side=TOP, fill=X)

        tb.Button(
            actions,
            text="开始处理",
            command=self._on_start,
        ).pack(side=LEFT)

        tb.Button(
            actions,
            text="停止",
            command=self._on_stop,
        ).pack(side=LEFT, padx=(8, 0))

        # 推送区域：包含文件类型过滤和推送按钮
        push_section = tb.Frame(actions)
        push_section.pack(side=LEFT, padx=(8, 0))
        
        # 文件类型过滤选择器（仅推送时使用）
        tb.Label(push_section, text="推送类型：", font=("Helvetica", 9)).pack(side=LEFT)
        file_type_options = [
            "全部",
            FILETYPE_MEMBER_TRADE,
            FILETYPE_INSTORE_ORDER,
            FILETYPE_INCOME_DISCOUNT,
            FILETYPE_COUPON_STAT,
            FILETYPE_MEMBER_STORAGE,
            FILETYPE_MEMBER_CARD_EXPORT,
        ]
        self.file_type_combo = tb.Combobox(
            push_section,
            textvariable=self.var_file_type_filter,
            values=file_type_options,
            state="readonly",
            width=18,
        )
        self.file_type_combo.pack(side=LEFT, padx=(4, 8))
        
        tb.Button(
            push_section,
            text="仅推送待上传",
            command=self._on_push_only,
        ).pack(side=LEFT)

        tb.Button(
            actions,
            text="重置（解除熔断/清空队列）",
            command=self._on_reset,
        ).pack(side=RIGHT)

        # Progress
        progress = tb.Labelframe(root, text="进度", padding=8)
        progress.pack(side=TOP, fill=X)

        self.progress_bar = tb.Progressbar(progress, maximum=100, value=0)
        self.progress_bar.pack(fill=X)
        self.progress_label = tb.Label(progress, text="就绪")
        self.progress_label.pack(anchor="w", pady=(6, 0))

        # Logs
        logs = tb.Labelframe(root, text="日志", padding=8)
        logs.pack(side=TOP, fill=BOTH, expand=True)

        # 日志工具栏
        log_toolbar = tb.Frame(logs, padding=(0, 0, 0, 4))
        log_toolbar.pack(side=TOP, fill=X)
        
        tb.Label(log_toolbar, text="搜索:", foreground="gray").pack(side=LEFT, padx=(0, 4))
        self.log_search_var = tb.StringVar()
        self.log_search_var.trace_add("write", lambda *args: self._filter_logs())
        search_entry = tb.Entry(log_toolbar, textvariable=self.log_search_var, width=20)
        search_entry.pack(side=LEFT, padx=(0, 4))
        
        tb.Button(log_toolbar, text="导出", command=self._export_logs, width=8).pack(side=RIGHT)
        tb.Button(log_toolbar, text="清除", command=self._clear_logs, width=8).pack(side=RIGHT, padx=(4, 0))
        
        self.log_text = tb.Text(logs, wrap="word")
        self.log_text.pack(side=LEFT, fill=BOTH, expand=True)
        sb = tb.Scrollbar(logs, command=self.log_text.yview)
        sb.pack(side=RIGHT, fill=Y)
        self.log_text.configure(yscrollcommand=sb.set, state="disabled")
        
        # 添加右键菜单
        self.log_menu = tk.Menu(self, tearoff=0)
        self.log_menu.add_command(label="复制", command=self._copy_log_selection)
        self.log_menu.add_command(label="清除", command=self._clear_logs)
        self.log_menu.add_separator()
        self.log_menu.add_command(label="导出日志", command=self._export_logs)
        self.log_text.bind("<Button-3>", self._show_log_context_menu)
        
        # 添加右键菜单
        self.log_menu = tk.Menu(self, tearoff=0)
        self.log_menu.add_command(label="复制", command=self._copy_log_selection)
        self.log_menu.add_command(label="清除", command=self._clear_logs)
        self.log_menu.add_separator()
        self.log_menu.add_command(label="导出日志", command=self._export_logs)
        self.log_text.bind("<Button-3>", self._show_log_context_menu)

        status = tb.Frame(root)
        status.pack(side=BOTTOM, fill=X)
        self.status_label = tb.Label(status, text="就绪", anchor="w")
        self.status_label.pack(side=LEFT, fill=X, expand=True)
        
        # 帮助按钮
        tb.Button(status, text="关于", command=self._show_about, width=8).pack(side=RIGHT)
        tb.Button(status, text="帮助", command=self._show_help, width=8).pack(side=RIGHT, padx=(4, 0))
        
        # 帮助按钮
        tb.Button(status, text="帮助", command=self._show_help, width=8).pack(side=RIGHT, padx=(4, 0))
        tb.Button(status, text="关于", command=self._show_about, width=8).pack(side=RIGHT)

    def _on_change_push_mode(self) -> None:
        mode = str(self.var_push_mode.get())
        self.config_obj.dry_run = mode != "real"
        self.bus.log(f"push_mode={'预演' if self.config_obj.dry_run else '真实推送'}")
        self._auto_save_config()
        self._refresh_mode_banner()

    def _on_toggle_archive(self) -> None:
        self.config_obj.archive_to_processed_dir = bool(self.var_archive.get())
        self.bus.log(f"archive_to_processed_dir={self.config_obj.archive_to_processed_dir}")
        self._auto_save_config()

    def _on_save_config(self) -> None:
        self._sync_config_from_ui()
        try:
            save_config(self.config_obj)
            self.bus.log("配置已保存：siyu_etl_config.json")
        except Exception as e:
            self.bus.log(f"保存配置失败: {e}")
            Messagebox.show_error(str(e), "保存失败", parent=self)

    def _on_webhook_config(self) -> None:
        """打开 Webhook 配置对话框"""
        dialog = WebhookConfigDialog(self, self.config_obj)
        result = dialog.show()
        if not result.cancelled:
            if result.webhooks:
                self.config_obj.webhooks = result.webhooks
            if result.batch_size is not None:
                self.config_obj.batch_size = result.batch_size
            if result.request_timeout_seconds is not None:
                self.config_obj.request_timeout_seconds = result.request_timeout_seconds
            try:
                save_config(self.config_obj)
                self.bus.log("Webhook 配置已保存")
                Messagebox.show_info("Webhook 配置已保存", "成功", parent=self)
            except Exception as e:
                self.bus.log(f"保存 Webhook 配置失败: {e}")
                Messagebox.show_error(f"保存失败: {e}", "错误", parent=self)

    def _auto_save_config(self) -> None:
        # Best-effort: don't disturb users
        try:
            self._sync_config_from_ui()
            save_config(self.config_obj)
        except Exception:
            pass

    def _sync_config_from_ui(self) -> None:
        self.config_obj.platform_key = str(self.var_platform_key.get()).strip()
        ui_mode = str(self.var_push_mode.get())
        self.config_obj.dry_run = ui_mode != "real"
        self.config_obj.archive_to_processed_dir = bool(self.var_archive.get())

    def _on_pick_files(self) -> None:
        from tkinter import filedialog

        initialdir = self.config_obj.last_open_dir or None
        paths = filedialog.askopenfilenames(
            title="选择 Excel 文件",
            filetypes=[("Excel", "*.xlsx"), ("All files", "*.*")],
            initialdir=initialdir,
        )
        files = [Path(p) for p in paths]
        if files:
            self.config_obj.last_open_dir = str(files[0].parent)
            self._auto_save_config()
        self._set_selected_files(files)

    def _on_files_dropped(self, files: Iterable[Path]) -> None:
        self._set_selected_files(list(files))

    def _set_selected_files(self, files: list[Path]) -> None:
        xlsx = [p for p in files if p.suffix.lower() == ".xlsx"]
        if not xlsx:
            self.bus.log("未选择到 .xlsx 文件")
            return

        self._selected_files = xlsx
        self.bus.log(f"已选择 {len(self._selected_files)} 个文件")
        self._render_selected_files()

    def _render_selected_files(self) -> None:
        self.files_list.configure(state="normal")
        self.files_list.delete("1.0", END)
        for p in self._selected_files:
            self.files_list.insert(END, str(p) + "\n")
        self.files_list.configure(state="disabled")

    def _on_start(self) -> None:
        if self._worker and self._worker.is_alive():
            self.bus.log("任务已在运行中")
            return
        if not self._selected_files:
            Messagebox.show_warning("请先选择 Excel 文件", "提示", parent=self)
            return

        # START BUTTON BEHAVIOR (no confusion):
        # Always parse only. Never push here.
        self._sync_config_from_ui()
        if not Messagebox.okcancel(
            "【开始处理】只做解析/清洗/去重/入库，不会推送。\n需要推送请点击【仅推送待上传】。\n确认继续？",
            "确认开始处理（仅解析入库）",
            parent=self,
        ):
            return

        self._stop_event.clear()
        self.bus.log("开始处理（仅解析入库）")
        self._worker = threading.Thread(target=self._worker_run, daemon=True)
        self._worker.start()

    def _on_push_only(self) -> None:
        if self._worker and self._worker.is_alive():
            self.bus.log("任务已在运行中")
            return
        self._sync_config_from_ui()

        # 获取选择的文件类型过滤
        file_type_filter_value = self.var_file_type_filter.get().strip()
        filter_text = f"（仅推送: {file_type_filter_value}）" if file_type_filter_value != "全部" else ""
        filter_hint = f"\n已选择推送类型：{file_type_filter_value}" if file_type_filter_value != "全部" else ""
        
        if self.config_obj.dry_run:
            if not Messagebox.okcancel(
                f"当前为【预演模式】：不会发送 webhook，只展示推送预览（前3包）。{filter_hint}\n确认继续？",
                "确认预演（不发送请求）",
                parent=self,
            ):
                return
        else:
            if not Messagebox.okcancel(
                f"当前为【真实推送模式】：将串行发送 webhook。\n重要规则：上一包没返回，下一包不会发送（会自动停止）。{filter_hint}\n确认继续？",
                "确认真实推送",
                parent=self,
            ):
                return
        
        self._stop_event.clear()
        self.bus.log(f"开始推送（仅推送待上传）{filter_text}")
        self._worker = threading.Thread(target=self._worker_push_only, daemon=True)
        self._worker.start()

    def _on_stop(self) -> None:
        if not (self._worker and self._worker.is_alive()):
            self.bus.log("当前没有运行中的任务")
            return
        self.bus.log("请求停止任务...")
        self._stop_event.set()

    def _on_reset(self) -> None:
        if not Messagebox.okcancel(
            "将清空本地队列（SQLite upload_tasks）并解除所有熔断，是否继续？",
            "确认重置",
            parent=self,
        ):
            return
        try:
            clear_all_tasks(self.config_obj.db_path)
            self.breaker.reset()
            self.bus.log("已重置：清空队列 + 解除熔断")
        except Exception as e:
            self.bus.log(f"重置失败: {e}")
            Messagebox.show_error(str(e), "重置失败", parent=self)

    def _worker_run(self) -> None:
        try:
            stats = parse_only(
                cfg=self.config_obj,
                db_path=self.config_obj.db_path,
                file_paths=self._selected_files,
                log=self.bus.log,
                progress=self.bus.progress,
                stop_flag=lambda: self._stop_event.is_set(),
            )
            self.var_parse_stats.set(
                f"解析：parsed={stats.parsed_rows} inserted={stats.inserted_rows} dup={stats.duplicate_rows} skipped={stats.skipped_rows}"
            )
            self.var_last_status.set("状态：解析完成（未推送）")
        except Exception as e:
            self.bus.log(f"运行失败: {e}")
            self.bus.progress(0, 1, "失败")
            self.var_last_status.set(f"状态：失败（解析） {e}")

    def _worker_push_only(self) -> None:
        try:
            # 获取选择的文件类型过滤
            file_type_filter_value = self.var_file_type_filter.get().strip()
            file_type_filter = None if file_type_filter_value == "全部" else file_type_filter_value
            
            ps = push_only(
                cfg=self.config_obj,
                db_path=self.config_obj.db_path,
                breaker=self.breaker,
                log=self.bus.log,
                progress=self.bus.progress,
                stop_flag=lambda: self._stop_event.is_set(),
                file_type_filter=file_type_filter,
            )
            mode_text = "预演" if ps.mode == "preview" else "真实推送"
            self.var_push_stats.set(
                f"推送：mode={mode_text} pending_rows={ps.pending_rows} "
                f"batches={ps.total_batches} attempted={ps.attempted_batches} "
                f"success={ps.success_batches} skipped={ps.skipped_batches}"
            )
            self.var_last_status.set(f"状态：{ps.stopped_reason}")
            if ps.last_errors:
                self.bus.log("最近错误（最多20条）：")
                for e in ps.last_errors[-5:]:
                    self.bus.log(e)
        except Exception as e:
            self.bus.log(f"推送失败: {e}")
            self.bus.progress(0, 1, "失败")
            self.var_last_status.set(f"状态：失败（推送） {e}")

    def _refresh_mode_banner(self) -> None:
        mode = str(self.var_push_mode.get())
        if mode != "real":
            self.var_mode_text.set("预演模式：不发送请求，仅预览将要推送的前3包")
        else:
            self.var_mode_text.set("真实推送模式：会发送 webhook（串行发送，上一包没返回则自动停止）")

    def _tick(self) -> None:
        # Drain logs
        for line in self.bus.drain_logs():
            self.log_text.configure(state="normal")
            self.log_text.insert(END, line + "\n")
            self.log_text.see(END)
            self.log_text.configure(state="disabled")

        # Drain progress (keep last)
        snaps = self.bus.drain_progress()
        if snaps:
            snap = snaps[-1]
            pct = 0
            if snap.total > 0:
                pct = int((snap.current / snap.total) * 100)
            self.progress_bar.configure(value=pct)
            label = snap.message or f"{snap.current}/{snap.total}"
            self.progress_label.configure(text=label)
            self.status_label.configure(text=label)

        self.after(100, self._tick)

    def _setup_keyboard_shortcuts(self) -> None:
        """设置键盘快捷键"""
        # Ctrl+O: 打开文件
        self.bind("<Control-o>", lambda e: self._on_pick_files())
        # Ctrl+S: 保存配置
        self.bind("<Control-s>", lambda e: self._on_save_config())
        # Ctrl+W: Webhook 配置
        self.bind("<Control-w>", lambda e: self._on_webhook_config())
        # F5: 开始处理
        self.bind("<F5>", lambda e: self._on_start())
        # F6: 仅推送
        self.bind("<F6>", lambda e: self._on_push_only())
        # Escape: 停止
        self.bind("<Escape>", lambda e: self._on_stop())

    def _filter_logs(self) -> None:
        """过滤日志（简单实现：高亮匹配）"""
        # 简单实现：只搜索，不实际过滤
        # 完整实现需要维护原始日志列表
        pass

    def _clear_logs(self) -> None:
        """清除日志"""
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", END)
        self.log_text.configure(state="disabled")
        self.bus.log("日志已清除")

    def _export_logs(self) -> None:
        """导出日志"""
        from tkinter import filedialog
        filename = filedialog.asksaveasfilename(
            title="导出日志",
            defaultextension=".txt",
            filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")],
        )
        if filename:
            try:
                content = self.log_text.get("1.0", END)
                Path(filename).write_text(content, encoding="utf-8")
                self.bus.log(f"日志已导出到: {filename}")
                Messagebox.show_info("日志已成功导出", "导出成功", parent=self)
            except Exception as e:
                self.bus.log(f"导出日志失败: {e}")
                Messagebox.show_error(f"导出失败: {e}", "错误", parent=self)

    def _copy_log_selection(self) -> None:
        """复制选中的日志"""
        try:
            if self.log_text.tag_ranges("sel"):
                self.clipboard_clear()
                self.clipboard_append(self.log_text.get("sel.first", "sel.last"))
        except Exception:
            pass

    def _show_log_context_menu(self, event) -> None:
        """显示日志右键菜单"""
        try:
            self.log_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.log_menu.grab_release()

    def _show_help(self) -> None:
        """显示帮助对话框"""
        help_text = """
Dianping ETL Uploader - 使用帮助

快捷键：
  Ctrl+O  - 选择文件
  Ctrl+S  - 保存配置
  Ctrl+W  - Webhook 配置
  F5      - 开始处理
  F6      - 仅推送待上传
  Esc     - 停止任务

操作流程：
  1. 选择推送模式（预演/真实推送）
  2. 拖拽或选择 Excel 文件
  3. 点击"开始处理"解析文件
  4. 点击"仅推送待上传"推送数据

支持的文件类型：
  • 会员交易明细
  • 店内订单明细(已结账)
  • 收入优惠统计
  • 优惠券统计表
  • 会员储值消费分析表
  • 会员卡导出

更多帮助请查看 README.md 和 docs/TROUBLESHOOTING.md
        """
        Messagebox.show_info(help_text, "使用帮助", parent=self)

    def _show_about(self) -> None:
        """显示关于对话框"""
        about_text = """
Dianping ETL Uploader
版本: 1.0.0

一个本地单机的 ETL + 推送工具：
• 解析大众点评导出的 Excel 数据
• 严格数据清洗和去重
• 按门店分组分片推送
• 支持重试/跳过/熔断机制

技术栈：
• Python 3.10+
• Tkinter + ttkbootstrap
• SQLite
• openpyxl

© 2026
        """
        Messagebox.show_info(about_text, "关于", parent=self)


def run_app() -> None:
    """
    运行应用程序
    
    创建应用程序实例并启动主事件循环。
    """
    app = App()
    app.mainloop()


