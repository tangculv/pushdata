from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import tkinter as tk
import ttkbootstrap as tb
from ttkbootstrap.constants import BOTH, BOTTOM, END, HORIZONTAL, LEFT, RIGHT, TOP, VERTICAL, X, Y, YES
from tkinter import Menu
from ttkbootstrap.dialogs import Messagebox

from siyu_etl.batch_service import BatchService
from siyu_etl.circuit_breaker import CircuitBreaker
from siyu_etl.config import AppConfig
from siyu_etl.constants import DEFAULT_CIRCUIT_BREAKER_THRESHOLD
from siyu_etl.db import (
    SESSION_STATUS_COMPLETED,
    SESSION_STATUS_PARSED,
    clear_all_tasks,
    clear_batch_runtime_data,
)
from siyu_etl.processor import parse_only, push_only
from siyu_etl.settings import load_config, save_config
from siyu_etl.ui.config_dialog import WebhookConfigDialog
from siyu_etl.ui.dnd import create_dnd_window_or_none, detect_dnd_support, register_drop_target


@dataclass(frozen=True)
class ProgressSnapshot:
    current: int
    total: int
    message: str = ""


@dataclass
class FileQueueItem:
    path: Path
    file_name: str
    file_size: int
    status: str = "等待处理"
    file_type: str = "待识别"
    parsed_rows: int = 0
    uploaded_rows: int = 0
    error: str = ""
    file_id: str = ""


class UiBus:
    def __init__(self) -> None:
        self._log_q: "queue.Queue[str]" = queue.Queue()
        self._progress_q: "queue.Queue[ProgressSnapshot]" = queue.Queue()
        self._event_q: "queue.Queue[tuple[str, dict]]" = queue.Queue()

    def log(self, msg: str) -> None:
        ts = time.strftime("%H:%M:%S")
        self._log_q.put(f"[{ts}] {msg}")

    def progress(self, current: int, total: int, message: str = "") -> None:
        self._progress_q.put(ProgressSnapshot(current=current, total=total, message=message))

    def event(self, kind: str, **payload) -> None:
        self._event_q.put((kind, payload))

    def drain_logs(self, limit: int = 200) -> list[str]:
        out: list[str] = []
        for _ in range(limit):
            try:
                out.append(self._log_q.get_nowait())
            except queue.Empty:
                break
        return out

    def drain_progress(self, limit: int = 50) -> list[ProgressSnapshot]:
        out: list[ProgressSnapshot] = []
        for _ in range(limit):
            try:
                out.append(self._progress_q.get_nowait())
            except queue.Empty:
                break
        return out

    def drain_events(self, limit: int = 100) -> list[tuple[str, dict]]:
        out: list[tuple[str, dict]] = []
        for _ in range(limit):
            try:
                out.append(self._event_q.get_nowait())
            except queue.Empty:
                break
        return out


_dnd_tk_class = create_dnd_window_or_none()
_AppBase = _dnd_tk_class if _dnd_tk_class is not None else tb.Window


class App(_AppBase):
    COLOR_TEXT_PRIMARY = "#0F172A"
    COLOR_TEXT_SECONDARY = "#475569"
    COLOR_TEXT_MUTED = "#94A3B8"
    COLOR_ACCENT = "#1E3A5F"
    COLOR_ACCENT_SOFT = "#E8EEF6"
    COLOR_SUCCESS = "#0F766E"
    COLOR_WARNING = "#A16207"
    COLOR_DANGER = "#B91C1C"
    COLOR_GOLD = "#B6925B"
    COLOR_BORDER = "#E5E7EB"
    COLOR_SURFACE = "#F8F5EF"

    def __init__(self, config: Optional[AppConfig] = None) -> None:
        self.config_obj = config or load_config()
        if _dnd_tk_class is not None:
            super().__init__()
            self.title("私域营销数据传输工具")
            self.style = tb.Style(theme="litera")
        else:
            super().__init__(themename="litera")
            self.title("私域营销数据传输工具")

        self.geometry("1280x920")
        self.minsize(1180, 820)
        self.resizable(True, True)

        self.bus = UiBus()
        self._worker: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._upload_totals: tuple[int, int] = (0, 0)
        self.breaker = CircuitBreaker(threshold=DEFAULT_CIRCUIT_BREAKER_THRESHOLD)
        self.batch_service = BatchService(self.config_obj.db_path)
        self._file_pool: list[FileQueueItem] = []
        self._selected_row_path: str = ""
        self._current_phase = "待开始"
        self._current_session_id = ""
        self._phase_started_at: float = time.time()

        self.var_status_bar = tb.StringVar(value="请先把这次要处理的 Excel 文件放进来")
        self.var_phase = tb.StringVar(value="当前阶段：还没开始")
        self.var_summary_files = tb.StringVar(value="文件：0")
        self.var_summary_success = tb.StringVar(value="成功：0")
        self.var_summary_failed = tb.StringVar(value="失败：0")
        self.var_summary_uploaded = tb.StringVar(value="已处理：0｜已上传：0")
        self.var_completion_note = tb.StringVar(value="")
        self.var_progress_hint = tb.StringVar(value="支持一次选多个文件，也可以后面继续添加")
        self.var_step_hint = tb.StringVar(value="当前步骤：等待开始")
        self.var_elapsed = tb.StringVar(value="")
        self.var_logs_visible = tb.BooleanVar(value=False)
        self.var_platform_key = tb.StringVar(value=str(self.config_obj.platform_key))
        self.config_obj.dry_run = False
        self.var_archive = tb.BooleanVar(value=bool(self.config_obj.archive_to_processed_dir))

        self._build_ui()
        self._configure_styles()
        self._setup_keyboard_shortcuts()
        self._tick()
        self._refresh_summary()

        dnd = detect_dnd_support()
        if dnd.enabled:
            self.bus.log("已启用拖拽，可直接拖入多个 Excel 文件")
        else:
            self.bus.log("当前环境不支持拖拽，请使用“选择文件”按钮")
            self.bus.log(dnd.reason)

        self.deiconify()
        self.lift()
        try:
            self.focus_force()
        except Exception:
            pass

    def _configure_styles(self) -> None:
        try:
            style = self.style if hasattr(self, "style") else tb.Style()
            style.configure("Luxury.Treeview",
                            background="#FCFBF8",
                            fieldbackground="#FCFBF8",
                            foreground=self.COLOR_TEXT_PRIMARY,
                            rowheight=34,
                            borderwidth=0)
            style.configure("Luxury.Treeview.Heading",
                            background="#F4EFE6",
                            foreground=self.COLOR_ACCENT,
                            relief="flat",
                            font=("Helvetica", 11, "bold"))
            style.map("Luxury.Treeview", background=[("selected", "#E9E2D5")], foreground=[("selected", self.COLOR_TEXT_PRIMARY)])
            style.configure("Completion.TLabel", background="#F4EFE6", foreground=self.COLOR_GOLD, padding=12, font=("Helvetica", 11, "bold"))
            style.configure("CompletionSuccess.TLabel", background="#ECFDF5", foreground=self.COLOR_SUCCESS, padding=12, font=("Helvetica", 11, "bold"))
            style.configure("CompletionWarning.TLabel", background="#FFFBEB", foreground=self.COLOR_WARNING, padding=12, font=("Helvetica", 11, "bold"))
            style.configure("CompletionMuted.TLabel", background="#F3F4F6", foreground=self.COLOR_TEXT_SECONDARY, padding=12, font=("Helvetica", 11, "bold"))
            style.configure("HeaderCaption.TLabel", background="#FFFFFF", foreground=self.COLOR_GOLD, font=("Helvetica", 10, "bold"))
            style.configure("HeaderTitle.TLabel", background="#FFFFFF", foreground=self.COLOR_TEXT_PRIMARY, font=("Helvetica", 24, "bold"))
            style.configure("HeaderStatus.TLabel", background="#FFFFFF", foreground=self.COLOR_TEXT_SECONDARY, font=("Helvetica", 10))
            style.configure("HeroStatValue.TLabel", background="#FFFFFF", foreground=self.COLOR_TEXT_PRIMARY, font=("Helvetica", 18, "bold"))
            style.configure("HeroStatLabel.TLabel", background="#FFFFFF", foreground=self.COLOR_TEXT_MUTED, font=("Helvetica", 10))
            style.configure("SectionTitle.TLabel", background="#FFFFFF", foreground=self.COLOR_TEXT_PRIMARY, font=("Helvetica", 13, "bold"))
            style.configure("SectionDesc.TLabel", background="#FFFFFF", foreground=self.COLOR_TEXT_SECONDARY, font=("Helvetica", 10))
            style.configure("StatusChip.TLabel", background=self.COLOR_ACCENT_SOFT, foreground=self.COLOR_ACCENT, padding=(10, 5), font=("Helvetica", 10, "bold"))
        except Exception:
            pass

    def _build_ui(self) -> None:
        root = tb.Frame(self, padding=18, bootstyle="light")
        root.pack(fill=BOTH, expand=True)

        hero = tb.Frame(root, bootstyle="light")
        hero.pack(side=TOP, fill=X, pady=(0, 16))
        hero_card = tb.Frame(hero, bootstyle="light", padding=18)
        hero_card.pack(fill=X)

        hero_top = tb.Frame(hero_card, bootstyle="light")
        hero_top.pack(fill=X)
        hero_left = tb.Frame(hero_top, bootstyle="light")
        hero_left.pack(side=LEFT, fill=X, expand=YES)
        tb.Label(hero_left, text="私域营销数据传输工具", style="HeaderCaption.TLabel").pack(anchor="w")
        tb.Label(hero_left, text="门店数据自动处理与上传", style="HeaderTitle.TLabel").pack(anchor="w", pady=(4, 0))
        tb.Label(hero_left, text="把文件放进来，系统会自动处理、分批上传，并持续给出确定的进度反馈。", style="SectionDesc.TLabel").pack(anchor="w", pady=(6, 0))

        hero_right = tb.Frame(hero_top, bootstyle="light")
        hero_right.pack(side=RIGHT, anchor="n")
        tb.Label(hero_right, textvariable=self.var_phase, style="StatusChip.TLabel").pack(anchor="e")

        tb.Label(hero_card, textvariable=self.var_status_bar, style="HeaderStatus.TLabel").pack(anchor="w", pady=(12, 0))

        stats_row = tb.Frame(hero_card, bootstyle="light")
        stats_row.pack(fill=X, pady=(16, 0))
        self._create_stat_card(stats_row, self.var_summary_files, "本次文件数").pack(side=LEFT, fill=X, expand=YES, padx=(0, 10))
        self._create_stat_card(stats_row, self.var_summary_uploaded, "处理 / 上传条数").pack(side=LEFT, fill=X, expand=YES, padx=5)
        self._create_stat_card(stats_row, self.var_summary_success, "成功文件").pack(side=LEFT, fill=X, expand=YES, padx=5)
        self._create_stat_card(stats_row, self.var_summary_failed, "失败文件").pack(side=LEFT, fill=X, expand=YES, padx=(10, 0))

        action_card = tb.Frame(root, bootstyle="light", padding=14)
        action_card.pack(side=TOP, fill=X, pady=(0, 12))
        tb.Label(action_card, text="操作区", style="SectionTitle.TLabel").pack(anchor="w")
        tb.Label(action_card, text="建议先一次放齐文件，再点击开始；中途也可以继续添加。", style="SectionDesc.TLabel").pack(anchor="w", pady=(2, 10))
        action_row = tb.Frame(action_card, bootstyle="light")
        action_row.pack(fill=X)
        tb.Button(action_row, text="选择文件", command=self._on_pick_files, bootstyle="outline-dark").pack(side=LEFT)
        tb.Button(action_row, text="继续添加", command=self._on_add_more_files, bootstyle="outline-dark").pack(side=LEFT, padx=(10, 0))
        self.btn_process = tb.Button(action_row, text="开始处理", command=self._on_start_process, bootstyle="dark", width=14)
        self.btn_process.pack(side=LEFT, padx=(14, 0))
        tb.Button(action_row, text="停止", command=self._on_stop, bootstyle="outline-dark").pack(side=LEFT, padx=(8, 0))
        self.btn_clear_selected = tb.Button(action_row, text="移除选中", command=self._on_remove_selected, bootstyle="link")
        self.btn_clear_selected.pack(side=RIGHT)

        progress_card = tb.Frame(root, bootstyle="light", padding=14)
        progress_card.pack(side=TOP, fill=X, pady=(0, 12))
        top_progress = tb.Frame(progress_card, bootstyle="light")
        top_progress.pack(fill=X)
        tb.Label(top_progress, text="执行进度", style="SectionTitle.TLabel").pack(side=LEFT)
        tb.Label(top_progress, textvariable=self.var_elapsed, foreground=self.COLOR_GOLD).pack(side=RIGHT)
        self.progress_bar = tb.Progressbar(progress_card, maximum=100, value=0, bootstyle="warning-striped")
        self.progress_bar.pack(fill=X, pady=(10, 8))
        self.progress_label = tb.Label(progress_card, text="等待开始", foreground=self.COLOR_TEXT_PRIMARY, font=("Helvetica", 11, "bold"))
        self.progress_label.pack(anchor="w")
        self.step_label = tb.Label(progress_card, textvariable=self.var_step_hint, foreground=self.COLOR_GOLD)
        self.step_label.pack(anchor="w", pady=(4, 0))
        tb.Label(progress_card, textvariable=self.var_progress_hint, style="SectionDesc.TLabel").pack(anchor="w", pady=(6, 0))
        self.completion_wrap = tb.Frame(root, bootstyle="light")
        self.completion_wrap.pack(fill=X, pady=(0, 10))
        self.completion_label = tb.Label(self.completion_wrap, textvariable=self.var_completion_note, style="CompletionSuccess.TLabel")
        self.completion_label.pack(anchor="w", fill=X)
        self.completion_wrap.pack_forget()

        content_header = tb.Frame(root, bootstyle="light")
        content_header.pack(fill=X, pady=(0, 8))
        tb.Label(content_header, text="文件与日志", style="SectionTitle.TLabel").pack(side=LEFT)
        tb.Label(content_header, text="左侧看文件结果，右下可展开详细日志。", style="SectionDesc.TLabel").pack(side=LEFT, padx=(10, 0))

        self.content_pane = tk.PanedWindow(root, orient=VERTICAL, sashrelief="flat", bd=0, bg="#EEE7DC", sashwidth=10, showhandle=False)
        self.content_pane.pack(side=TOP, fill=BOTH, expand=True, pady=(0, 10))
        self.content_pane.configure(height=520)

        list_box = tb.Frame(self.content_pane, bootstyle="light", padding=10)

        columns = ("file_name", "status", "parsed_rows", "uploaded_rows", "error")
        self.files_tree = tb.Treeview(list_box, columns=columns, show="headings", height=14, style="Luxury.Treeview")
        headings = {
            "file_name": "文件名",
            "status": "结果",
            "parsed_rows": "已处理",
            "uploaded_rows": "已上传",
            "error": "问题说明",
        }
        widths = {
            "file_name": 420,
            "status": 120,
            "parsed_rows": 100,
            "uploaded_rows": 100,
            "error": 360,
        }
        for col in columns:
            self.files_tree.heading(col, text=headings[col])
            self.files_tree.column(col, width=widths[col], anchor="w")
        self.files_tree.pack(side=LEFT, fill=BOTH, expand=True)
        self.files_tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        tree_scroll = tb.Scrollbar(list_box, command=self.files_tree.yview, bootstyle="round")
        tree_scroll.pack(side=RIGHT, fill=Y)
        self.files_tree.configure(yscrollcommand=tree_scroll.set)
        register_drop_target(self.files_tree, on_files=self._on_files_dropped, on_error=lambda m: self.bus.log(m))
        self.content_pane.add(list_box, stretch="always")

        self.logs_frame = tb.Frame(self.content_pane, bootstyle="light", padding=10)
        log_wrap = tb.Frame(self.logs_frame, bootstyle="light")
        log_wrap.pack(fill=BOTH, expand=True)
        self.log_text = tb.Text(
            log_wrap,
            height=14,
            wrap="none",
            relief="flat",
            bd=0,
            background="#FCFBF8",
            foreground=self.COLOR_TEXT_SECONDARY,
            insertbackground=self.COLOR_TEXT_PRIMARY,
        )
        self.log_text.pack(side=LEFT, fill=BOTH, expand=True)
        self.log_scroll_y = tb.Scrollbar(log_wrap, orient=VERTICAL, command=self.log_text.yview, bootstyle="round")
        self.log_scroll_y.pack(side=RIGHT, fill=Y)
        self.log_scroll_x = tb.Scrollbar(self.logs_frame, orient=HORIZONTAL, command=self.log_text.xview, bootstyle="round")
        self.log_scroll_x.pack(side=BOTTOM, fill=X)
        self.log_text.configure(state="disabled", yscrollcommand=self.log_scroll_y.set, xscrollcommand=self.log_scroll_x.set)

        self.logs_frame_visible = False

        footer = tb.Frame(root, bootstyle="light")
        footer.pack(side=BOTTOM, fill=X, pady=(4, 0))
        self.footer = footer
        self.btn_more = tb.Button(footer, text="更多", command=self._show_more_menu, bootstyle="link")
        self.btn_more.pack(side=LEFT)
        self.btn_toggle_logs = tb.Button(footer, text="查看详细过程", command=self._toggle_logs, bootstyle="link")
        self.btn_toggle_logs.pack(side=RIGHT)

        self.more_menu = Menu(self, tearoff=0)
        self.more_menu.add_command(label="恢复上次未上传", command=self._on_resume_last_upload)
        self.more_menu.add_command(label="清空这次文件", command=self._on_clear_files)
        self.more_menu.add_command(label="清空本地记录", command=self._on_reset)
        self.more_menu.add_separator()
        self.more_menu.add_command(label="帮助", command=self._show_help)
        self.more_menu.add_command(label="导出记录", command=self._export_logs)

    def _create_stat_card(self, parent, value_var: tk.StringVar, label: str) -> tb.Frame:
        card = tb.Frame(parent, bootstyle="light", padding=12)
        tb.Label(card, textvariable=value_var, style="HeroStatValue.TLabel").pack(anchor="w")
        tb.Label(card, text=label, style="HeroStatLabel.TLabel").pack(anchor="w", pady=(4, 0))
        return card

    def _show_more_menu(self) -> None:
        try:
            x = self.btn_more.winfo_rootx()
            y = self.btn_more.winfo_rooty() + self.btn_more.winfo_height()
            self.more_menu.tk_popup(x, y)
        finally:
            try:
                self.more_menu.grab_release()
            except Exception:
                pass

    def _set_process_button_text(self, text: str) -> None:
        try:
            self.btn_process.configure(text=text)
        except Exception:
            pass

    def _toggle_logs(self) -> None:
        visible = bool(self.var_logs_visible.get())
        if visible:
            if self.logs_frame_visible:
                try:
                    self.content_pane.forget(self.logs_frame)
                except Exception:
                    pass
                self.logs_frame_visible = False
            self.var_logs_visible.set(False)
            self.btn_toggle_logs.configure(text="查看详细过程")
        else:
            if not self.logs_frame_visible:
                self.content_pane.add(self.logs_frame, stretch="never")
                try:
                    self.content_pane.sash_place(0, 0, max(self.winfo_height() - 260, 420))
                except Exception:
                    pass
                self.logs_frame_visible = True
            self.var_logs_visible.set(True)
            self.btn_toggle_logs.configure(text="收起详细过程")


    def _on_toggle_archive(self) -> None:
        self.config_obj.archive_to_processed_dir = bool(self.var_archive.get())
        self._auto_save_config()

    def _on_save_config(self) -> None:
        self._sync_config_from_ui()
        try:
            save_config(self.config_obj)
            self.bus.log("配置已保存")
        except Exception as e:
            self.bus.log(f"保存配置失败: {e}")
            Messagebox.show_error(str(e), "保存失败", parent=self)

    def _on_webhook_config(self) -> None:
        dialog = WebhookConfigDialog(self, self.config_obj)
        result = dialog.show()
        if not result.cancelled:
            if result.webhooks:
                self.config_obj.webhooks = result.webhooks
            if result.batch_size is not None:
                self.config_obj.batch_size = result.batch_size
            if result.request_timeout_seconds is not None:
                self.config_obj.request_timeout_seconds = result.request_timeout_seconds
            self._on_save_config()

    def _auto_save_config(self) -> None:
        try:
            self._sync_config_from_ui()
            save_config(self.config_obj)
        except Exception:
            pass

    def _sync_config_from_ui(self) -> None:
        self.config_obj.platform_key = str(self.var_platform_key.get()).strip()
        self.config_obj.dry_run = False
        self.config_obj.archive_to_processed_dir = bool(self.var_archive.get())

    def _ask_excel_files(self) -> list[Path]:
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
        return files

    def _on_pick_files(self) -> None:
        self._replace_file_pool(self._ask_excel_files())

    def _on_add_more_files(self) -> None:
        self._append_files(self._ask_excel_files())

    def _on_files_dropped(self, files: Iterable[Path]) -> None:
        self._append_files(list(files))

    def _replace_file_pool(self, files: list[Path]) -> None:
        self._file_pool = []
        self._current_session_id = ""
        self._set_phase("待开始")
        self._append_files(files, replacing=True)

    def _append_files(self, files: list[Path], replacing: bool = False) -> None:
        xlsx_files = [Path(p) for p in files if Path(p).suffix.lower() == ".xlsx"]
        if not xlsx_files:
            if files:
                self.bus.log("本次没有可用的 .xlsx 文件")
            return

        usable_files: list[Path] = []
        blocked_processed = 0
        for fp in xlsx_files:
            lower_name = fp.name.lower()
            lower_parts = [part.lower() for part in fp.parts]
            if "_processed" in lower_name or "processed" in lower_parts:
                blocked_processed += 1
                self.bus.log(f"已拦截疑似已处理文件: {fp.name}")
                continue
            usable_files.append(fp)

        if blocked_processed:
            Messagebox.show_warning(
                f"已拦截 {blocked_processed} 个 processed 文件，请使用原始导出的 Excel 文件。",
                "文件已拦截",
                parent=self,
            )
        if not usable_files:
            return

        existing = {str(item.path.resolve()) for item in self._file_pool}
        added = 0
        duplicate = 0
        for fp in usable_files:
            key = str(fp.resolve())
            if key in existing:
                duplicate += 1
                continue
            size = fp.stat().st_size if fp.exists() else 0
            self._file_pool.append(FileQueueItem(path=fp, file_name=fp.name, file_size=size))
            existing.add(key)
            added += 1

        if added:
            self._current_session_id = ""
            msg = "已载入本次文件" if replacing else "已追加文件"
            self.bus.log(f"{msg}：新增 {added} 个")
        if duplicate:
            self.bus.log(f"已自动去重：忽略重复文件 {duplicate} 个")
        self._refresh_file_tree()
        self._refresh_summary()

    def _on_tree_select(self, _event=None) -> None:
        selected = self.files_tree.selection()
        self._selected_row_path = str(selected[0]) if selected else ""

    def _on_remove_selected(self) -> None:
        if not self._selected_row_path:
            Messagebox.show_warning("请先在列表中点选一个文件", "提示", parent=self)
            return
        before = len(self._file_pool)
        self._file_pool = [item for item in self._file_pool if str(item.path) != self._selected_row_path]
        self._selected_row_path = ""
        if len(self._file_pool) < before:
            self._current_session_id = ""
            self.bus.log("已移除选中的文件")
        self._refresh_file_tree()
        self._refresh_summary()

    def _find_resumable_session_id(self) -> tuple[str, int]:
        best_session_id = ""
        best_pending = 0
        best_file_count = -1
        for session in self.batch_service.list_sessions(limit=20):
            pending = self.batch_service.count_session_tasks(session_id=session.session_id, status="PENDING")
            if pending <= 0:
                continue
            if session.status in {SESSION_STATUS_COMPLETED}:
                continue
            file_count = int(session.total_files or 0)
            if pending > best_pending or (pending == best_pending and file_count > best_file_count):
                best_session_id = session.session_id
                best_pending = pending
                best_file_count = file_count
        return best_session_id, best_pending

    def _load_session_into_ui(self, session_id: str) -> bool:
        files = self.batch_service.list_files(session_id)
        if not files:
            return False
        self._file_pool = [
            FileQueueItem(
                path=Path(record.file_path),
                file_name=record.file_name,
                file_size=record.file_size,
            )
            for record in files
        ]
        self._current_session_id = session_id
        self._refresh_from_session()
        return True

    def _on_resume_last_upload(self) -> None:
        self.bus.log("开始检查是否存在可恢复的未上传任务")
        try:
            if self._worker and self._worker.is_alive():
                self.bus.log("当前已有任务在运行")
                Messagebox.show_warning("当前已有任务在运行，请稍后再试", "提示", parent=self)
                return
            session_id, pending = self._find_resumable_session_id()
            if not session_id:
                self.bus.log("未找到可恢复的未上传任务")
                Messagebox.show_info("当前没有可恢复的未上传任务", "提示", parent=self)
                return
            self.bus.log(f"找到可恢复任务：session_id={session_id}，待上传 {pending} 条")
            if not self._load_session_into_ui(session_id):
                self.bus.log(f"恢复失败：session_id={session_id} 的文件列表为空或无法读取")
                Messagebox.show_warning("找到了本地任务，但恢复文件列表失败，请联系技术支持", "提示", parent=self)
                return
            self._set_phase("正在准备推送")
            self._refresh_summary()
            self.bus.log(f"已恢复上次任务：{len(self._file_pool)} 个文件，待上传 {pending} 条")
            restore_message = (
                f"已找到可恢复任务：{len(self._file_pool)} 个文件，待上传 {pending} 条。\n"
                "继续后将直接开始推送。\n"
                "是否现在继续？"
            )
            if not Messagebox.okcancel(
                restore_message,
                "恢复上传",
                parent=self,
            ):
                self.bus.log("用户取消了恢复上传")
                return
            self.bus.log("用户确认恢复上传，开始继续推送")
            self._start_upload_stage(auto_started=False)
        except Exception as e:
            self.bus.log(f"恢复未上传任务失败: {e}")
            Messagebox.show_error(str(e), "恢复上传失败", parent=self)

    def _on_clear_files(self) -> None:
        if not self._file_pool:
            return
        if not Messagebox.okcancel("将清空本次已选择的文件，是否继续？", "确认清空", parent=self):
            return
        self._file_pool = []
        self._current_session_id = ""
        self._set_phase("待开始")
        self.bus.log("已清空本次文件")
        self._refresh_file_tree()
        self._refresh_summary()

    def _refresh_file_tree(self) -> None:
        for item in self.files_tree.get_children():
            self.files_tree.delete(item)
        for row in self._file_pool:
            self.files_tree.insert(
                "",
                END,
                iid=str(row.path),
                values=(
                    row.file_name,
                    row.status,
                    row.parsed_rows,
                    row.uploaded_rows,
                    row.error or "—",
                ),
            )

    @staticmethod
    def _format_size(size: int) -> str:
        if size >= 1024 * 1024:
            return f"{size / 1024 / 1024:.1f} MB"
        if size >= 1024:
            return f"{size / 1024:.1f} KB"
        return f"{size} B"

    @staticmethod
    def _ui_status(raw_status: str) -> str:
        mapping = {
            "PENDING_PARSE": "等待处理",
            "PARSING": "处理中",
            "PARSE_SUCCESS": "待推送",
            "READY_TO_UPLOAD": "待推送",
            "UPLOADING": "上传中",
            "UPLOAD_SUCCESS": "上传完成",
            "PARSE_FAILED": "检查失败",
            "UPLOAD_FAILED": "上传失败",
            "STOPPED": "已停止",
        }
        return mapping.get(raw_status, raw_status or "等待处理")

    def _refresh_from_session(self) -> None:
        if not self._current_session_id:
            return
        files = self.batch_service.list_files(self._current_session_id)
        if not files:
            return
        by_path = {str(item.path): item for item in self._file_pool}
        for record in files:
            item = by_path.get(record.file_path)
            if not item:
                fp = Path(record.file_path)
                item = FileQueueItem(path=fp, file_name=record.file_name, file_size=record.file_size)
                self._file_pool.append(item)
            item.file_id = record.file_id
            item.status = self._ui_status(record.status)
            item.file_type = record.file_type or "待识别"
            item.parsed_rows = record.parse_rows
            item.uploaded_rows = record.uploaded_rows
            item.error = record.upload_error or record.parse_error
        summary = self.batch_service.summary(self._current_session_id)
        if summary:
            status_map = {
                "CREATED": "待开始",
                "PARSING": "处理中",
                SESSION_STATUS_PARSED: "正在准备推送",
                "UPLOADING": "上传中",
                "COMPLETED": "已完成",
                "PARTIAL_FAILED": "已完成",
                "FAILED": "已完成",
                "STOPPED": "已停止",
            }
            next_phase = status_map.get(summary.status, self._current_phase)
            if next_phase != self._current_phase:
                self._set_phase(next_phase)
            if self._current_phase == "正在准备推送" and any(item.status in {"上传中", "上传完成"} for item in self._file_pool):
                self._set_phase("上传中")
        self._refresh_file_tree()
        self._refresh_summary()

    def _set_phase(self, phase: str) -> None:
        if self._current_phase != phase:
            self._current_phase = phase
            self._phase_started_at = time.time()

    def _phase_elapsed_text(self) -> str:
        elapsed = max(0, int(time.time() - self._phase_started_at))
        minutes, seconds = divmod(elapsed, 60)
        if minutes >= 60:
            hours, minutes = divmod(minutes, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    def _refresh_summary(self) -> None:
        total_files = len(self._file_pool)
        success_files = sum(1 for x in self._file_pool if x.status == "上传完成")
        failed_files = sum(1 for x in self._file_pool if "失败" in x.status or x.status == "已停止")
        total_parsed = sum(x.parsed_rows for x in self._file_pool)
        total_uploaded = sum(x.uploaded_rows for x in self._file_pool)

        phase_label = self._current_phase
        if self._current_phase == "已完成":
            phase_label = "已全部完成" if failed_files == 0 else "部分完成"
        self.var_phase.set(f"当前阶段：{phase_label}")
        self.var_summary_files.set(f"文件：{total_files}")
        self.var_summary_success.set(f"成功：{success_files}")
        self.var_summary_failed.set(f"失败：{failed_files}")
        self.var_summary_uploaded.set(f"已处理：{total_parsed}｜已上传：{total_uploaded}")
        elapsed_text = self._phase_elapsed_text()
        if self._current_phase in {"处理中", "正在准备推送", "上传中"}:
            self.var_elapsed.set(f"已持续：{elapsed_text}")
        else:
            self.var_elapsed.set("")
        self.var_completion_note.set("")
        try:
            self.completion_label.configure(style="CompletionSuccess.TLabel")
            self.completion_wrap.pack_forget()
        except Exception:
            pass

        if total_files > 0 and self._current_phase == "已完成":
            if failed_files == 0:
                self.progress_label.configure(text=f"这次共 {total_files} 个文件，已全部处理并上传完成")
                self.var_completion_note.set("全部完成，可以直接继续下一批")
                self.completion_label.configure(style="CompletionSuccess.TLabel")
                self.completion_wrap.pack(fill=X, pady=(0, 10))
            else:
                self.progress_label.configure(text=f"这次共 {total_files} 个文件，成功 {success_files} 个，失败 {failed_files} 个")
                self.var_completion_note.set("有文件没处理成功，请看列表里的问题说明")
                self.completion_label.configure(style="CompletionWarning.TLabel")
                self.completion_wrap.pack(fill=X, pady=(0, 10))

        if total_files == 0:
            self.var_status_bar.set("请先把这次要处理的 Excel 文件放进来")
            self.var_progress_hint.set("支持一次选多个文件，也可以后面继续添加")
            self.var_step_hint.set("当前步骤：等待开始")
            self.progress_label.configure(text="等待开始")
            self._set_process_button_text("开始处理")
        elif self._current_phase == "待开始":
            self.var_status_bar.set(f"已经放入 {total_files} 个文件，确认后点一次开始就行")
            self.var_progress_hint.set("系统会自动处理并上传；如果文件还没加全，可以继续添加")
            self.var_step_hint.set("当前步骤：等待点击开始")
            self.progress_label.configure(text="等待开始")
            self._set_process_button_text("开始处理")
        elif self._current_phase == "处理中":
            self.var_status_bar.set(f"第 1 步 / 2：正在处理文件，请稍候（共 {total_files} 个）")
            self.var_progress_hint.set("文件较大时会比较久，请保持窗口开启")
            self.var_step_hint.set("当前步骤：第 1 步（处理文件）→ 完成后自动进入第 2 步（上传数据）")
            self._set_process_button_text("处理中...")
        elif self._current_phase == "已停止":
            self.var_status_bar.set("本次操作已停止，可继续调整文件后重新开始")
            self.var_progress_hint.set("已完成的结果会保留，未完成的文件可以重新检查或重新上传")
            self.var_completion_note.set("本次已停止")
            self.completion_label.configure(style="CompletionMuted.TLabel")
            self.completion_wrap.pack(fill=X, pady=(0, 10))
            self._set_process_button_text("重新处理")
        elif self._current_phase == "正在准备推送":
            elapsed_text = self._phase_elapsed_text()
            self.var_status_bar.set(f"第 1 步已完成，正在整理上传数据（已持续 {elapsed_text}）")
            self.var_progress_hint.set(f"本次已处理 {total_parsed} 条，系统正在生成上传批次，请不要关闭窗口")
            self.var_step_hint.set("当前步骤：正在进入第 2 步（上传数据）")
            self.progress_label.configure(text=f"处理完成，共 {total_files} 个文件，已处理 {total_parsed} 条，正在生成上传批次")
            self._set_process_button_text("准备上传...")
        elif self._current_phase == "上传中":
            self.var_status_bar.set(f"第 2 步 / 2：正在上传数据，请稍候（共 {total_files} 个文件）")
            self.var_progress_hint.set(f"上传过程中请不要关闭窗口｜已处理 {total_parsed} 条｜已上传 {total_uploaded} 条")
            self.var_step_hint.set("当前步骤：第 2 步（上传数据）")
            self._set_process_button_text("上传中...")
        elif self._current_phase == "已完成":
            self.var_step_hint.set("当前步骤：全部完成")
            if failed_files == 0:
                self.var_status_bar.set(f"本次已全部完成，共成功处理并上传 {success_files} 个文件")
                self.var_progress_hint.set(f"本次共处理 {total_parsed} 条，已上传 {total_uploaded} 条；可以关闭窗口，或继续添加新文件")
            else:
                self.var_status_bar.set(f"本次处理结束：成功 {success_files} 个，失败 {failed_files} 个")
                self.var_progress_hint.set(f"本次共处理 {total_parsed} 条，已上传 {total_uploaded} 条；请看列表里的问题说明，处理后再重新开始")
            self._set_process_button_text("继续处理新文件")

    def _on_start_process(self) -> None:
        if self._worker and self._worker.is_alive():
            self.bus.log("当前已有任务在运行")
            return
        if not self._file_pool:
            Messagebox.show_warning("请先选择至少一个 Excel 文件", "提示", parent=self)
            return
        if not Messagebox.okcancel("系统会先处理文件，再自动推送。确认开始处理吗？", "开始处理", parent=self):
            return

        self._start_parse_stage()

    def _start_parse_stage(self) -> None:
        self._sync_config_from_ui()
        self._stop_event.clear()
        self._set_phase("处理中")
        self._current_session_id = ""
        for item in self._file_pool:
            item.status = "等待检查"
            item.file_type = "待识别"
            item.parsed_rows = 0
            item.uploaded_rows = 0
            item.error = ""
            item.file_id = ""
        self._refresh_file_tree()
        self._refresh_summary()
        self.progress_bar.configure(value=0)
        self.progress_label.configure(text=f"正在处理文件 0/{len(self._file_pool)}")
        self.bus.log(f"开始处理，本次共 {len(self._file_pool)} 个文件")
        self._worker = threading.Thread(target=self._worker_parse_run, daemon=True)
        self._worker.start()

    def _on_start_parse(self) -> None:
        self._on_start_process()

    def _worker_parse_run(self) -> None:
        files = [item.path for item in self._file_pool]
        try:
            stats = parse_only(
                cfg=self.config_obj,
                db_path=self.config_obj.db_path,
                file_paths=files,
                log=self.bus.log,
                progress=self.bus.progress,
                stop_flag=lambda: self._stop_event.is_set(),
                session_id=self._current_session_id or None,
            )
            processed_files = len(files)
            self.bus.log(f"文件处理完成：本次共处理 {processed_files} 个文件，解析 {stats.parsed_rows} 条，准备进入上传阶段")
            self.bus.event(
                "parse_finished",
                session_id=stats.session_id,
                auto_start_upload=True,
            )
            return
        except Exception as e:
            self.bus.log(f"文件检查失败: {e}")
            self.bus.event("parse_failed")
        finally:
            self.bus.event("refresh_session")

    def _on_start_upload(self) -> None:
        self._start_upload_stage(auto_started=False)

    def _start_upload_stage(self, auto_started: bool) -> None:
        if self._worker and self._worker.is_alive():
            self.bus.log("当前已有任务在运行")
            return
        if not self._current_session_id:
            Messagebox.show_warning("当前还没有可上传的结果，请重新开始处理", "提示", parent=self)
            return

        self._sync_config_from_ui()
        self._stop_event.clear()
        self._set_phase("上传中")
        summary = self.batch_service.summary(self._current_session_id)
        if summary:
            self._upload_totals = (int(summary.uploaded_rows or 0), int(summary.total_rows or 0))
        else:
            self._upload_totals = (0, 0)
        self._refresh_summary()
        if auto_started:
            self.bus.log("处理完成，系统已自动进入上传阶段")
        else:
            self.bus.log("开始上传本次文件")
        self._worker = threading.Thread(target=self._worker_upload_run, daemon=True)
        self._worker.start()

    def _worker_upload_run(self) -> None:
        try:
            push_only(
                cfg=self.config_obj,
                db_path=self.config_obj.db_path,
                breaker=self.breaker,
                log=self.bus.log,
                progress=self.bus.progress,
                stop_flag=lambda: self._stop_event.is_set(),
                session_id=self._current_session_id or None,
            )
            self.bus.log("推送阶段完成")
            self.bus.event("upload_finished")
        except Exception as e:
            self.bus.log(f"上传失败: {e}")
            self.bus.event("upload_failed")
        finally:
            self.bus.event("refresh_session")

    def _on_stop(self) -> None:
        if not (self._worker and self._worker.is_alive()):
            self.bus.log("当前没有运行中的任务")
            return
        self._stop_event.set()
        self._set_phase("已停止")
        self.progress_label.configure(text="正在停止，请稍候")
        self.bus.log("已请求停止当前操作")

    def _on_worker_parse_finished(self, session_id: str, auto_start_upload: bool) -> None:
        self._current_session_id = session_id
        self._worker = None
        self._set_phase("正在准备推送")
        self._refresh_from_session()
        self._refresh_summary()
        if auto_start_upload:
            self.after(50, lambda: self._start_upload_stage(auto_started=True))

    def _on_worker_parse_failed(self) -> None:
        self._worker = None
        self._set_phase("已完成")
        self.progress_label.configure(text="处理未完成")
        self._refresh_from_session()
        self._refresh_summary()

    def _on_worker_upload_finished(self) -> None:
        self._worker = None
        self._refresh_from_session()
        self.progress_bar.configure(value=100)
        self.progress_label.configure(text="本次处理与上传已完成")
        self.var_status_bar.set("本次处理与上传已完成")
        self._refresh_summary()

    def _on_worker_upload_failed(self) -> None:
        self._worker = None
        self._set_phase("已完成")
        self.progress_label.configure(text="上传未完成")
        self._refresh_from_session()
        self._refresh_summary()

    def _handle_bus_events(self) -> None:
        for kind, payload in self.bus.drain_events():
            if kind == "refresh_session":
                if self._current_session_id:
                    self._refresh_from_session()
            elif kind == "parse_finished":
                self._on_worker_parse_finished(
                    session_id=str(payload.get("session_id") or ""),
                    auto_start_upload=bool(payload.get("auto_start_upload", False)),
                )
            elif kind == "parse_failed":
                self._on_worker_parse_failed()
            elif kind == "upload_finished":
                self._on_worker_upload_finished()
            elif kind == "upload_failed":
                self._on_worker_upload_failed()

    def _on_reset(self) -> None:
        if not Messagebox.okcancel("将清空本地待处理数据、本次批量记录，并解除熔断状态，是否继续？", "确认重置", parent=self):
            return
        try:
            clear_all_tasks(self.config_obj.db_path)
            clear_batch_runtime_data(self.config_obj.db_path)
            self.breaker.reset()
            self._current_session_id = ""
            self._set_phase("待开始")
            for item in self._file_pool:
                item.status = "等待处理"
                item.file_type = "待识别"
                item.parsed_rows = 0
                item.uploaded_rows = 0
                item.error = ""
                item.file_id = ""
            self._refresh_file_tree()
            self._refresh_summary()
            self.bus.log("已重置本地待处理数据和本次批量记录")
        except Exception as e:
            self.bus.log(f"重置失败: {e}")
            Messagebox.show_error(str(e), "重置失败", parent=self)

    def _tick(self) -> None:
        for line in self.bus.drain_logs():
            self.log_text.configure(state="normal")
            try:
                _, bottom = self.log_text.yview()
            except Exception:
                bottom = 1.0
            self.log_text.insert(END, line + "\n")
            if bottom >= 0.999:
                self.log_text.see(END)
            self.log_text.configure(state="disabled")

        self._handle_bus_events()

        snaps = self.bus.drain_progress()
        if snaps:
            snap = snaps[-1]
            pct = int((snap.current / snap.total) * 100) if snap.total > 0 else 0
            self.progress_bar.configure(value=pct)
            label = snap.message or f"{snap.current}/{snap.total}"
            if self._current_phase == "处理中":
                self.progress_label.configure(text=f"正在处理：{label}")
            elif self._current_phase == "上传中":
                self.progress_label.configure(text=f"正在上传：{label}")
            else:
                self.progress_label.configure(text=label)
            if self._current_phase == "处理中":
                self.var_status_bar.set(f"第 1 步 / 2：正在处理第 {snap.current}/{max(snap.total, 1)} 个文件")
                self.var_step_hint.set("当前步骤：第 1 步（处理文件）")
                self.var_progress_hint.set("文件较大时会比较久，请保持窗口开启")
            elif self._current_phase == "上传中":
                uploaded_rows, total_rows = self._upload_totals
                if self._current_session_id:
                    summary = self.batch_service.summary(self._current_session_id)
                    if summary:
                        uploaded_rows = int(summary.uploaded_rows or 0)
                        total_rows = int(summary.total_rows or 0)
                        self._upload_totals = (uploaded_rows, total_rows)
                self.var_status_bar.set(f"第 2 步 / 2：正在上传第 {snap.current}/{max(snap.total, 1)} 批")
                self.var_step_hint.set("当前步骤：第 2 步（上传数据）")
                if total_rows > 0:
                    self.var_progress_hint.set(
                        f"上传过程中请不要关闭窗口｜当前批次 {snap.current}/{max(snap.total, 1)}｜已上传 {uploaded_rows}/{total_rows} 条"
                    )
                else:
                    self.var_progress_hint.set(f"上传过程中请不要关闭窗口｜当前批次 {snap.current}/{max(snap.total, 1)}")

        if self._current_session_id and not (self._worker and self._worker.is_alive()):
            self._refresh_from_session()

        self.after(200, self._tick)

    def _setup_keyboard_shortcuts(self) -> None:
        self.bind("<Control-o>", lambda e: self._on_pick_files())
        self.bind("<Control-Shift-O>", lambda e: self._on_add_more_files())
        self.bind("<Control-s>", lambda e: self._on_save_config())
        self.bind("<Control-w>", lambda e: self._on_webhook_config())
        self.bind("<F5>", lambda e: self._on_start_process())
        self.bind("<Escape>", lambda e: self._on_stop())

    def _clear_logs(self) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", END)
        self.log_text.configure(state="disabled")
        self.bus.log("已清空详细记录")

    def _export_logs(self) -> None:
        from tkinter import filedialog

        filename = filedialog.asksaveasfilename(
            title="导出处理记录",
            defaultextension=".txt",
            filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")],
        )
        if filename:
            try:
                Path(filename).write_text(self.log_text.get("1.0", END), encoding="utf-8")
                self.bus.log(f"处理记录已导出到: {filename}")
            except Exception as e:
                self.bus.log(f"导出处理记录失败: {e}")
                Messagebox.show_error(f"导出失败: {e}", "错误", parent=self)

    def _show_help(self) -> None:
        Messagebox.show_info(
            "使用步骤：\n1. 先把这次要处理的 Excel 文件加进来\n2. 如果没加全，可以继续添加，系统会自动去重\n3. 点击【开始处理】\n4. 系统会自动先处理，再继续上传\n\n提示：文件较大时请耐心等待，处理中尽量不要关闭窗口。",
            "使用帮助",
            parent=self,
        )

    def _show_about(self) -> None:
        Messagebox.show_info(
            "私域营销数据传输工具\n\n本工具支持一次加入多个 Excel，先处理，再自动上传到对应 webhook。\n当前版本已经支持批量会话、分阶段处理和结果追踪。",
            "关于",
            parent=self,
        )


def run_app() -> None:
    app = App()
    app.mainloop()

