from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import tkinter as tk
import ttkbootstrap as tb
from ttkbootstrap.constants import BOTH, BOTTOM, END, LEFT, RIGHT, TOP, X, Y, YES
from tkinter import Menu
from ttkbootstrap.dialogs import Messagebox

from siyu_etl.batch_service import BatchService
from siyu_etl.circuit_breaker import CircuitBreaker
from siyu_etl.config import AppConfig
from siyu_etl.constants import DEFAULT_CIRCUIT_BREAKER_THRESHOLD
from siyu_etl.db import clear_all_tasks, clear_batch_runtime_data
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

    def log(self, msg: str) -> None:
        ts = time.strftime("%H:%M:%S")
        self._log_q.put(f"[{ts}] {msg}")

    def progress(self, current: int, total: int, message: str = "") -> None:
        self._progress_q.put(ProgressSnapshot(current=current, total=total, message=message))

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


_dnd_tk_class = create_dnd_window_or_none()
_AppBase = _dnd_tk_class if _dnd_tk_class is not None else tb.Window


class App(_AppBase):
    COLOR_TEXT_PRIMARY = "#0F172A"
    COLOR_TEXT_SECONDARY = "#475569"
    COLOR_TEXT_MUTED = "#94A3B8"
    COLOR_ACCENT = "#1E3A5F"
    COLOR_GOLD = "#B6925B"
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

        self.geometry("1120x860")
        self.resizable(True, True)

        self.bus = UiBus()
        self._worker: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.breaker = CircuitBreaker(threshold=DEFAULT_CIRCUIT_BREAKER_THRESHOLD)
        self.batch_service = BatchService(self.config_obj.db_path)
        self._file_pool: list[FileQueueItem] = []
        self._selected_row_path: str = ""
        self._current_phase = "待开始"
        self._current_session_id = ""

        self.var_status_bar = tb.StringVar(value="请先把这次要处理的 Excel 文件放进来")
        self.var_phase = tb.StringVar(value="当前阶段：还没开始")
        self.var_summary_files = tb.StringVar(value="文件：0")
        self.var_summary_success = tb.StringVar(value="成功：0")
        self.var_summary_failed = tb.StringVar(value="失败：0")
        self.var_summary_uploaded = tb.StringVar(value="已上传：0")
        self.var_completion_note = tb.StringVar(value="")
        self.var_progress_hint = tb.StringVar(value="支持一次选多个文件，也可以后面继续添加")
        self.var_logs_visible = tb.BooleanVar(value=False)
        self.var_platform_key = tb.StringVar(value=str(self.config_obj.platform_key))
        self.var_push_mode = tk.StringVar(master=self, value=("preview" if bool(self.config_obj.dry_run) else "real"))
        self.var_archive = tb.BooleanVar(value=bool(self.config_obj.archive_to_processed_dir))

        try:
            self.var_push_mode.trace_add("write", lambda *_: self._on_change_push_mode())
        except Exception:
            pass

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
            style.configure("CompletionSuccess.TLabel", background="#F4EFE6", foreground=self.COLOR_GOLD, padding=12, font=("Helvetica", 11, "bold"))
            style.configure("CompletionWarning.TLabel", background="#F7F1E8", foreground="#A16207", padding=12, font=("Helvetica", 11, "bold"))
            style.configure("CompletionMuted.TLabel", background="#F3F4F6", foreground=self.COLOR_TEXT_SECONDARY, padding=12, font=("Helvetica", 11, "bold"))
            style.configure("HeaderCaption.TLabel", background="#FFFFFF", foreground=self.COLOR_GOLD, font=("Helvetica", 10))
            style.configure("HeaderTitle.TLabel", background="#FFFFFF", foreground=self.COLOR_TEXT_PRIMARY, font=("Helvetica", 22, "bold"))
            style.configure("HeaderStatus.TLabel", background="#FFFFFF", foreground=self.COLOR_TEXT_SECONDARY, font=("Helvetica", 10))
        except Exception:
            pass

    def _build_ui(self) -> None:
        root = tb.Frame(self, padding=22, bootstyle="light")
        root.pack(fill=BOTH, expand=True)

        header = tb.Frame(root, bootstyle="light")
        header.pack(side=TOP, fill=X, pady=(0, 16))
        header_card = tb.Frame(header, bootstyle="light")
        header_card.pack(fill=X)
        tb.Label(header_card, text="私域营销数据传输工具", style="HeaderCaption.TLabel").pack(anchor="w")
        tb.Label(header_card, text="批量文件上传", style="HeaderTitle.TLabel").pack(anchor="w", pady=(2, 0))
        tb.Label(header_card, textvariable=self.var_status_bar, style="HeaderStatus.TLabel").pack(anchor="w", pady=(6, 0))

        action_row = tb.Frame(root, bootstyle="light")
        action_row.pack(side=TOP, fill=X, pady=(0, 12))
        tb.Button(action_row, text="选择文件", command=self._on_pick_files, bootstyle="outline-dark").pack(side=LEFT)
        tb.Button(action_row, text="继续添加", command=self._on_add_more_files, bootstyle="outline-dark").pack(side=LEFT, padx=(10, 0))
        self.btn_process = tb.Button(action_row, text="开始处理", command=self._on_start_process, bootstyle="dark", width=14)
        self.btn_process.pack(side=LEFT, padx=(14, 0))
        tb.Button(action_row, text="停止", command=self._on_stop, bootstyle="outline-dark").pack(side=LEFT, padx=(8, 0))

        summary_row = tb.Frame(root, bootstyle="light")
        summary_row.pack(side=TOP, fill=X, pady=(0, 12))
        summary_left = tb.Frame(summary_row, bootstyle="light")
        summary_left.pack(side=LEFT, fill=X, expand=YES)
        tb.Label(summary_left, textvariable=self.var_phase, font=("Helvetica", 11, "bold"), foreground=self.COLOR_TEXT_PRIMARY).pack(side=LEFT)
        tb.Label(summary_left, text="｜", foreground=self.COLOR_TEXT_MUTED).pack(side=LEFT, padx=10)
        tb.Label(summary_left, textvariable=self.var_summary_files, foreground=self.COLOR_TEXT_SECONDARY).pack(side=LEFT)
        tb.Label(summary_left, text="｜", foreground=self.COLOR_TEXT_MUTED).pack(side=LEFT, padx=10)
        tb.Label(summary_left, textvariable=self.var_summary_success, foreground=self.COLOR_TEXT_SECONDARY).pack(side=LEFT)
        tb.Label(summary_left, text="｜", foreground=self.COLOR_TEXT_MUTED).pack(side=LEFT, padx=10)
        tb.Label(summary_left, textvariable=self.var_summary_failed, foreground=self.COLOR_TEXT_SECONDARY).pack(side=LEFT)
        self.btn_clear_selected = tb.Button(summary_row, text="移除选中", command=self._on_remove_selected, bootstyle="link")
        self.btn_clear_selected.pack(side=RIGHT)

        progress_row = tb.Frame(root, bootstyle="light")
        progress_row.pack(side=TOP, fill=X, pady=(0, 10))
        self.progress_bar = tb.Progressbar(progress_row, maximum=100, value=0, bootstyle="warning-striped")
        self.progress_bar.pack(fill=X)
        self.progress_label = tb.Label(root, text="等待开始", foreground=self.COLOR_TEXT_SECONDARY)
        self.progress_label.pack(anchor="w", pady=(0, 6))
        self.completion_wrap = tb.Frame(root, bootstyle="light")
        self.completion_wrap.pack(fill=X, pady=(0, 10))
        self.completion_label = tb.Label(self.completion_wrap, textvariable=self.var_completion_note, style="CompletionSuccess.TLabel")
        self.completion_label.pack(anchor="w", fill=X)
        self.completion_wrap.pack_forget()

        list_box = tb.Frame(root, bootstyle="light")
        list_box.pack(side=TOP, fill=BOTH, expand=True, pady=(0, 10))

        columns = ("file_name", "status", "uploaded_rows", "error")
        self.files_tree = tb.Treeview(list_box, columns=columns, show="headings", height=14, style="Luxury.Treeview")
        headings = {
            "file_name": "文件名",
            "status": "结果",
            "uploaded_rows": "已上传",
            "error": "问题说明",
        }
        widths = {
            "file_name": 420,
            "status": 120,
            "uploaded_rows": 100,
            "error": 420,
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

        self.logs_frame = tb.Frame(root, bootstyle="light")
        self.logs_frame.pack(side=TOP, fill=X, pady=(0, 8))
        self.log_text = tb.Text(self.logs_frame, height=6, wrap="word", relief="flat", bd=0, background="#FCFBF8", foreground=self.COLOR_TEXT_SECONDARY, insertbackground=self.COLOR_TEXT_PRIMARY)
        self.log_text.pack(fill=X)
        self.log_text.configure(state="disabled")
        self.logs_frame.pack_forget()

        footer = tb.Frame(root, bootstyle="light")
        footer.pack(side=BOTTOM, fill=X, pady=(4, 0))
        self.footer = footer
        self.btn_more = tb.Button(footer, text="更多", command=self._show_more_menu, bootstyle="link")
        self.btn_more.pack(side=LEFT)
        self.btn_toggle_logs = tb.Button(footer, text="查看详细过程", command=self._toggle_logs, bootstyle="link")
        self.btn_toggle_logs.pack(side=RIGHT)

        self.more_menu = Menu(self, tearoff=0)
        self.more_menu.add_command(label="清空这次文件", command=self._on_clear_files)
        self.more_menu.add_command(label="清空本地记录", command=self._on_reset)
        self.more_menu.add_separator()
        self.more_menu.add_command(label="帮助", command=self._show_help)
        self.more_menu.add_command(label="导出记录", command=self._export_logs)

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
            self.logs_frame.pack_forget()
            self.var_logs_visible.set(False)
            self.btn_toggle_logs.configure(text="查看详细过程")
        else:
            self.logs_frame.pack(side=TOP, fill=X, pady=(0, 8), before=self.footer)
            self.var_logs_visible.set(True)
            self.btn_toggle_logs.configure(text="收起详细过程")


    def _on_change_push_mode(self) -> None:
        self.config_obj.dry_run = str(self.var_push_mode.get()) != "real"
        self._auto_save_config()

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
        self.config_obj.dry_run = str(self.var_push_mode.get()) != "real"
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
        self._current_phase = "待开始"
        self._append_files(files, replacing=True)

    def _append_files(self, files: list[Path], replacing: bool = False) -> None:
        xlsx_files = [Path(p) for p in files if Path(p).suffix.lower() == ".xlsx"]
        if not xlsx_files:
            if files:
                self.bus.log("本次没有可用的 .xlsx 文件")
            return

        existing = {str(item.path.resolve()) for item in self._file_pool}
        added = 0
        duplicate = 0
        for fp in xlsx_files:
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

    def _on_clear_files(self) -> None:
        if not self._file_pool:
            return
        if not Messagebox.okcancel("将清空本次已选择的文件，是否继续？", "确认清空", parent=self):
            return
        self._file_pool = []
        self._current_session_id = ""
        self._current_phase = "待开始"
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
            "PARSING": "检查中",
            "PARSE_SUCCESS": "即将上传",
            "READY_TO_UPLOAD": "即将上传",
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
                "PARSING": "检查中",
                "PARSED": "即将上传",
                "UPLOADING": "上传中",
                "COMPLETED": "已完成",
                "PARTIAL_FAILED": "已完成",
                "FAILED": "已完成",
                "STOPPED": "已停止",
            }
            self._current_phase = status_map.get(summary.status, self._current_phase)
            if self._current_phase == "即将上传" and any(item.status == "上传中" for item in self._file_pool):
                self._current_phase = "上传中"
        self._refresh_file_tree()
        self._refresh_summary()

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
        self.var_summary_uploaded.set(f"已上传：{total_uploaded}")
        self.var_completion_note.set("")
        try:
            self.completion_label.configure(style="CompletionSuccess.TLabel")
            self.completion_wrap.pack_forget()
        except Exception:
            pass

        if total_files > 0 and self._current_phase == "已完成":
            if failed_files == 0:
                self.progress_label.configure(text=f"这次共 {total_files} 个文件，已全部处理完成")
                self.var_completion_note.set("全部完成，可以直接继续下一批")
                self.completion_label.configure(style="CompletionSuccess.TLabel")
                self.completion_wrap.pack(fill=X, pady=(0, 10), before=self.files_tree.master)
            else:
                self.progress_label.configure(text=f"这次共 {total_files} 个文件，成功 {success_files} 个，失败 {failed_files} 个")
                self.var_completion_note.set("有文件没处理成功，请看列表里的问题说明")
                self.completion_label.configure(style="CompletionWarning.TLabel")
                self.completion_wrap.pack(fill=X, pady=(0, 10), before=self.files_tree.master)

        if total_files == 0:
            self.var_status_bar.set("请先把这次要处理的 Excel 文件放进来")
            self.var_progress_hint.set("支持一次选多个文件，也可以后面继续添加")
            self.progress_label.configure(text="等待开始")
            self._set_process_button_text("开始处理")
        elif self._current_phase == "待开始":
            self.var_status_bar.set(f"已经放入 {total_files} 个文件，确认后点一次开始就行")
            self.var_progress_hint.set("系统会自动检查并上传；如果文件还没加全，可以继续添加")
            self.progress_label.configure(text="等待开始")
            self._set_process_button_text("开始处理")
        elif self._current_phase == "检查中":
            self.var_status_bar.set(f"正在检查文件，请稍候（共 {total_files} 个）")
            self.var_progress_hint.set("文件较大时会比较久，请保持窗口开启")
            self._set_process_button_text("检查中...")
        elif self._current_phase == "已停止":
            self.var_status_bar.set("本次操作已停止，可继续调整文件后重新开始")
            self.var_progress_hint.set("已完成的结果会保留，未完成的文件可以重新检查或重新上传")
            self.var_completion_note.set("本次已停止")
            self.completion_label.configure(style="CompletionMuted.TLabel")
            self.completion_wrap.pack(fill=X, pady=(0, 10), before=self.files_tree.master)
            self._set_process_button_text("重新处理")
        elif self._current_phase == "即将上传":
            self.var_status_bar.set("文件检查完成，马上开始上传")
            self.var_progress_hint.set("不用再操作，系统会自动继续")
            self._set_process_button_text("准备上传...")
        elif self._current_phase == "上传中":
            self.var_status_bar.set(f"正在上传文件，请稍候（共 {total_files} 个）")
            self.var_progress_hint.set("上传过程中请不要关闭窗口")
            self._set_process_button_text("上传中...")
        elif self._current_phase == "已完成":
            if failed_files == 0:
                self.var_status_bar.set(f"本次已全部完成，共成功处理 {success_files} 个文件")
                self.var_progress_hint.set("可以关闭窗口，或继续添加新文件")
            else:
                self.var_status_bar.set(f"本次处理结束：成功 {success_files} 个，失败 {failed_files} 个")
                self.var_progress_hint.set("请看列表里的问题说明，处理后再重新开始")
            self._set_process_button_text("继续处理新文件")

    def _on_start_process(self) -> None:
        if self._worker and self._worker.is_alive():
            self.bus.log("当前已有任务在运行")
            return
        if not self._file_pool:
            Messagebox.show_warning("请先选择至少一个 Excel 文件", "提示", parent=self)
            return
        if not Messagebox.okcancel("系统会自动先检查，再继续上传。确认开始处理吗？", "开始处理", parent=self):
            return

        self._start_parse_stage()

    def _start_parse_stage(self) -> None:
        self._sync_config_from_ui()
        self._stop_event.clear()
        self._current_phase = "检查中"
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
        self.progress_label.configure(text="正在准备处理")
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
            self._current_session_id = stats.session_id
            self._refresh_from_session()
            self._current_phase = "即将上传"
            self._refresh_summary()
            self.bus.log("文件检查完成，开始继续上传")
            self._start_upload_stage(auto_started=True)
            return
        except Exception as e:
            self._current_phase = "已完成"
            self.progress_label.configure(text="处理未完成")
            self.bus.log(f"文件检查失败: {e}")
        finally:
            self._refresh_from_session()

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
        self._current_phase = "上传中"
        self._refresh_summary()
        if auto_started:
            self.bus.log("检查通过，系统已自动继续上传")
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
            self._refresh_from_session()
            self.bus.log("上传阶段完成")
            self.progress_bar.configure(value=100)
            self.progress_label.configure(text="本次处理完成")
        except Exception as e:
            self._current_phase = "已完成"
            self.progress_label.configure(text="处理未完成")
            self.bus.log(f"上传失败: {e}")
        finally:
            self._refresh_from_session()

    def _on_stop(self) -> None:
        if not (self._worker and self._worker.is_alive()):
            self.bus.log("当前没有运行中的任务")
            return
        self._stop_event.set()
        self._current_phase = "已停止"
        self.progress_label.configure(text="正在停止，请稍候")
        self.bus.log("已请求停止当前操作")

    def _on_reset(self) -> None:
        if not Messagebox.okcancel("将清空本地待处理数据、本次批量记录，并解除熔断状态，是否继续？", "确认重置", parent=self):
            return
        try:
            clear_all_tasks(self.config_obj.db_path)
            clear_batch_runtime_data(self.config_obj.db_path)
            self.breaker.reset()
            self._current_session_id = ""
            self._current_phase = "待开始"
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
            self.log_text.insert(END, line + "\n")
            self.log_text.see(END)
            self.log_text.configure(state="disabled")

        snaps = self.bus.drain_progress()
        if snaps:
            snap = snaps[-1]
            pct = int((snap.current / snap.total) * 100) if snap.total > 0 else 0
            self.progress_bar.configure(value=pct)
            label = snap.message or f"{snap.current}/{snap.total}"
            self.progress_label.configure(text=label)
            if self._current_phase == "检查中":
                self.var_status_bar.set(f"正在检查第 {snap.current}/{max(snap.total, 1)} 个文件")
                self.var_progress_hint.set("文件较大时会比较久，请保持窗口开启")
            elif self._current_phase == "上传中":
                self.var_status_bar.set(f"正在上传第 {snap.current}/{max(snap.total, 1)} 个文件")
                self.var_progress_hint.set("上传过程中请不要关闭窗口")

        if self._current_session_id:
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
            "使用步骤：\n1. 先把这次要处理的 Excel 文件加进来\n2. 如果没加全，可以继续添加，系统会自动去重\n3. 点击【开始处理】\n4. 系统会自动先检查，再继续上传\n\n提示：文件较大时请耐心等待，处理中尽量不要关闭窗口。",
            "使用帮助",
            parent=self,
        )

    def _show_about(self) -> None:
        Messagebox.show_info(
            "私域营销数据传输工具\n\n本工具支持一次加入多个 Excel，先检查，再按配置上传到对应 webhook。\n当前版本已经支持批量会话、分阶段处理和结果追踪。",
            "关于",
            parent=self,
        )


def run_app() -> None:
    app = App()
    app.mainloop()
