"""
拖拽功能支持模块

该模块提供文件拖拽功能的支持，使用 tkinterdnd2 库（如果可用）。
如果拖拽功能不可用，应用程序会降级为使用文件选择对话框。

注意：拖拽支持是可选的，因为 tkinter 的 DnD 不是内置功能。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import tkinter as tk


@dataclass(frozen=True)
class DndSupport:
    """
    拖拽支持信息数据类
    
    Attributes:
        enabled: 是否启用拖拽功能
        reason: 启用或禁用的原因说明
    """
    enabled: bool
    reason: str


def detect_dnd_support() -> DndSupport:
    """
    Drag-and-drop support is optional because tkinter's DnD is not built-in.
    We use tkinterdnd2 if available, otherwise the app falls back to file dialog.
    """
    try:
        from tkinterdnd2 import TkinterDnD  # noqa: F401

        # Verify underlying tkdnd library is available WITHOUT creating a Tk root.
        # Creating/destroying extra Tk roots can break Variable bindings in the real app.
        interp = tk.Tcl()
        interp.call("package", "require", "tkdnd")
        return DndSupport(enabled=True, reason="tkinterdnd2 + tkdnd available")
    except Exception as e:  # pragma: no cover
        return DndSupport(enabled=False, reason=f"drag-and-drop disabled: {e}")


def create_dnd_window_or_none():
    """
    Returns a Tk subclass that supports DnD if tkinterdnd2 is installed AND
    can be initialized (i.e., the underlying tkdnd library is available), else None.
    
    We verify tkdnd availability via a Tcl interpreter (no GUI root).
    """
    try:
        from tkinterdnd2 import TkinterDnD  # type: ignore
        interp = tk.Tcl()
        interp.call("package", "require", "tkdnd")
        return TkinterDnD.Tk
    except Exception:
        # ImportError: tkinterdnd2 not installed
        # RuntimeError: tkdnd library cannot be loaded (the actual error we're seeing)
        # Exception: any other error during initialization
        return None


def parse_drop_files(data: str) -> list[Path]:
    """
    解析拖拽文件数据字符串
    
    tkinterdnd2 提供的字符串格式可能是：
      - '{/path/with space/a.xlsx} {/path/b.xlsx}' （多个文件，带空格路径用大括号包裹）
      - '/path/a.xlsx' （单个文件）
    
    Args:
        data: 拖拽事件提供的文件路径字符串
        
    Returns:
        文件路径列表
    """
    s = (data or "").strip()
    if not s:
        return []

    out: list[str] = []
    if s.startswith("{") and "}" in s:
        cur = []
        in_brace = False
        for ch in s:
            if ch == "{":
                in_brace = True
                cur = []
                continue
            if ch == "}":
                in_brace = False
                out.append("".join(cur))
                cur = []
                continue
            if in_brace:
                cur.append(ch)
        # If parsing failed, fall back below
    else:
        out = s.split()

    files: list[Path] = []
    for p in out:
        p = p.strip()
        if not p:
            continue
        files.append(Path(p))
    return files


def register_drop_target(
    widget,
    on_files: Callable[[list[Path]], None],
    on_error: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    将组件注册为拖拽目标
    
    如果 tkinterdnd2 可用，则注册组件为文件拖拽目标。
    当文件被拖拽到组件上时，会调用 on_files 回调。
    
    Args:
        widget: 要注册的 tkinter 组件
        on_files: 文件拖拽成功时的回调函数
        on_error: 错误处理回调函数（可选）
        
    Returns:
        如果成功注册则返回 True，否则返回 False
    """
    try:
        from tkinterdnd2 import DND_FILES  # type: ignore

        widget.drop_target_register(DND_FILES)

        def _handle(event):
            try:
                paths = parse_drop_files(getattr(event, "data", ""))
                on_files(paths)
            except Exception as e:
                if on_error:
                    on_error(f"解析拖拽文件失败: {e}")

        widget.dnd_bind("<<Drop>>", _handle)
        return True
    except Exception:
        return False


