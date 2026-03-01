"""
配置对话框模块

提供 Webhook URL 配置界面。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import ttkbootstrap as tb
from ttkbootstrap.constants import BOTH, LEFT, RIGHT, TOP, X
from ttkbootstrap.dialogs import Messagebox

from siyu_etl.config import AppConfig, Webhooks


@dataclass
class ConfigDialogResult:
    """配置对话框结果"""
    webhooks: Optional[Webhooks] = None
    batch_size: Optional[int] = None
    request_timeout_seconds: Optional[int] = None
    cancelled: bool = False


class WebhookConfigDialog:
    """Webhook 配置对话框"""

    def __init__(self, parent, config: AppConfig) -> None:
        """
        初始化配置对话框

        Args:
            parent: 父窗口
            config: 当前配置
        """
        self.parent = parent
        self.config = config
        self.result = ConfigDialogResult(cancelled=True)

        # 创建对话框窗口
        self.dialog = tb.Toplevel(parent)
        self.dialog.title("Webhook 配置")
        self.dialog.geometry("700x500")
        self.dialog.resizable(True, True)
        self.dialog.transient(parent)
        self.dialog.grab_set()

        # 创建变量
        self.webhook_vars: dict[str, tb.StringVar] = {}
        self.batch_size_var = tb.IntVar(value=config.batch_size)
        self.timeout_var = tb.IntVar(value=config.request_timeout_seconds)

        self._build_ui()

    def _build_ui(self) -> None:
        """构建 UI"""
        root = tb.Frame(self.dialog, padding=12)
        root.pack(fill=BOTH, expand=True)

        # 说明
        info = tb.Label(
            root,
            text="配置各数据源对应的 Webhook URL。修改后需要保存配置才会生效。",
            foreground="gray",
        )
        info.pack(side=TOP, fill=X, pady=(0, 12))

        # 创建滚动区域
        canvas = tb.Canvas(root)
        scrollbar = tb.Scrollbar(root, orient="vertical", command=canvas.yview)
        scrollable_frame = tb.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")),
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        # Webhook 配置项
        webhook_labels = {
            "member_trade_detail": "会员交易明细",
            "in_store_order_detail": "店内订单明细(已结账)",
            "income_discount_stat": "收入优惠统计",
            "coupon_stat": "优惠券统计表",
            "member_storage_analysis": "会员储值消费分析表",
            "member_card_export": "会员卡导出",
        }

        webhook_frame = tb.Labelframe(scrollable_frame, text="Webhook URL 配置", padding=12)
        webhook_frame.pack(side=TOP, fill=X, pady=(0, 12))

        for key, label in webhook_labels.items():
            row = tb.Frame(webhook_frame)
            row.pack(side=TOP, fill=X, pady=(0, 8))

            tb.Label(row, text=f"{label}:", width=20, anchor="w").pack(side=LEFT)
            var = tb.StringVar(value=getattr(self.config.webhooks, key, ""))
            self.webhook_vars[key] = var
            entry = tb.Entry(row, textvariable=var, width=60)
            entry.pack(side=LEFT, fill=X, expand=True, padx=(8, 0))

        # 其他配置
        other_frame = tb.Labelframe(scrollable_frame, text="其他配置", padding=12)
        other_frame.pack(side=TOP, fill=X, pady=(0, 12))

        # 批次大小
        batch_row = tb.Frame(other_frame)
        batch_row.pack(side=TOP, fill=X, pady=(0, 8))
        tb.Label(batch_row, text="批次大小:", width=20, anchor="w").pack(side=LEFT)
        batch_spin = tb.Spinbox(
            batch_row,
            from_=1,
            to=1000,
            textvariable=self.batch_size_var,
            width=20,
        )
        batch_spin.pack(side=LEFT, padx=(8, 0))
        tb.Label(batch_row, text="条/包", foreground="gray").pack(side=LEFT, padx=(8, 0))

        # 超时时间
        timeout_row = tb.Frame(other_frame)
        timeout_row.pack(side=TOP, fill=X)
        tb.Label(timeout_row, text="请求超时:", width=20, anchor="w").pack(side=LEFT)
        timeout_spin = tb.Spinbox(
            timeout_row,
            from_=5,
            to=300,
            textvariable=self.timeout_var,
            width=20,
        )
        timeout_spin.pack(side=LEFT, padx=(8, 0))
        tb.Label(timeout_row, text="秒", foreground="gray").pack(side=LEFT, padx=(8, 0))

        # 按钮
        buttons = tb.Frame(root)
        buttons.pack(side=TOP, fill=X, pady=(12, 0))

        tb.Button(
            buttons,
            text="取消",
            command=self._on_cancel,
        ).pack(side=RIGHT, padx=(8, 0))

        tb.Button(
            buttons,
            text="保存",
            command=self._on_save,
            bootstyle="primary",
        ).pack(side=RIGHT)

        # 配置滚动
        canvas.pack(side=LEFT, fill=BOTH, expand=True)
        scrollbar.pack(side=RIGHT, fill="y")

    def _on_save(self) -> None:
        """保存配置"""
        try:
            # 验证批次大小
            batch_size = self.batch_size_var.get()
            if batch_size < 1 or batch_size > 1000:
                Messagebox.show_error("批次大小必须在 1-1000 之间", "配置错误", parent=self.dialog)
                return

            # 验证超时时间
            timeout = self.timeout_var.get()
            if timeout < 5 or timeout > 300:
                Messagebox.show_error("超时时间必须在 5-300 秒之间", "配置错误", parent=self.dialog)
                return

            # 验证 Webhook URL
            webhooks_dict = {}
            for key, var in self.webhook_vars.items():
                url = var.get().strip()
                if url and not (url.startswith("http://") or url.startswith("https://")):
                    Messagebox.show_error(
                        f"{key} 的 URL 必须以 http:// 或 https:// 开头",
                        "配置错误",
                        parent=self.dialog,
                    )
                    return
                webhooks_dict[key] = url

            # 创建结果
            self.result = ConfigDialogResult(
                webhooks=Webhooks(**webhooks_dict),
                batch_size=batch_size,
                request_timeout_seconds=timeout,
                cancelled=False,
            )

            self.dialog.destroy()
        except Exception as e:
            Messagebox.show_error(f"保存配置失败: {e}", "错误", parent=self.dialog)

    def _on_cancel(self) -> None:
        """取消"""
        self.dialog.destroy()

    def show(self) -> ConfigDialogResult:
        """
        显示对话框并返回结果

        Returns:
            ConfigDialogResult 对象
        """
        self.dialog.wait_window()
        return self.result
