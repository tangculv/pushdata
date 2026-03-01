"""
主程序入口

启动图形用户界面应用程序。
"""

import os
import sys

from siyu_etl.ui.app import run_app


def main() -> None:
    """
    主函数：启动应用程序

    创建并运行图形用户界面。
    """
    # 打包为 exe 时，将工作目录设为 exe 所在目录，保证配置/DB 与 exe 同目录
    if getattr(sys, "frozen", False):
        os.chdir(os.path.dirname(sys.executable))
    run_app()


if __name__ == "__main__":
    main()


