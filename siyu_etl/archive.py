"""
文件归档模块

该模块负责将处理完成的 Excel 文件归档到 processed 目录。
"""

from __future__ import annotations

import shutil
from pathlib import Path


def archive_file(
    file_path: Path,
    *,
    to_processed_dir: bool = True,
    suffix: str = "_processed",
) -> Path:
    """
    归档文件：移动到 ./processed 目录并在扩展名前添加后缀
    
    如果目标文件已存在，会自动添加数字后缀以避免覆盖。
    
    Args:
        file_path: 要归档的文件路径
        to_processed_dir: 是否移动到 processed 目录（默认：True）
        suffix: 文件名后缀（默认："_processed"）
        
    Returns:
        新文件路径
        
    Raises:
        如果移动失败会抛出异常
    """
    p = Path(file_path)
    parent = p.parent
    target_dir = parent / "processed" if to_processed_dir else parent
    target_dir.mkdir(parents=True, exist_ok=True)

    new_name = f"{p.stem}{suffix}{p.suffix}"
    dst = target_dir / new_name

    # Avoid overwrite
    if dst.exists():
        for i in range(1, 1000):
            cand = target_dir / f"{p.stem}{suffix}_{i}{p.suffix}"
            if not cand.exists():
                dst = cand
                break

    shutil.move(str(p), str(dst))
    return dst


