# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for Siyu ETL Client (Windows one-dir build).
# Run on Windows: pyinstaller siyu.spec

block_cipher = None

a = Analysis(
    ["main.py"],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        "siyu_etl",
        "siyu_etl.ui",
        "siyu_etl.ui.app",
        "siyu_etl.ui.config_dialog",
        "siyu_etl.ui.dnd",
        "openpyxl",
        "requests",
        "ttkbootstrap",
        "tkinterdnd2",
    ],
    hookspath=["build/hooks"],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "pytest",
        "tests",
        "docs",
        "scripts",
        "data",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="SiyuETL",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # No black console window on Windows
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="SiyuETL",
)
