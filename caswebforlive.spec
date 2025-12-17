# Run: pyinstaller caswebforlive.spec

from PyInstaller.utils.hooks import collect_submodules

block_cipher = None

datas = [
    ('templates', 'templates'),
    ('static', 'static'),
    ('db_config.ini', '.'),          # optional: include a default
]

hiddenimports = []
hiddenimports += collect_submodules('routes')
hiddenimports += collect_submodules('flask_caching')
hiddenimports += collect_submodules('pymysql')
hiddenimports += collect_submodules('cryptography')
hiddenimports += collect_submodules('waitress')


a = Analysis(
    ['run.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='Coretally',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    icon='apcon.ico',
)