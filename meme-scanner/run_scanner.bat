@echo off
rem Double-click to start the scanner on Windows.
rem Uses the py launcher so this picks a modern Python (3.11+) rather than
rem whatever old version happens to own the "python" name on PATH.
cd /d "%~dp0"
where py >nul 2>&1
if %errorlevel%==0 (
    py -3 scanner.py %*
) else (
    python scanner.py %*
)
pause
