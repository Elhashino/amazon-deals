@echo off
rem Double-click to follow alerted coins and log what happens to them.
rem Free to run: DexScreener only, no Solana RPC, no Helius credits.
rem Uses the py launcher so this picks a modern Python (3.11+) rather than
rem whatever old version happens to own the "python" name on PATH.
cd /d "%~dp0"
where py >nul 2>&1
if %errorlevel%==0 (
    py -3 track.py %*
) else (
    python track.py %*
)
pause
