@echo off
chcp 65001 >nul
title Discord Presence Radar & Stalker
echo ========================================================
echo   Discord Presence Radar Baslatiliyor...
echo   Tarayicinizda http://127.0.0.1:8050 adresini acin
echo ========================================================
start http://127.0.0.1:8050
python app.py
pause
