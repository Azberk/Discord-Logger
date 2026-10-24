@echo off
chcp 65001 >nul
title Bulut Sunucusunu Guncelle (Git Push)
echo ========================================================
echo   Degisiklikler Bulut Sunucusuna (GitHub / Cloud) Gonderiliyor...
echo ========================================================
git add .
git commit -m "Guncelleme: %date% %time%"
git push
echo.
echo [BASARILI] Degisiklikler gonderildi! Bulut sunucunuz 1 dakika icinde otomatik guncellenecektir.
pause
