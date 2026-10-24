@echo off
chcp 65001 >nul
title GitHub Ilk Giris ve Yukleme
echo ========================================================
echo   GitHub Hesabiniz Dogrulaniyor ve Dosyalar Yukleniyor...
echo   (Acilan pencereden "Sign in with your browser" butonuna basin)
echo ========================================================
git branch -M main
git push -u origin main
echo.
echo ========================================================
echo   Islem tamamlandi!
echo ========================================================
pause
