@echo off
REM OpenFlux Build Script for Windows
REM Builds Linux and Windows AMD64 binaries

setlocal enabledelayedexpansion

echo === Building OpenFlux ===
echo.

REM Clean old binaries
if exist openflux-linux-amd64 del /f openflux-linux-amd64
if exist openflux-windows-amd64.exe del /f openflux-windows-amd64.exe

REM Build Windows AMD64
echo Building for Windows AMD64...
set CGO_ENABLED=0
set GOOS=windows
set GOARCH=amd64
go build -ldflags="-s -w" -trimpath -o openflux-windows-amd64.exe .

if !errorlevel! equ 0 (
    echo SUCCESS: openflux-windows-amd64.exe built successfully
    dir openflux-windows-amd64.exe
) else (
    echo FAILED: Could not build Windows binary
    exit /b 1
)

echo.

REM Build Linux AMD64 (cross-compile)
echo Building for Linux AMD64...
set CGO_ENABLED=0
set GOOS=linux
set GOARCH=amd64
go build -ldflags="-s -w" -trimpath -o openflux-linux-amd64 .

if !errorlevel! equ 0 (
    echo SUCCESS: openflux-linux-amd64 built successfully
    dir openflux-linux-amd64
) else (
    echo FAILED: Could not build Linux binary
    exit /b 1
)

echo.
echo === Build Complete ===
echo.
echo Usage examples:
echo   Linux:   ./openflux-linux-amd64 --exit-node --transport yandex --urls "DOC1,DOC2,DOC3" --mode proxy
echo   Windows: .\openflux-windows-amd64.exe --exit-node --transport yandex --urls "DOC1,DOC2,DOC3" --mode proxy
echo.

endlocal
