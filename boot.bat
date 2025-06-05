@echo off
for /l %%i in (0,1,6) do (
    setlocal enabledelayedexpansion
    set "num=0%%i"
    set "num=!num:~-2!"
    start "storage_!num!" cmd /c python storage.py --config=./docs/storage-!num!.yaml
    endlocal
)

for /l %%i in (0,1,2) do (
    setlocal enabledelayedexpansion
    set "num=0%%i"
    set "num=!num:~-2!"
    start "gateway_!num!" cmd /c python gateway.py --config=./docs/gateway-!num!.yaml
    endlocal
)
