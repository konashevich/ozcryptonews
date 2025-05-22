REM @echo off
cd /d "%~dp0"

REM Activate the virtual environment
call "venv\Scripts\activate.bat"

REM Run Python scripts
python asic.py
python ausblock.py
python austrac.py
python australiandefiassociation.py
python australianfintech.py
python coindesk.py
python cointelegraph.py
python cryptonews.py
python decrypt.py
python regtechglobal.py
python dfcrc.py
python web3au.py
python telegrambotsender.py
git_commit_push.py
