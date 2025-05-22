@echo off
cd /d "%~dp0"
call ".venv\Scripts\activate.bat"

REM Redirect output to log file
python asic.py >> log.txt 2>&1
python ausblock.py >> log.txt 2>&1
python austrac.py >> log.txt 2>&1
python australiandefiassociation.py >> log.txt 2>&1
python australianfintech.py >> log.txt 2>&1
python coindesk.py >> log.txt 2>&1
python cointelegraph.py >> log.txt 2>&1
python cryptonews.py >> log.txt 2>&1
python decrypt.py >> log.txt 2>&1
python regtechglobal.py >> log.txt 2>&1
python dfcrc.py >> log.txt 2>&1
python web3au.py >> log.txt 2>&1
python telegrambotsender.py >> log.txt 2>&1
python commit_and_push_articles.py >> log.txt 2>&1

pause