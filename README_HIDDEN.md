Running the autorun script invisibly

This repository includes `autorun_cryptonews.bat` which activates the virtualenv and runs several Python scripts.

If you added the batch to Windows Task Scheduler it will open a visible console window when it runs. To run it fully hidden, use the included `run_hidden.vbs` wrapper which launches the batch with its window hidden.

Quick steps

1. Place `run_hidden.vbs` in the same folder as `autorun_cryptonews.bat` (already included in this repo).
2. In Task Scheduler create or update your task to run:

   - Program/script: wscript.exe
   - Add arguments (optional): "C:\\Users\\akona\\OneDrive\\Dev\\crytpo-news-au\\run_hidden.vbs"

   You can also use `cscript.exe` but `wscript.exe` is preferable for GUI-less runs.

3. In the General tab choose "Run whether user is logged on or not" and (optionally) check "Run with highest privileges" if your scripts need elevated permissions.

Caveats and notes

- When running "whether user is logged on or not" the task may run in session 0. GUI or interactive elements (like message boxes) won't be visible and mapped network drives may not be available. Use UNC paths or ensure credentials are available.
- If your scripts rely on environment variables from an interactive session, set them explicitly in the batch or use absolute paths.
- If the scheduled task must run under a different account you must supply that account's credentials in Task Scheduler.
- If you need robust service-like behavior consider installing the Python runner as a Windows service (e.g., using NSSM: https://nssm.cc) or converting the scripts to a proper service.

Alternative: Run PowerShell hidden

You can also create a small PowerShell script to start the batch hidden. Example to add as the scheduled program:

  Program/script: powershell.exe
  Arguments: -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File "C:\\Users\\akona\\OneDrive\\Dev\\crytpo-news-au\\run_hidden_ps.ps1"

If you prefer that approach I can add `run_hidden_ps.ps1` to the repo.

Security

Be careful storing credentials in scheduled tasks. Prefer using Task Scheduler's account storage and avoid plaintext passwords in scripts.

If you want, I can also:
- Add a `run_hidden_ps.ps1` PowerShell wrapper that validates paths and logs output to a file.
- Add logging to `autorun_cryptonews.bat` so you have visibility into runs when it's hidden.
