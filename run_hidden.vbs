Set WshShell = CreateObject("WScript.Shell")
cwd = WshShell.CurrentDirectory
scriptPath = WScript.ScriptFullName
' Resolve script folder
scriptFolder = Left(scriptPath, Len(scriptPath) - Len(WScript.ScriptName))

' Change to repository folder (where the VBS lives) then run batch hidden
WshShell.CurrentDirectory = scriptFolder

' Adjust the batch filename below if you moved or renamed it
cmd = "cmd /c """ & scriptFolder & "autorun_cryptonews.bat""""

' 0 = hidden window, True = wait for completion
WshShell.Run cmd, 0, True
