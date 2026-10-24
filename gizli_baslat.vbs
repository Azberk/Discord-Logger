Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
WshShell.Run "python app.py", 0, False
MsgBox "Discord Presence Radar arka planda (gizli modda) baslatildi!" & vbCrLf & vbCrLf & "Panel adresi: http://127.0.0.1:8050", 64, "Discord Presence Radar"
