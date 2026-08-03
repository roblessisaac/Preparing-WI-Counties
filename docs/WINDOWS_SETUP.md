# Windows Setup Notes

## Recommended installation

Install 64-bit Python 3.12 from python.org and select **Add Python to PATH** during installation.

## PowerShell execution policy

If PowerShell blocks virtual-environment activation, run this once for the current user:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Then activate:

```powershell
.\.venv\Scripts\Activate.ps1
```

## Free disk space

Keep at least 5–10 GB free for extraction, full-fidelity files, runtime files, reports and temporary validation copies.

## GitHub Desktop

GitHub Desktop can be used instead of command-line Git:

1. Create the repository on GitHub.
2. Clone it through GitHub Desktop.
3. Copy the starter files into the cloned folder.
4. Commit only the code and documentation.
5. Confirm the source ZIP and output folders do not appear in the commit list.
