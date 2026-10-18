@echo off
REM Build the public GitHub tree from the committed oak repo. Dry run unless --apply is given.
REM Usage: publish.bat [--ref master] [--apply] [--verbose] [--report file]   (see publish.mjs)
node "%~dp0publish.mjs" %*
