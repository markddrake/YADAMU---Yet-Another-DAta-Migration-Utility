npx electron-packager ./src yadamu --platform=win32 --arch=x64   --out=c:\dist --overwrite --ignore="scratch" --ignore="sessions"
powershell compress-archive -force c:\dist\yadamu-win32-x64 c:\dist\yadamu-win32-x64.zip
oypy /Y c:\dist\yadamu-win32-x64.zip c:\deployment\YADAMU\dist
