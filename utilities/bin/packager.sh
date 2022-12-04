cp -r /mnt/c/Deployment/YADAMU .
cd YADAMU/src
npm install
cd ..
npx electron-packager ./src yadamu --platform=linux --arch=x64  --out=./dist --overwrite --ignore="scratch" --ignore="sessions"
npx electron-packager ./src yadamu --platform=mas --arch=x64  --out=./dist --overwrite --ignore="scratch" --ignore="sessions"
# npx electron-packager ./src yadamu --platform=win32 --arch=x64  --out=./dist --overwrite --ignore="scratch" --ignore="sessions"
cd dist
zip -r yadamu-linux-x64.zip yadamu-linux-x64/
zip -r yadamu-mas-x64.zip yadamu-mas-x64/
# zip -r yadamu-win32-x64.zip yadamu-win32-x64/
# cp *.zip /mnt/c/Deployment/YADAMU/dist/
