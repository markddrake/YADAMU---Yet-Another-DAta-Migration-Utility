set BASE_FOLDER=%1
if exist  %BASE_FOLDER%\ rmdir /s /q %BASE_FOLDER%
mkdir %BASE_FOLDER%