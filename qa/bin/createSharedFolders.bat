set BASE_FOLDER=%1
if not exist %BASE_FOLDER%\              mkdir %BASE_FOLDER%
if not exist %BASE_FOLDER%\log\          mkdir %BASE_FOLDER%\log
if not exist %BASE_FOLDER%\longRegress\  mkdir %BASE_FOLDER%\longRegress
if not exist %BASE_FOLDER%\shortRegress\ mkdir %BASE_FOLDER%\shortRegress
if not exist %BASE_FOLDER%\stagingArea\  mkdir %BASE_FOLDER%\stagingArea
if not exist %BASE_FOLDER%\cmdLine\      mkdir %BASE_FOLDER%\cmdLine
if not exist %BASE_FOLDER%\output\       mkdir %BASE_FOLDER%\output
if not exist %BASE_FOLDER%\scratch\      mkdir %BASE_FOLDER%\scratch
if not exist %BASE_FOLDER%\test\         mkdir %BASE_FOLDER%\test
if not exist %BASE_FOLDER%\work\         mkdir %BASE_FOLDER%\work
