for /f "usebackq tokens=1-2 delims=," %%a in ("volumes.csv") do (
      echo %%a %%b )