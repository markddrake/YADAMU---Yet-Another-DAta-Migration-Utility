for /f "usebackq tokens=1-2 delims= " %%a in ("%DCONFIG%\bin\images.txt") do (
  docker rmi %%a:%%b
)
docker images