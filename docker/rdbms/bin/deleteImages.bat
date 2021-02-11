for /f "usebackq tokens=1-2 delims= " %%a in ("bin\images.txt") do (
  docker rmi %%a:%%b
)
docker images