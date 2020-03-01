set DCONFIG=%CD%
for /f "usebackq tokens=1 delims=," %%a in ("%DCONFIG%\bin\containers.csv") do (
  docker container start %%a
)
docker ps -a