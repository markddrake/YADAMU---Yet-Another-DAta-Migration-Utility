set DCONFIG=%CD%
for /f "usebackq tokens=1 delims=," %%a in ("%DCONFIG%\bin\containers.csv") do (
  docker container rm %%a
)
docker ps -a
for /f "usebackq tokens=1 delims=," %%a in ("%DCONFIG%\bin\volumes.csv") do (
  docker volume rm %%a
)
docker volume prune
docker volume ls
docker network rm YADAMU-NET