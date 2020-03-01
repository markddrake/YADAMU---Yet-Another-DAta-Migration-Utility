FOR /f "tokens=*" %%i IN ('docker-machine env') DO @%%i                                                          
for /f "tokens=2 delims=:/" %%a in ("%DOCKER_HOST%") DO (SET DOCKER_IP_ADDR=%%a)
