docker pull mcr.microsoft.com/azure-storage/azurite
docker volume create AZURE-01_DATA
docker run --name AZURE-01 -p 10000:10000 -v AZURE-01_DATA:/data -d  mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0 --blobPort 10000
docker network connect YADAMU-NET AZURE-01
docker pull minio/minio
docker volume create MINIO-01_DATA
docker run --name MINIO-01 -p 9000:9000   -v MINIO-01_DATA:/data -d -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" minio/minio server /data
docker network connect YADAMU-NET MINIO-01