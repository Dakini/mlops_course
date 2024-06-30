docker-compose up -d
sleep 1

aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration
sleep 1
aws --endpoint-url=http://localhost:4566 s3 ls

# docker-compose  down