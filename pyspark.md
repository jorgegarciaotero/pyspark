#### PYSPARK

#Docker installation and execution
docker pull bitnami/spark:latest
docker run --name spark bitnami/spark:latest
docker run --name spark -p 8080:8080 -p 7077:7077 bitnami/spark:latest

#Copy and execute a script
docker cp test.py spark:/opt/bitnami/spark/test.py    
docker exec -it spark bash
spark-submit /opt/bitnami/spark/test.py
