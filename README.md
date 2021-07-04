# belajar_spark_streaming
belajar spark streaming digital skola

command console 1:
- docker-compose up -d
- py kafka_data_producer.py

INPUT DATA:
![input_data_streaming](https://user-images.githubusercontent.com/81307439/124395413-402f2e00-dd2e-11eb-8c26-f65893061756.JPG)

command console 2:
- spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark_structured_streaming.py

OUTPUT SPARK STRUCTURED STREAMING:

![output_spark_structured_streaming](https://user-images.githubusercontent.com/81307439/124395328-b717f700-dd2d-11eb-860c-eed34fab9e63.JPG).JPG)
