### Spark test

#### To run locally
```bash
./gradlew run 
```
This will run SQL job that uses DataFrame Api with default arguments


#### To run using spark-submit jar in spark cluster
First build shadowJar
```bash
./gradlew shadowJar
```

Then run it
```bash
./bin/spark-submit --class spark.test.app.App --master spark://master:7077 spark-app-test-1.0-SNAPSHOT-all.jar \
--type SQL \
--start 2018-03-23 \
--end 2018-03-26 \
--metadata /path/to/ds1.csv \
--events /path/to/ds2_part \
--output /path/to/json \
--master spark://master:7077
```

Show usage
```bash
./bin/spark-submit --class spark.test.app.App spark-app-test-1.0-SNAPSHOT-all.jar --help
```

RDD job
```bash
./bin/spark-submit --class spark.test.app.App --master spark://master:7077 spark-app-test-1.0-SNAPSHOT-all.jar \
--type RDD \
--start 2018-03-23 \
--end 2018-03-26 \
--events /path/to/json/ \
--output /path/to/rdd_json/ \
--master spark://master:7077
```

Partition job
```bash
./bin/spark-submit --class spark.test.app.App --master spark://master:7077 spark-app-test-1.0-SNAPSHOT-all.jar \
--type PARTITION \
--events path/to/ds2.csv \
--output /path/to/paritioned_data \
--master spark://master:7077
``` 

Job types:
`RDD|SQL|PARTITION`

`RDD` - uses RDD Transformation to convert 15 minutes slots into hour slots, input for the job is the output of SQL job in json format \
`SQL` - uses DataFrame Api to generate output data set \
`PARTITION` - partition data into dates partitions