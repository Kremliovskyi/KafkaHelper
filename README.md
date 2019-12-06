JavaFX app that currently supports only consuming records from Apache Kafka.

1. Records displayed have keys, headers and values that are pretty printed JSON strings.
2. Each record has counter at the beginning that ease to see how many records were fetched.
3. App supports from 1 to 5 parallel consumers. 
4. If group rebalancing has started there is a mechanism that avoids records' duplicates.
5. Each time app starts new consumer group id is created.
6. If "Seek to end" checkbox is disabled consumers will fetch from beginning of all partitions and vice versa.
7. If "Is topic Avro?" checkbox is enabled then you must provide Schema Registry IP and data from Avro topic will be fetched. 
8. It is possible to select the whole record by clicking on each element or you may use buttons "Select All" , "Deselect All" to select/deselect all records. To copy elements you should right click of the mouse button and select "Copy".
9. You may open debug kafka logs with the corresponding "Open Logs" button. File with logs is stored in user temp folder and is named kafka-debug.log. File will be opened automatically in the default app for this type of file - .log
10. "Clear" button will only clear the records pane without disrupting consumption.
11. It is highly advisable to first stop consumer group prior to closing the app.

Avro consumption in progress, one record selected for copying:
![Avro consumption in progress, one record selected for copying](https://github.com/Kremliovskyi/KafkaHelper/blob/master/src/test/resources/working-consumers.png)

Regular topic consumption was stopped:
![Regular topic consumption was stopped](https://github.com/Kremliovskyi/KafkaHelper/blob/master/src/test/resources/metrics-raw.png)


