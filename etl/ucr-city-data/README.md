## Hadoop Map Reduce code to merge `ucr-city-crime-data` and Sunlight Foundation's cleaned `fines taxes data`

### Requirements:
- Apache Commons CSV library. [download link](https://commons.apache.org/proper/commons-csv/).  

### Usage:
- Place the apache csv jar in the /user/user-id/lib directory.
- Then jar will be dynamically added to the classpath in UCRCityCrimeDataRunner.java using `job.addFileToClassPath(path_to_jar)`.
- While compiling UCRCityCrimeDataMapper.java, csv jar has to be added to the java classpath using following command.
- Commands:   
```
javac -classpath `yarn classpath`:commons-csv-1.4.jar -d . UCRCityCrimeDataMapper.java
javac -classpath `yarn classpath` -d . UCRCityCrimeDataReducer.java
javac -classpath `yarn classpath`:. -d . UCRCityCrimeDataRunner.java

jar -cvf ucr-data-etl.jar *.class

hadoop jar ucr-data-etl.jar UCRCityCrimeDataRunner path-to-data output-path
```
