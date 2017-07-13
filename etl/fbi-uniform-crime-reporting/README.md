## Hadoop Map Reduce code to merge `fbi-crime-data` and ICPSR `agency to county mapping data`

### Requirements:
- Apache Commons CSV library. [download link](https://commons.apache.org/proper/commons-csv/).  

### Usage:
- Place the apache csv jar in the /user/user-id/lib directory.
- Then jar will be dynamically added to the classpath in FBIDataETLRunner.java using `job.addFileToClassPath(path_to_jar)`.
- While compiling FBIDataETLMapper.java, csv jar has to be added to the java classpath using following command.
- Commands:   
```
javac -classpath `yarn classpath`:commons-csv-1.4.jar -d . FBIDataETLMapper.java
javac -classpath `yarn classpath` -d . FBIDataETLReducer.java
javac -classpath `yarn classpath`:. -d . FBIDataETLRunner.java

jar -cvf fbi-data-etl.jar *.class

hadoop jar fbi-data-etl.jar FBIDataETLRunner path-to-data output-path
```
