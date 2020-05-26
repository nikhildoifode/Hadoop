Requirements -
Hadoop version: 2.7.1
Spark version: 2.4.5
Apache Maven: 3.3.9
Java: 1.8 (Java 8)

Input directory: /user/root/cse532/input/covid19_full_data.csv
Cache directory: /user/root/cse532/cache/populations.csv
Output directory: /user/root/cse532/output/

Steps for running hadoop tasks:
$ cd hadoop
$ mkdir build
$ javac -cp `hadoop classpath` Covid19_1.java Covid19_2.java Covid19_3.java -d build
$ jar -cvf Covid19.jar -C build/ .

Output Directory will be /user/root/cse532/output/

Task 1: (true and false are case sensitive)
$ hadoop jar Covid19.jar Covid19_1 cse532/input/ <true|false> cse532/output/

Task 2: (Only YYYY-MM-DD date format is accepted)
$ hadoop jar Covid19.jar Covid19_2 cse532/input/ 2020-01-01 2020-03-31 cse532/output/

Task 3: (Make sure to add the file name  along with path for populations.csv)
$ hadoop jar Covid19.jar Covid19_3 cse532/input/ cse532/cache/populations.csv cse532/output/

Steps for running Spark tasks:
// Spark implementation is dependent upon Hadoop hdfs for checking if the file/directory/path exists or not. Configuration is added in pom.xml
// JAR file is built in target directory in same folder.
// Spark output will be in 2 separate files in output directory.

$ cd spark
$ mvn package

Task 1: (Only YYYY-MM-DD date format is accepted)
$ spark-submit --class Covid19_1 target/SparkCovid19.jar cse532/input/ 2020-01-01 2020-03-31 cse532/output/

Task 2:
$ spark-submit --class Covid19_2 target/SparkCovid19.jar cse532/input/ cse532/cache/ cse532/output/
