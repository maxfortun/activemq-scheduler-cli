### Purpose
Copy [ActiveMQ Scheduler](https://activemq.apache.org/delay-and-schedule-message-delivery) messages to disk or to another broker. 

### Building
```bash
mvn clean package
```

### Running
```bash
java -jar target/activemq-scheduler-cp-*-jar-with-dependencies.jar
```

### Usage
```
usage: $JAVA_HOME/bin/java
       -jar
       target/activemq-scheduler-cp-*-jar-with-dependencies.jar
       [-sb <arg>] [-sp <arg>] [-st <arg>] [-su <arg>] [-tb <arg>]
       [-td <arg>] [-tid <arg>] [-tp <arg>] [-tu <arg>]

 -sb,--source-broker <arg>    Source broker url.
 -sp,--source-pass <arg>      Source broker password.
 -st,--source-timeout <arg>   Source timeout. Default: 6000
 -su,--source-user <arg>      Source broker username.

 -tb,--target-broker <arg>    Target broker url.
 -td,--target-dir <arg>       Target directory.
 -tid,--target-id <arg>       Target id property field. Default: scheduledJobId
 -tp,--target-pass <arg>      Target broker password.
 -tu,--target-user <arg>      Target broker username.

```
