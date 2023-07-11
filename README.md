### Premise
Be that troubleshooting or migration, sometimes it may be neccessary to look behind the curtains of the [ActiveMQ Scheduler](https://activemq.apache.org/delay-and-schedule-message-delivery). This little utility allows to copy scheduled messages to a file system or to another broker. And, if you must, even cleans up after itself.  

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
usage: /usr/lib/jvm/java-11-openjdk-11.0.14.1.1-1.el7_9.x86_64/bin/java
       -jar
       target/activemq-scheduler-cp-1.0.0-SNAPSHOT-jar-with-dependencies.jar
       [-dr] [-sb <arg>] [-sp <arg>] [-srm <arg>] [-st <arg>] [-su <arg>]
       [-tb <arg>] [-td <arg>] [-tid <arg>] [-tp <arg>] [-tu <arg>]

 -dr,--dry-run                Do not alter broker states by either
                              forwarding to target or removing from
                              source.
 -sb,--source-broker <arg>    Source broker url.
 -sp,--source-pass <arg>      Source broker password.
 -srm,--source-rm <arg>       Remove from source. Values: never, always,
                              success, error. Default: never
 -st,--source-timeout <arg>   Source timeout. Default: 6000
 -su,--source-user <arg>      Source broker username.
 -tb,--target-broker <arg>    Target broker url.
 -td,--target-dir <arg>       Target directory.
 -tid,--target-id <arg>       Target id property field. Default:
                              scheduledJobId
 -tp,--target-pass <arg>      Target broker password.
 -tu,--target-user <arg>      Target broker username.

```

### SSL
For [SSL](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html#CustomizingStores) 
```
-Djavax.net.ssl.keyStore=/path/to/keyStore.ks -Djavax.net.ssl.keyStorePassword=changeit -Djavax.net.ssl.trustStore=/path/to/trustStore.ts -Djavax.net.ssl.trustStorePassword=changeit )
```

### Debugging
For TLDR;
```
-Dorg.slf4j.simpleLogger.defaultLogLevel=debug
```
