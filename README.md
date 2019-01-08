# Hive_MDE

Default output is to stdout.
```
mvn package -DskipTests=true
cd target
java -jar hive-1.0-SNAPSHOT.jar [-mde|-qli] -d <ConfigDir> [-u <Username>] [-p <Password>] [-o <OutputFile>]
```
