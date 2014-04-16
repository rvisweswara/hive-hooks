Contains hive custom hooks

build: mvn clean install

Following are the available hooks

1) QueueHandlerHiveDriverRunHook : 

This hook helps determing the allowed queue name for the user and override it automatically while executing hive queries.
This hook assumes queue/pool name property is mapred.job.queue.name for MR and tez.queue.name for tez. if the queue name is set to "default" or null, this hook tries to find the better non-default queue allowed for the user and sets it automatically. This hook will be invoked as Pre Driver Run hook and will effect queue determination while executing hive queries via HiveCLI, HiveServer2, Hue's Beeswax server 

Steps to configure:
-------------------

a) Add the following property to /etc/hive/conf/hive-site.xml

```
   <property>
    <name>hive.exec.driver.run.hooks</name>
    <value>org.apache.hadoop.mapred.QueueHandlerHiveDriverRunHook</value>
  </property>
```

b) Copy the compiled jar hive-custom-hooks-*jar to /usr/lib/hive/lib/ on Hive/HiveServer2/Hue hosts.

c) Restart hue/hiveserver2.


