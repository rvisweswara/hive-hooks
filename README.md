Contains hive custom hooks

build: mvn clean install

Following are the available hooks

1) QueueHandlerHiveDriverRunHook : 

This hook helps determing the allowed queue name for the user and override it automatically while executing hive queries. this hook assumes queue/pool name property is mapred.job.queue.name. if the queue name is set to "default", this class tries to find the better non default queue name allowed for the user and sets it automatically. This hook will be invoked as Pre Driver Run hook and will effect queue determination whive executing hive queries via HiveCLI, HiveServer2, Hue's Beeswax 

Steps to configure:
-------------------

a) Add the following property to /etc/hive/conf/hive-site.xml

```
   <property>
    <name>hive.exec.driver.run.hooks</name>
    <value>org.apache.hadoop.mapred.QueueHandlerHiveDriverRunHook</value>
  </property>
```

b) copy the compiled jar hive-custom-hooks-*jar to /usr/lib/hive/lib/ on Hive/HiveServer2/Hue hosts.


