#diqube - Operating a cluster#

diqube servers are meant to be executed in a cluster. This document gives some hints on running diqube in a cluster.

##The server executable##

After building from source (see the README.md in the source root), there is an executable uber jar built in `diqube-server/target/diqube-server-1-SNAPSHOT.jar`. You can easily start this after building via

```
cd diqube-server
java -jar target/diqube-server-1-SNAPSHOT.jar
```

This will use the default server configuration and logging configuration. Some sample data will be loaded right away, too. 

##Customizing the server configuration##

The server configuration is loaded from a simple properties file, whose location can be specified using the `diqube.properties` system property when starting the server. The set of properties that are available can be seen in the default configuration at [`server.properties`](/diqube-server/src/main/resources/server.properties) - if you specifiy your own configuration and you do not set a specific configuration value, the default will be used as fallback. A description of what these properties mean is available in the [`ConfigKey` class](/diqube-config/src/main/java/org/diqube/config/ConfigKey.java).

For example if you want to operate a cluster with two nodes, you could use the following configurations:

Node 1 (assume the IP that is accessible by other nodes is 192.168.0.1):
```
host=192.168.0.1
port=5101
clusterNodes=192.168.0.2:5102
```

Node 2 (assume the IP that is accessible by other nodes is 192.168.0.2):
```
host=192.168.0.2
port=5102
clusterNodes=192.168.0.1:5101
```

When then starting the server using `java -Ddiqube.properties=/home/diqube/server.properties -jar target/diqube-server-1-SNAPSHOT.jar` on both machines, the one that is started last will greet the first one and they will introduce themselves to each other (see log).

When executing queries now, the sample data won't suffice, but we have to slightly adjust the `firstRowId`. See below.

##Loading data##

Of course you want to be able to load some data into each diqube server. First of all, some general information about how to do this.

###Data distribution###

When a query reaches one of the diqube servers in a cluster, it will in the end distribute the query to all cluster nodes that hold some part of the data of the table that is being queried. This means that first of all the data needs to be distributed in the cluster. 

Currently diqube uses a very simple approach to this: It expects the data of a table to be split beforehand and only a *shard* of the table to be presented to a single diqube server instance for loading. A *shard* (or better: *table shard*) contains the data of a set of consecutive rows of a table - it's basically one piece of a row-wise table starting from a specific row index and includes all data of all rows up to another specific row index. This row index is called **row id** in diqube. Each diqube server can serve only *one* *table shard* of the same table currently (see #33), but serve table shards of multiple tables.
After you have split the data of your table and created one file for each diqube server that should load the data, what you need additionally, is a `.control` file. The sample one currently is:

```
table=age
file=age.json
type=json
defaultColumnType=long
firstRowId=0
```

1. `table` denotes the name of the table the data file is part of.
2. `file` points to the file on the local HDD from which the data should be loaded.
3. `type` is the data type the data is available in. Currently there is `csv`, `json` and `diqube`, you should prefer `diqube`, but `json` is also somewhat fine (see below for a description of `diqube`).
4. `defaultColumnType` is the column type that should be used as fallback. For `json` there is an automatic data type identification being executed currently, so you might choose any of the core column types here basically with effectively no effect: `string`, `double` or `long`. This value is ignroed for `diqube`. This will be re-worked in the future (see e.g. #15 and #16).
5. `firstRowId` is the rowId of the first row of data that is available in the file.

The last property is probably the most interesting for now: `firstRowId`. As stated before, the data of the whole table needs to be split before the data can be loaded by diqube. Nevertheless, *row ids* need to be maintained: Each *table shard* contains the data of a consecutive set of rows, starting from `firstRowId`. Therefore, the value needs to be different for each diqube server that loads a part of the table. 

When looking at the row ids of the whole table (=across all cluster nodes), the first row id of the table needs to be **0** and then for each row id there needs to be data available somewhere in the cluster, up to the biggest row id (= the last row id of the table).

###JSON format###

When loading data from a JSON file, the file needs to have a specific layout: It needs to define an *array* object as top level object. This array then contains a list of objects, where each of those objects defines the data for one **row** of the table (shard). The row-objects in turn can be complex objects (=contain other complex objects) and contain arrays. Please note that each of the row-objects though has to define the same set of properties currently (see #14).

###diqube format###

When importing either JSON or CSV, the import data is always stored row-wise. As diqube is a column-store, it will store the values of one column of all rows together, which means it needs to transpose the input data (= read all rows, then cut the data into single columns and then start to compress single columns etc.).
This transposing and compressing (1) takes some amount of time and (2) might take up a pretty notable amount of main memory. Usually when operating a cluster, you do not want the server process itself take up that much amount of memory for importing new data while it is serving requests at the same time - you do not want to risk that the process is killed because an out-of-memory error and you do not want to have queries being executed to slow down. Therefore you can transpose and compress the data in a separate step using the `diqube-tranpose` executable (it's a uber jar, too). That executable can read the usual input files for diqube, execute the transposing and the compression and then serialize that data into a file. That data can then easily be loaded by diqube-server, because it already is available in a columnar format and already compressed using diqube-mechanisms.

Files of the .diqube data format can also be created as output of a Apache Hadoop Map/Reduce job, using `DiqubeOutputFormat` which is available in diqube-hadoop/. Have a look at the [diqube Data Examples](https://github.com/diqube/diqube-data-examples) repository to see how to use it. 

###Finally loading the data###

After both the data file and the control file are prepared and available, the loading of the shard can be triggered by copying the control file into the directory that is specified by the property `dataDir` in the server configuration (default is simply a relative `data` directory). diqube server monitors this directory for new control files being created and removed and triggers un-/loading accordingly. Please note that control files need to have the file extension `.control`. 
After doing this, there should be some information in the server log that new data has been loaded. 

###Sample data###

To load the sample data in a cluster environment, you need to specify a different `firstRowId` on each diqube server. The sample data currently contains 100 rows - that means that one server should specify `firstRowId=0`, the next should specify `firstRowId=100` etc. in their respective .control files: Row id 0 and 100 will then contain the same data etc. (because both .control files point to the same JSON file).

##Overriding loglevels##

This can be done by providing a different logging configuration. diqube uses [logback](http://logback.qos.ch/manual/configuration.html), therefore the system property `logback.configurationFile` can be used to point to an alternative logging configuration.

##The User Interface##

TODO