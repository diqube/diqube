# diqube - Operating a cluster

diqube servers are meant to be executed in a cluster. This document gives some hints on running diqube in a cluster.

## The server executable

After building from source (see the README.md in the source root), there is an executable uber jar built in `diqube-server/target/diqube-server-1-SNAPSHOT.jar`. You can easily start this after building via

```
cd diqube-server
java -jar target/diqube-server-1-SNAPSHOT.jar
```

This will use the default server configuration and logging configuration. Some sample data will be loaded right away, too. 

## Customizing the server configuration

The server configuration is loaded from a simple properties file, whose location can be specified using the `diqube.properties` system property when starting the server. The set of properties that are available can be seen in the default configuration at [`server.properties`](/diqube-server/src/main/resources/server.properties) - if you specifiy your own configuration and you do not set a specific configuration value, the default will be used as fallback. A description of what these properties mean is available in the [`ConfigKey` class](/diqube-config/src/main/java/org/diqube/config/ConfigKey.java).

Please note that there are currently two **mandatory property** that need to be set:
  * [`messageIntegritySecret`](/diqube-config/src/main/java/org/diqube/config/ConfigKey.java#L200)
  * [`ticketRsaPrivateKeyPemFile`](/diqube-config/src/main/java/org/diqube/config/ConfigKey.java#L257)
    * If .pem is password protected: [`ticketRsaPrivateKeyPassword`](/diqube-config/src/main/java/org/diqube/config/ConfigKey.java#L262)
    * Note: create .pem e.g. with `openssl genrsa -des3 -out ticket.pem 2048`

You might also want to change:
  * [`superuser`](/diqube-config/src/main/java/org/diqube/config/ConfigKey.java#L312)
  * [`superuserPassword`](/diqube-config/src/main/java/org/diqube/config/ConfigKey.java#L317)

For example if you want to operate a cluster with two nodes, you could use the following configurations:

Node 1 (assume the IP that is accessible by other nodes is 192.168.0.1):
```
host=192.168.0.1
port=5101
clusterNodes=192.168.0.2:5102

messageIntegritySecret=SOME_RANDOM_VALUE   # A not-guessable, random value; same value for all servers
ticketRsaPrivateKeyPemFile=/same/pem/file/for/all/nodes.pem
ticketRsaPrivateKeyPassword=yourPassword
superuser=root
superuserPassword=yourSuperuserPassword    # Obviously: Same for all nodes, too.
```

Node 2 (assume the IP that is accessible by other nodes is 192.168.0.2):
```
host=192.168.0.2
port=5102
clusterNodes=192.168.0.1:5101

messageIntegritySecret=SOME_RANDOM_VALUE
ticketRsaPrivateKeyPemFile=/same/pem/file/for/all/nodes.pem
ticketRsaPrivateKeyPassword=yourPassword
superuser=root
superuserPassword=yourSuperuserPassword
```

When then starting the server using `java -Ddiqube.properties=/home/diqube/server.properties -jar target/diqube-server-1-SNAPSHOT.jar` on both machines, the one that is started last will greet the first one and they will introduce themselves to each other (see log).

When executing queries now, the sample data won't suffice, but we have to slightly adjust the `firstRowId`. See below.

## Loading data

Of course you want to be able to load some data into each diqube server. First of all, some general information about how to do this.

### Data distribution

When a query reaches one of the diqube servers in a cluster, it will in the end distribute the query to all cluster nodes that hold some part of the data of the table that is being queried. This means that first of all the data needs to be distributed in the cluster. 

Currently diqube uses a very simple approach to this: It expects the data of a table to be split beforehand and one or multiple *shard(s)* of the table to be loaded by a single diqube server instance. A *shard* (or better: *table shard*) contains the data of a set of consecutive rows of a table - it's basically one piece of a row-wise table starting from a specific row index and includes all data of all columns up to another specific row index. This row index is called **row id** in diqube. Each diqube server can serve multiple shards of the same table at once. When loading data from JSON or CSV files directly, the data of each file will be loaded into a single shard. For "diqube" files (see below), this is different.
After you have split the data of your table and created one or multiple file(s) for each diqube server that should load the data, what you need additionally is one/multiple `.control` file(s) (for each data file you need one control file). The sample one currently is (see [here](https://github.com/diqube/diqube/tree/master/diqube-server/data)):

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
4. `defaultColumnType` is the column type that should be used as fallback. For `json` there is an automatic data type identification being executed currently, so you might choose any of the core column types here basically with effectively no effect: `string`, `double` or `long`. This value is ignored for `diqube`. This might be re-worked in the future (see e.g. #15 and #16).
5. `firstRowId` is the rowId of the first row of data that is available in the file.

The last property is probably the most interesting for now: `firstRowId`. As stated before, the data of the whole table needs to be split before the data can be loaded by diqube. Nevertheless, *row ids* need to be maintained: Each data file contains the data of a consecutive set of rows, starting from `firstRowId` (potentially split into multiple table shards). Therefore, the value needs to be different for each diqube server that loads one or multiple part(s) of the table.

When looking at the row ids of the whole table (=across all cluster nodes), the first row id of the table needs to be **0** and then for each row id there needs to be data available somewhere in the cluster, up to the biggest row id (= the last row id of the table). If you operate a cluster with just one server (e.g. for testing purposes), the setting is trivial therefore: It's always "0" unless you load multiple files of a single table on that single diqube server.

### JSON format

When loading data from a JSON file, that file needs to have a specific layout: It needs to define an *array* object as top level object. This array then contains a list of objects, where each of those objects defines the data for one **row** of the table (shard). The row-objects in turn can be complex objects (=contain other complex objects) and contain arrays.

Each JSON file that is loaded directly by a diqube server will result in one *table shard* being built, which will serve the rowId range starting from *firstRowId* (see control file).

### diqube format

When importing either JSON or CSV, the import data is always stored row-wise in that file. As diqube is a column-store, it will internally store the values of one column of all rows together, which means it needs to transpose the JSON/CSV input data (= read all rows, then cut the data into single columns and then start to compress single columns etc.).
This transposing and compressing (1) takes some amount of time and (2) might take up a pretty notable amount of main memory. Usually when operating a cluster, you do not want the server process itself take up that much amount of memory for importing new data while it is serving requests at the same time - you do not want to risk that the process is killed because an out-of-memory error and you do not want to slow down any query executions.

Therefore you can transpose and compress the data in a separate step using the [diqube-tool](https://github.com/diqube/diqube/tree/master/diqube-tool) (it's a uber jar, too). That executable has a command `transpose` which can read the usual input files for diqube, execute the transposing and the compression and then serialize that data into a file. That data can then easily and quickly be loaded by diqube-server, because it already is available in a columnar format and already compressed using diqubes own mechanisms.

Files of the .diqube data format can also be created as output of a Apache Hadoop Map/Reduce job, using [DiqubeOutputFormat](https://github.com/diqube/diqube/blob/master/diqube-hadoop/src/main/java/org/diqube/hadoop/DiqubeOutputFormat.java) which is available in diqube-hadoop/. Have a look at the [diqube Data Examples](https://github.com/diqube/diqube-data-examples) repository to see how to use it. 

In contrast to JSON or CSV files, diqube files can contain **multiple table shards** at once. That means that internally the diqube server will maintain multiple shards (which might influence query execution speed/responsiveness). Anyway, when specifying the *firstRowIds* in the corresponding control files, you can simply ignore that fact.

#### Example

Assume you have created two diqube files using diqube-hadoop. diqube-hadoop will automatically create diqube files which have multiple table shards in it: When the main memory taken by the reducers exceeds a specific amount, diqube-hadoop will start creating a table shard, flush that to the output file and therefore free up some main memory before accepting any more input data.

If you now want to load these two diqube files on a single or multiple diqube servers, you need to create the control files first. 

The first control file is simple, as it contains firstRowId = 0:

```
table=pums
file=pums1.diqube
type=diqube
firstRowId=0
```

The data of the table shards that are served from that data then span the row IDs 0..[number of rows in pums1 - 1]. Therefore the second diqube file needs a control file with a firstRowId setting of [number fo rows in pums1], as this is the next free row ID. In order to get the number of rows stored in a diqube file, you can use [diqube-tool](https://github.com/diqube/diqube/tree/master/diqube-tool):

```
$ java -jar diqube-tool.jar info -i pums1.diqube
Number of table shards: 7
Total number of rows: 500000
Comment: abc
```

Using this information, the second control file will look like this:

```
table=pums
file=pums2.diqube
type=diqube
firstRowId=500000
```

### Finally loading the data

After both the data file and the control file are prepared and available, the loading of the shard can be triggered by copying the control file into the directory that is specified by the property `dataDir` in the server configuration (default is simply a relative `data` directory). diqube server monitors this directory for new control files being created and removed and triggers un-/loading accordingly. Please note that control files need to have the file extension `.control`. 
After doing this, there should be some information in the server log that new data has been loaded. 

### Sample data

To load the sample data in a cluster environment, you need to specify a different `firstRowId` on each diqube server. The sample data currently contains 100 rows - that means that one server should specify `firstRowId=0`, the next should specify `firstRowId=100` etc. in their respective .control files: Row id 0 and 100 will then contain the same data etc. (because both .control files point to the same JSON file).

## Advanced configuration properties

### Flatten
diql supports selections on flattened tables, as can be read in the Query Guide. The flattened versions of tables will be created on demand and will be cached in both main memory and on disk. Using the server.properties key `flattenMemoryCacheSizeMb` you can specify how much main memory should be used by that cache maximum and using the key `flattenDiskCacheLocation` you can specify where to put the cached data on disk. 

As the flatten process might take a while, one can ask diqube to flatten the data of a new table directly after loading that data. Use the property `autoFlatten` in the tables control files for this. The value is a comma-separated list of fields to automatically flatten by. Using this will make executing queries on the flattened table faster for the first time.

## Overriding loglevels

This can be done by providing a different logging configuration. diqube uses [logback](http://logback.qos.ch/manual/configuration.html), therefore the system property `logback.configurationFile` can be used to point to an alternative logging configuration.

## The User Interface

TODO

### Deployment

diqube-ui can be configured using [context init parameters][1]. The configuration keys can currently be found in 
[DiqubeServletConfig](https://github.com/diqube/diqube/tree/master/diqube-ui/src/main/java/org/diqube/ui/DiqubeServletConfig.java). 
There is currently one **mandatory** key: `diqube.ticketPublicKeyPem`.

`diqube.ticketPublicKeyPem` points to a file containing a public RSA key in OpenSSL PEM format. Note that this needs to be the public key of the private/public key pair that is configured for diqube-servers under `ticketRsaPrivateKeyPemFile`. Note that the UI though must only get the public key configured, not the private one. This is because the UI itself will never sign its own Tickets, but will only just validate ones signed by diqube-server. Therefore it is a configuration error to provide a pem file to the UI that contains a private key. Extract the public key from a file that contains a private/public key pair with `openssl rsa -in private.pem -pubout -out public.pem`.

To be able to provide these configuration properties, deployment to Tomcat should be done using a context xml file. Use and adjust the following example file and copy it into Tomcats `conf/Catalina/localhost` directory: [tomcat-sample-context.xml](tomcat-sample-context.xml). Be sure to name the file just like the context-path you want the app to be available at.

[1]: http://tomcat.apache.org/tomcat-8.0-doc/config/context.html#Context_Parameters