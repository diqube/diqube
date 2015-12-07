#diqube - Distributed Query Base#

[![Build Status](http://build.diqube.org/buildStatus/icon?job=diqube)](http://build.diqube.org)
[![Project Statistics](https://www.openhub.net/p/diqube/widgets/project_thin_badge?format=gif)](https://www.openhub.net/p/diqube)

diqube is a fast, distributed, in-memory column-store which enables you to analyze large amounts of read-only data
easily. 

##Getting started##

To get started with diqube, you either need to 
* prepare a [Docker](https://www.docker.com) image which will build & start a specific version of diqube for you or
* install a build environment on your local machine and build and start diqube from there.

In order to prepare some data to be loaded into diqube server, you might want to read the chapter "Loading data" of the 
[Operating a cluster guide](docs/OperatingCluster.md).

###Using Docker to get started###

To create a Docker image, follow the instructions at
[diqube-docker/build-and-run](https://github.com/diqube/diqube-docker/tree/master/build-and-run).

###Building manually from source###

Make sure you have the prerequisites installed:

 * Java 8
 * [Maven 3][1] (>= 3.1)
 * [Apache Thrift][2] compiler 0.9.3

In addition to that, you need to provide the location of the Thrift compiler executable to the maven build. Do this by
setting the property "thrift.executable" in your settings.xml:

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"  
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
              xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
      <profiles>
        <profile>
          <id>local-thrift-093</id>
          <activation>
            <activeByDefault>true</activeByDefault>
          </activation>
          <properties>
            <thrift.executable>/path/to/your/thrift/compiler/thrift</thrift.executable>
          </properties>
        </profile>
      </profiles>
    </settings>

After everything is prepared, execute the build using

    $ mvn clean install -DskipTests

####Starting diqube####

Execute the following after building, note that the passwords and secrets **need to be changed for production use**, obviously:

    $ cd diqube-server
    
    $ echo "messageIntegritySecret=CHANGE_THIS_IN_PRODUCTION" > server.properties
    $ openssl genrsa -des3 -out ticket.pem -passout pass:diqube 2048
    $ echo "ticketRsaPrivateKeyPemFile=`pwd`/ticket.pem" >> server.properties
    $ echo "ticketRsaPrivateKeyPassword=diqube" >> server.properties
    $ echo "superuser=root" >> server.properties
    $ echo "superuserPassword=diqube" >> server.properties
    
    $ java -Ddiqube.properties=server.properties -jar target/diqube-server-1-SNAPSHOT.jar

The server will then start and watch the 'data' directory inside diqube-server for any new data to be loaded (.control
files). There is some sample data already provided. For more information see [Operating a cluster](/docs/OperatingCluster.md).

The diqube server itself does not contain the diqube UI. The UI is available as standard web archive (.war file) which 
can be deployed using a context xml like [tomcat-sample-context](docs/tomcat-sample-context.xml) (for additional 
configuration keys see [DiqubeServletConfig](diqube-ui/src/main/java/org/diqube/ui/DiqubeServletConfig.java)) to an 
[Apache Tomcat 8.0.26+][3]. You can find the .war in diqube-ui/target after building. Please note 
that only Apache Tomcat 8.0.26+ is supported, as it relies on [Tomcat Bug 58232][6] being fixed which is the case 
starting with 8.0.26. The UI only supports current versions of Chrome.

##Documentation##

See the  [`docs`](/docs) directory for some documentation.

##License##

diqube is distributed under the terms of the [GNU Affero General Public License 3][4].

[1]: https://maven.apache.org
[2]: https://thrift.apache.org
[3]: https://tomcat.apache.org
[4]: http://www.gnu.org/licenses/agpl-3.0.html
[5]: http://tomcat.apache.org/tomcat-8.0-doc/config/context.html#Context_Parameters
[6]: https://bz.apache.org/bugzilla/show_bug.cgi?id=58232
