#diqube - Distributed Query Base#

diqube is a fast, distributed, in-memory column-store which enables you to analyze large amounts of read-only data
easily. 

##Building from source##

[![Build Status](http://build.diqube.org/buildStatus/icon?job=diqube)](http://build.diqube.org)

Make sure you have the prerequisites installed:

 * Java 8
 * [Maven 3][1]
 * [Apache Thrift][2] compiler 0.9.2

In addition to that, you need to provide the location of the Thrift compiler executable to the maven build. Do this by
setting the property "thrift.executable" in your settings.xml:

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"  
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
              xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
      <profiles>
        <profile>
          <id>local-thrift-092</id>
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

    $ mvn clean install

##Starting diqube##

Execute the following after building:

    $ cd diqube-server
    $ java -jar target/diqube-server-1-SNAPSHOT.jar

The server will then start and watch the 'data' directory inside diqube-server for any new data to be loaded (.control
files). There is some sample data already provided. For more information see [Operating a cluster](/docs/OperatingCluster.md).

The diqube server itself does not contain the diqube UI. The UI is available as standard web archive (.war file) which 
can be deployed to a [Apache Tomcat 8.0.24][3]. You can find the .war in diqube-ui/target after building. Please note 
that currently only Apache Tomcat 8.0.24 is supported, as there is a bug in Tomcat and diqube has implemented a 
work-around. Configuration of the UI can be done via [context init parameters][5], see 
[DiqubeServletConfig](blob/master/diqube-ui/src/main/java/org/diqube/ui/DiqubeServletConfig.java) for configuration 
options.

##Documentation##

See the  [`docs`](/docs) directory for some documentation.

##License##

diqube is distributed under the terms of the [GNU Affero General Public License 3][4].

[1]: https://maven.apache.org
[2]: https://thrift.apache.org
[3]: https://tomcat.apache.org
[4]: http://www.gnu.org/licenses/agpl-3.0.html
[5]: http://tomcat.apache.org/tomcat-8.0-doc/config/context.html#Context_Parameters