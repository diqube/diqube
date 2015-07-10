#diqube - Distributed Query Base#

diqube is a fast, distributed, in-memory column-store which enables you to analyze large amounts of read-only data
easily. 

##Building from source##

Make sure you have the prerequisites installed:

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

Currently there is no ready-to-be-used binary built, but you can start the server using Maven on the command line.
Execute the following after building:

    $ cd diqube-server
    $ mvn exec:java -Dexec.mainClass=org.diqube.server.Server

The server will then start and watch the 'data' directory inside diqube-server for any new data to be loaded (.control
files). There is some sample data already provided.

The diqube server itself does not contain the diqube UI (will be implemented soon, currently you need to watch the log
output!). The UI is available as standard web archive (.war file) which can be deployed e.g. to a standard 
[Apache Tomcat][3]. You can find the .war in diqube-ui/target after building.

##License##

diqube is distributed under the terms of the [GNU Affero General Public License 3][4].

[1]: https://maven.apache.org
[2]: https://thrift.apache.org
[3]: https://tomcat.apache.org
[4]: http://www.gnu.org/licenses/agpl-3.0.html