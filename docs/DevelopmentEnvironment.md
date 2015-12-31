#Environment used to develop diqube#

* Eclipse Mars.1
  * [Palantirs TypeScript plugin](https://github.com/palantir/eclipse-typescript)
    * Set to the same settings as in tsconfig.json
    * Because the plugin cannot read node_modules as input (e.g. angular2), the pom.xml copies the needed files accordingly.
  * [AutoDeriv plugin](https://github.com/nodj/AutoDeriv)
