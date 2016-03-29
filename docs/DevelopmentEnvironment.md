#Environment used to develop diqube#

* Eclipse Mars.1
  * [Palantirs TypeScript plugin](https://github.com/palantir/eclipse-typescript) for TypeScript 1.7.0
  * [AutoDeriv plugin](https://github.com/nodj/AutoDeriv)

After importing the projects, adjust the following:
* diqube-ui project:
  * Configure -> Enable TypeScript builder
  * Properties -> TypeScript: Source folder "src/main/web", Output folder: "target/eclipse-typescript" (Eclipse TypeScript build is completely separate to the maven one, the results of the Eclipse TypeScript build are not used directly, but only by eclipse to have code assists etc).
  * Properties -> Maven: Set profiles to "!minjs,eclipse,disableNode"