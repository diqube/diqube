/*
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
module.exports = function(config) {
  // will be replaced with the path to the directory that contains the files of the final .war file.
  var warBasePath = {{warBasePath}};

  // this directory contains all the *js file from both, dependencies and our logic that is contained in the resulting
  // .war.
  // do not change this variable name, otherwise the replacement below won't work anymore!
  var webappBasePath = warBasePath + "/WEB-INF/classes/web/";

  config.set({

    // will be replaced with the directory that contains the pom.xml
    basePath : {{karmaBasePath}},

    files : [ 
             // the following will be replaced by a list of dependencies that are referenced from index.html (see pom.xml)
             {{originalJsFiles}}
             // in addition to the above, load angular mocks and our test files.
              "bower_components/angular-mocks/angular-mocks.js", 
              "BROWSER_BASED_FILES", 
              "src/test/js/*.js",
              "src/test/js/*/**/*.js" ],

    autoWatch : false,
    singleRun : true,

    frameworks : [ "jasmine" ],

    plugins : [ "karma-chrome-launcher", "karma-phantomjs-launcher", "karma-jasmine", "karma-junit-reporter" ],

    reporters : [ "dots", "junit" ],

    junitReporter : {
      outputDir : "target/karma-results",
      // outputFile: browser name,
      suite : "karma"
    }

  });
};
