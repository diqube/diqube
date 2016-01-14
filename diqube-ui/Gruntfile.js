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
module.exports = function(grunt) {

  grunt.initConfig({
    pkg: grunt.file.readJSON("package.json"),
    run: {
      compile_typescript: {
        cmd: "node_modules/typescript/bin/tsc",
        args: [ ]
      }
    },
    // usually we should uglify the result as the typescript output contains inline sourcemaps. 
    uglify: {
      options: {
        banner: "/*\n" +
                " * diqube: Distributed Query Base.\n *\n" +
            
                " * Copyright (C) 2015 Bastian Gloeckle\n *\n" +
            
                " * This file is part of diqube.\n *\n" +
            
                " * diqube is free software: you can redistribute it and/or modify\n" +
                " * it under the terms of the GNU Affero General Public License as\n" +
                " * published by the Free Software Foundation, either version 3 of the\n" +
                " * License, or (at your option) any later version.\n *\n" +
            
                " * This program is distributed in the hope that it will be useful,\n" +
                " * but WITHOUT ANY WARRANTY; without even the implied warranty of\n" +
                " * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n" +
                " * GNU Affero General Public License for more details.\n *\n" +
            
                " * You should have received a copy of the GNU Affero General Public License\n" +
                " * along with this program.  If not, see <http://www.gnu.org/licenses/>.\n" +
                " */\n"
      },
      typescript: {
        files: [{
          expand: true,
          cwd: "target/typescript-dev/main",
          src: "**/*.js",
          dest: "target/typescript-ugly/main"
        }],
      }
    },
    watch: {
      typescript: {
        files: "**/*.ts",
        tasks: [ "run:compile_typescript", "uglify:typescript" ],
      }
    }
  });

  grunt.loadNpmTasks("grunt-run");
  grunt.loadNpmTasks("grunt-contrib-watch");
  grunt.loadNpmTasks("grunt-contrib-uglify");

  // Default task(s).
  grunt.registerTask("default", [ "run:compile_typescript", "uglify:typescript" ]);

};