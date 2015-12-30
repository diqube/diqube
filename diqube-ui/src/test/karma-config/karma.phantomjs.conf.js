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
var baseConfig = require('./karma.default.conf.js');

module.exports = function(config) {
	// Load base config
	baseConfig(config);
	
	// PhantomJs does not have support for Promises, load a ployfill.
  var files = config.files;
  var idx = files.indexOf("BROWSER_BASED_FILES");
  files[idx] = "node_nodules/es6-promise/dist/es6-promise.js";
	
	// Override base config
	config.set({
	  files: files,
		browsers : [ 'PhantomJS' ]
	});

};
