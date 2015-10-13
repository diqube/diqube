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
"use strict";

/*
 * Installs simply polyfills for ES6 features to be used in PhantomJS.
 */

(function(global) {
  if (!String.prototype.includes) {
    String.prototype.includes = function(searchString, position) {
      if (position === undefined)
        position = 0;
      return this.indexOf(searchString, position) != -1;
    }
  }
  
  if (!String.prototype.startsWith) {
    String.prototype.startsWith = function(str, position) {
      position = position || 0;
      return this.indexOf(str, position) === position;
    };
  }
  
  if (!global.MutationObserver) {
    // noop MutationObserver
    global.MutationObserver = function() {
      this.observe = function() {}
      this.disconnect = function() {};
    }
  }
})(this);