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

var MockedScope = (function() {
  function MockedScope() {
    var me = this;
    
    // apply function simply calls the inner function, we do not need to digest anything etc.
    me.$apply = function(fn) {
      fn();
    }
    
    me.events = [];
    
    me.$on = function(eventName, resultFn) {
      me.events.push(eventName);
      me.events.push(resultFn);
    }
    
    /** This will fire an event. It will not actually broadcast to different scopes, though. */
    me.$broadcast = function(eventName, eventData) {
      for (var i = 0; i < me.events.length; i = i + 2) {
        if (me.events[i] === eventName) {
          me.events[i+1]({ /* this would be an Event object in real-life. Ignore in tests*/ }, eventData);
          return true;
        }
      }
      
      return false;
    }
  }
  
  return MockedScope;
})();