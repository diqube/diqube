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