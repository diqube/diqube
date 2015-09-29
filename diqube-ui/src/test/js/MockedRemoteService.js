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

var MockedRemoteService = (function() {
  
  /**
   * A mocked RemoteService.
   * 
   * @param handlerFn: function(resultHandler, commandName, commandData). This function has to call resultHandler
   *        functions, which are defined just like the resultHandler methods in RemoteService.
   */
  function MockedRemoteService(handlerFn) {
    this.execute = function execute_(scope, commandName, commandData, resultHandler) {
      handlerFn(new (function() {
        this.data = function data_(dataType, data) {
          scope.$apply(function() {
            resultHandler.data(dataType, data);
          });
        }
        this.exception = function exception_(text) {
          if (resultHandler.hasOwnProperty('exception')) {
            scope.$apply(function() {
              resultHandler.exception(text);
            });
          }
        }
        this.done = function done_() {
          if (resultHandler.hasOwnProperty('done')) {
            scope.$apply(function() {
              resultHandler.done();
            });
          }
        }
      })(), commandName, commandData);
      resultHandler.data()
    }
  }
  
  return MockedRemoteService;
})();