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
'use strict';

(function() {
  angular.module("diqube.remote", [ "ngWebsocket" ]).service("remoteService",
      [ "$websocket", "$log", function remoteServiceProvider($websocket, $log) {
        var me = this;
        
        me.execute = execute;
        me.cancel = cancel;

        // ==

        me.getSocket = getSocket;
        me.getService = getService;
        me.initialize = initialize;
        
        me.socketMessage = socketMessage;
        me.socketClose = socketClose;
        me.cleanupRequest = cleanupRequest;

        me.$baseUrlWithoutProtocol;
        me.$socketProtocol;
        me.requestRegistry = {};
        me.socket = null;
        me.nextRequestIdNumber = Number.MIN_SAFE_INTEGER;

        /**
         * Call this method to send a command to the server.
         * 
         * @param scope The $scope of the caller.
         * @param commandName The name of the command to execute.
         * @param commandData Additional data that the command might need to execute.
         * @param resultHandler All methods of this object will be called inside the given scope. 
         *  Methods:
         *      data(dataType, data): Some data was received. Return true in case the command should be cleaned up (= the command is done).
         *      exception(text): There was an exception reported by the server. Command is cleaned up automatically.
         *      done(): The server reported to be done with working on this command.
         * @returns requestId that can be used for a call to cancel(requestId).
         */
        function execute(scope, commandName, commandData, resultHandler) {
          var requestId = "id" + me.nextRequestIdNumber;
          me.nextRequestIdNumber += 1;
          
          var sock = me.getSocket();
          me.requestRegistry[requestId] = {
              resultHandler : resultHandler,
              scope : scope
          };
          
          sock.$$send({
            requestId: requestId, 
            command: commandName,
            commandData: commandData
          });
          
          return requestId;
        }
        
        /**
         * Cancel a specific request.
         * 
         * @param requestId The result of the execute call.
         */
        function cancel(requestId) {
          $log.info("Cancelling request ", requestId);
          me.getSocket().$$send({
            requestId: requestId,
            command: "cancel"
          });
          me.cleanupRequest(requestId);
        }
        
        /**
         * Called when a message is received on the Websocket.
         * @param resultEnvelope The data from the server.
         * @returns -
         */
        function socketMessage(resultEnvelope) {
          if (typeof resultEnvelope === 'string')
            // looks like sometimes we get a pure string forwarded here - ignore that.
            return;
          var requestId = resultEnvelope.requestId;
          var status = resultEnvelope.status;
          var data = resultEnvelope.data;
          var dataType = resultEnvelope.dataType;
          
          $log.debug("Received message on websocket: ", resultEnvelope);
          
          var res = me.requestRegistry[requestId];
          if (status != "done" && res == null) {
            $log.warn("Received data from websocket, but requestId unknown: ", resultEnvelope);
            return;
          } 
          
          if (status === "data") {
            res.scope.$apply(function() {
              var messageRes = res.resultHandler.data(dataType, data);
              if (messageRes === true)
                me.cleanupRequest(requestId);
            });
          } else if (status === "done") {
            if (res.resultHandler.hasOwnProperty('done')) {
              res.scope.$apply(function() {
                res.resultHandler.done();
              });
            }
            me.cleanupCommand(requestId);
          } else if (status === "exception") {
            $log.warn("Exception on request ", requestId, ": ", data);
            if (res.resultHandler.hasOwnProperty('exception')) {
              res.scope.$apply(function() {
                res.resultHandler.exception(data.text);
              });
            }
            me.cleanupRequest(requestId);
          }
        }
        
        function socketClose() {
        }
        
        /**
         * Cleans up resources reserved for a specific requestId. 
         */
        function cleanupRequest(requestId) {
          $log.debug("Cleaning up request ", requestId);
          delete me.requestRegistry[requestId]
        }

        function getSocket() {
          if (me.socket != null)
            return me.socket;
          
          var url = me.$socketProtocol + me.$baseUrlWithoutProtocol + "/socket";
          me.socket = $websocket.$new({
            url : url,
            lazy : false,
            reconnect : true,
            reconnectInterval : 2000,
            mock : false,
            enqueue : true
          });
          me.socket.$on("$message", me.socketMessage);
          me.socket.$on("$close", me.socketClose);
          
          return me.socket;
        }

        function getService() {
          // $location.protocol()
        }
        
        function initialize($location) {
          me.$baseUrlWithoutProtocol = "://" + $location.host() + ":" + $location.port() + globalContextPath;
          if ($location.protocol().toLowerCase() === "https")
            me.$socketProtocol = "wss";
          else
            me.$socketProtocol = "ws";
        }
      } ]).run([ "remoteService", "$location", function diqubeRemoteRun(remoteService, $location) {
    remoteService.initialize($location);
  } ]);
})();