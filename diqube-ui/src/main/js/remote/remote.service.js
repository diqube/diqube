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
    angular.module("diqube.remote", [ "ngWebsocket" ]).service("remoteService", [ "$websocket", function remoteServiceProvider($websocket) {
	var me = this;

	me.getSocket = getSocket;
	me.getService = getService;
	me.initialize = initialize;

	// ==

	me.$baseUrlWithoutProtocol;
	me.$socketProtocol;

	function initialize($location) {
	    me.$baseUrlWithoutProtocol = "://" + $location.host() + ":" + $location.port() + globalContextPath;
	    if ($location.protocol().toLowerCase() === "https")
		me.$socketProtocol = "wss";
	    else
		me.$socketProtocol = "ws";
	}

	function getSocket() {
	    var url = me.$socketProtocol + me.$baseUrlWithoutProtocol + "/socket";
	    return $websocket.$new({
		url : url,
		lazy : false,
		reconnect : true,
		reconnectInterval : 2000,
		mock : false,
		enqueue : true
	    });
	}

	function getService() {
	    // $location.protocol()
	}
    } ]).run([ "remoteService", "$location", function diqubeRemoteRun(remoteService, $location) {
	remoteService.initialize($location);
    } ]);
})();