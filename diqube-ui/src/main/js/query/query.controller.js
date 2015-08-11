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
(function() {
    "use strict";

    angular.module("diqube.query", [ "diqube.remote" ]).controller("QueryCtrl",
	    [ "remoteService", "$scope", function(remoteService, $scope) {
		var me = this;
		me.diql = "";
		me.result = null;
		me.exception = null;
		me.execute = execute;

		// ====

		function execute() {
		    me.data = "";
		    me.exception = null;
		    var ws = remoteService.getSocket();
		    ws.$$send({
			type : "query",
			data : {
			    diql : me.diql
			}
		    });
		    ws.$on("$message", function(data) {
			if (data.type == "result" && me.exception === null) {
			    $scope.$apply(function() {
				me.result = data.data
			    });
			} else if (data.type == "exception") {
			    $scope.$apply(function() {
				me.exception = data.data.text
				me.result = null;
			    });
			}

		    });
		    ws.$on("$close", function() {
		    });
		}
	    } ]);
})();