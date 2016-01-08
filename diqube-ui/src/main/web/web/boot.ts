///
/// diqube: Distributed Query Base.
///
/// Copyright (C) 2015 Bastian Gloeckle
///
/// This file is part of diqube.
///
/// diqube is free software: you can redistribute it and/or modify
/// it under the terms of the GNU Affero General Public License as
/// published by the Free Software Foundation, either version 3 of the
/// License, or (at your option) any later version.
///
/// This program is distributed in the hope that it will be useful,
/// but WITHOUT ANY WARRANTY; without even the implied warranty of
/// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
/// GNU Affero General Public License for more details.
///
/// You should have received a copy of the GNU Affero General Public License
/// along with this program.  If not, see <http://www.gnu.org/licenses/>.
///

import {bootstrap} from "angular2/platform/browser"
import {ROUTER_PROVIDERS} from "angular2/router";

import {DiqubeAppComponent} from "./diqube/diqube.app.component"
import {RemoteService} from "./diqube/remote/remote.service"
import {LoginStateService} from "./diqube/login-state/login-state.service"
import {CookiesService} from "./diqube/cookies/cookies.service";
import {AnalysisService} from "./diqube/analysis/analysis.service";
import {AnalysisExecutionService} from "./diqube/analysis/execution/analysis.execution.service";

bootstrap(DiqubeAppComponent, [ ROUTER_PROVIDERS, RemoteService, LoginStateService, CookiesService, AnalysisService, 
                                AnalysisExecutionService ]);  