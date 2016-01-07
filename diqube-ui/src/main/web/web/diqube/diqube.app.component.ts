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

import {Component} from "angular2/core";
import {RouteConfig, ROUTER_DIRECTIVES} from "angular2/router";

import {OpenAnalysisComponent} from "./open-analysis/open-analysis.component";
import {AboutComponent} from "./about/about.component";
import {LoginComponent} from "./login/login.component";
import {LogoutComponent} from "./logout/logout.component";

import {LoginStateService} from "./login-state/login-state.service";

@Component({
    selector: "diqube",
    templateUrl: "diqube/diqube.app.html",
    directives: [OpenAnalysisComponent, ROUTER_DIRECTIVES]
})
@RouteConfig([
  { path: "/about", name: "About", component: AboutComponent },
  { path: "/login", name: "Login", component: LoginComponent, useAsDefault: true },
  { path: "/logout", name: "Logout", component: LogoutComponent },
])
export class DiqubeAppComponent { 

  constructor(private loginStateService: LoginStateService) {}

  public isLoggedIn(): boolean {
    return this.loginStateService.isTicketAvailable();
  }
}