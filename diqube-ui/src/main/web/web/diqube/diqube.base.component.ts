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

import {OnInit} from "angular2/core";
import {OnActivate, OnDeactivate, ComponentInstruction} from "angular2/router";
import {LoginStateService} from "./login-state/login-state.service";
import {NavigationStateService} from "./navigation-state/navigation-state.service";

/**
 * Base class for components that are reachable by a router-outlet = which have a URL they are reachable at.
 * 
 * This is not meaningful for components which are only used in other components but are not reachable directly (e.g.
 * the "qube" component inside an "analysis").
 */
export class DiqubeBaseNavigatableComponent implements OnInit, OnActivate, OnDeactivate {
  /**
   * @param needsLogin: true if this component should only be opened if user is logged in
   * @param compoenentTitle: The readable title of the component that will be displayed in the toolbox.
   */
  constructor(private needsLogin: boolean, private componentTitle: string, private baseLoginStateService: LoginStateService, 
              private baseNavigationStateService: NavigationStateService) {}
  
  public ngOnInit(): any {
    this.validateLoggedIn();
  }
  
  private validateLoggedIn(): void {
    if (this.needsLogin) {
      if (!this.baseLoginStateService.isTicketAvailable())
        this.baseLoginStateService.loginAndReturnHere();
    }
  }
  
  public routerOnActivate(nextInstruction: ComponentInstruction, prevInstruction: ComponentInstruction): any {
    // Note: may not be called in component implements OnReuse. In our case this is AnalysisMainComponent, but that one
    // does not need to re-set the title anyway.
    if (this.baseNavigationStateService)
      this.baseNavigationStateService.setCurrentTitle(this.componentTitle);
  }
  
  public routerOnDeactivate(nextInstruction: ComponentInstruction, prevInstruction: ComponentInstruction): any {
    if (this.baseNavigationStateService)
      this.baseNavigationStateService.setCurrentTitle(undefined);
  }
}