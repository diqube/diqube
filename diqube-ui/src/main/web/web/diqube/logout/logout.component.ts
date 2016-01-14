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



import {Component, OnInit} from "angular2/core";
import {Router} from "angular2/router";
import {LogoutJsonCommandConstants} from "../remote/remote";
import {RemoteService} from "../remote/remote.service";
import {LoginStateService} from "../login-state/login-state.service";

@Component({
    selector: "diqube-logout",
    template: "<div>Logging out...</div>"
})
export class LogoutComponent implements OnInit {
  constructor(private loginStateService: LoginStateService, private remoteService: RemoteService) {}
  
  public static navigate(router: Router) {
    router.navigate([ "/Logout" ]);
  }
  
  public ngOnInit(): any {
    if (!this.loginStateService.isTicketAvailable()) {
      // this will navigate away.
      this.loginStateService.logoutSuccessful();
      return;
    }
    
    var me: LogoutComponent = this;
    this.remoteService.execute(LogoutJsonCommandConstants.NAME, null, {
      data: (dataType: string, data: any) => {
        return false;
      },
      exception: (msg: string) => {
        console.warn("Could not log out:", msg);
      },
      done: () => {
        me.loginStateService.logoutSuccessful();
      }
    });
  }
}