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
import {TicketJsonResult, TicketJsonResultConstants, LoginJsonCommand, LoginJsonCommandConstants} from "../remote/remote";
import {RemoteService} from "../remote/remote.service";
import {LoginStateService} from "../login-state/login-state.service";

@Component({
    selector: "diqube-login",
    templateUrl: "diqube/login/login.html"
})
export class LoginComponent implements OnInit {
  private static DEFAULT_ROUTER_TARGET: string = "About";  
  
  public login: { username: string; password: string; setCookie: boolean; } = { username: "", password: "", setCookie: false};
  public isLoggingIn: boolean = false; 
  public error: string = undefined;
  
  constructor(private loginStateService: LoginStateService, private remoteService: RemoteService) {}
  
  public static navigate(router: Router) {    
    router.navigate([ "/Login" ]);
  }
  public ngOnInit(): any {
    if (this.loginStateService.isTicketAvailable())
      // this will navigate away from the login page.
      this.loginStateService.loginSuccessful(this.loginStateService.ticket, this.loginStateService.username);
  }
  
  public executeLogin(): void {
    var me: LoginComponent = this;
    var data: LoginJsonCommand = {
      username: this.login.username,
      password: this.login.password
    };
    this.error = undefined;
    this.isLoggingIn = true;
    this.remoteService.execute(LoginJsonCommandConstants.NAME, data, {
      data: (dataType: string, data: any) => {
        if (dataType === TicketJsonResultConstants.TYPE) {
          var ticket: TicketJsonResult = <TicketJsonResult>data;
          me.loginStateService.storeTicketInCookie = me.login.setCookie;
          me.loginStateService.loginSuccessful(ticket.ticket, ticket.username);
        }
        return false;
      },
      exception: (msg: string) => {
        me.isLoggingIn = false;
        me.error = msg;
      },
      done: () => {
        me.isLoggingIn = false;
      }
    });
  }
}