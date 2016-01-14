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

import {Injectable} from "angular2/core";
import {Router} from "angular2/router";
import {CookiesService} from "../cookies/cookies.service";
import {DiqubeUtil} from "../util/diqube.util";

@Injectable()
export class LoginStateService {
  private static DEFAULT_ROUTER_TARGET_AFTER_LOGIN: string = "About";  
  
  private static TICKET_COOKIE: string = "DiqubeTicket";
  private static USERNAME_COOKIE: string = "DiqubeUser";  
  
  public ticket: string = undefined;
  public username: string;
  public storeTicketInCookie: boolean = false;
  
  private urlToOpenAfterSuccessfulLogin: string = undefined;
  
  constructor(private router: Router, private cookiesService: CookiesService) {
    this.ticket = this.cookiesService.getCookie(LoginStateService.TICKET_COOKIE);
    this.username = this.cookiesService.getCookie(LoginStateService.USERNAME_COOKIE);
    
    if ((!this.ticket && this.username) || (this.ticket && !this.username)) {
      // only one cookie available, remove both.
      this.logoutSuccessful();
    }  
  }
  
  public loginSuccessful(ticket: string, username: string): void {
    this.ticket = ticket;
    this.username = username;
    this.adjustCookies();
    
    if (this.urlToOpenAfterSuccessfulLogin !== undefined) {
      var newLocation: string = this.urlToOpenAfterSuccessfulLogin;
      this.urlToOpenAfterSuccessfulLogin = undefined;
      this.router.navigateByUrl(newLocation);
    } else {
      this.router.navigate([ LoginStateService.DEFAULT_ROUTER_TARGET_AFTER_LOGIN ]);
    }
  }
  
  public logoutSuccessful(): void {
    this.ticket = undefined;
    this.username = undefined;
    this.adjustCookies();
    this.router.navigate([ "Login" ]);
  }
  
  public loginAndReturnHere(): void {
    this.urlToOpenAfterSuccessfulLogin = window.location.pathname.substring(DiqubeUtil.globalContextPath().length);
    this.router.navigate([ "Login" ]);
  }
  
  public isTicketAvailable(): boolean {
    return this.ticket !== undefined;
  }
  
  private adjustCookies(): void {
    if (!this.storeTicketInCookie || !this.isTicketAvailable()) {
      this.cookiesService.removeCookie(LoginStateService.TICKET_COOKIE);
      this.cookiesService.removeCookie(LoginStateService.USERNAME_COOKIE);
    } else {
      this.cookiesService.setCookie(LoginStateService.TICKET_COOKIE, this.ticket);
      this.cookiesService.setCookie(LoginStateService.USERNAME_COOKIE, this.username);
    }
  }
}
