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

@Injectable()
export class CookiesService {
  
  public setCookie(cookieName: string, cookieValue: string): void {
    document.cookie = cookieName + "=" + cookieValue;
  }
  
  public removeCookie(cookieName: string): void {
    document.cookie = cookieName + "=;expires=Thu, 01 Jan 1970 00:00:01 GMT;";
  }
  
  public getCookie(cookieName: string): string {
    if (!document.cookie)
      return undefined;
    
    var cookiesString: string = document.cookie;

    var simpleLocation:number = cookiesString.indexOf(cookieName + "=");
    if (simpleLocation === -1)
      return undefined;
    
    if (simpleLocation !== 0) {
      var finalLocation: number = cookiesString.indexOf(";" + cookieName + "=");
      if (finalLocation === -1)
        finalLocation = cookiesString.indexOf("; " + cookieName + "=");
      if (finalLocation === -1) {
        console.warn("Could not find location of cookie", cookieName, "although it should be available!");
        return undefined;
      }
      cookiesString = cookiesString.substr(finalLocation);
    }
    
    // now, the searched cookie is the first in cookiesString.
    
    var cookieValue: string = cookiesString.replace(/[^=]*?\=\s*([^;]*).*$/, "$1");
    
    return cookieValue;
  }
}