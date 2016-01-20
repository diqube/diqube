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
import {Router} from "angular2/router";
import {RemoteService} from "../remote/remote.service";
import {VersionJsonResult, VersionJsonResultConstants, VersionJsonCommandConstants} from "../remote/remote";
import {NavigationStateService} from "../navigation-state/navigation-state.service";
import {DiqubeBaseNavigatableComponent} from "../diqube.base.component";

@Component({
    selector: "diqube-about",
    templateUrl: "diqube/about/about.html"
})
export class AboutComponent extends DiqubeBaseNavigatableComponent {
  gitCommitLong: string;
  gitCommit: string;
  buildTimestamp: string;
  
  constructor(remoteService: RemoteService, navigationStateService: NavigationStateService) {
    super(false, "About", undefined, navigationStateService);
    
    var me: AboutComponent = this;
    remoteService.execute(VersionJsonCommandConstants.NAME, null, {
      data: (dataType: string, data: any) => {
        if (dataType === VersionJsonResultConstants.TYPE) {
          var realData: VersionJsonResult = <VersionJsonResult>data;
          me.gitCommitLong = realData.gitCommitLong;
          me.gitCommit = realData.gitCommitShort;
          me.buildTimestamp = realData.buildTimestamp;
        }
        return false;
      },
      exception: (msg: string) => {},
      done: () => {}
    });
  }
  
  public static navigate(router: Router) {
    router.navigate([ "/About" ]);
  }
}
