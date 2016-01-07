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
import {AnalysisRefJsonResult, AnalysisRefJsonResultConstants, ListAllAnalysisJsonCommandConstants} from "../remote/remote";
import {RemoteService} from "../remote/remote.service";

@Component({
    selector: "diqube-open-analysis",
    templateUrl: "diqube/open-analysis/open-analysis.html"
})
export class OpenAnalysisComponent implements OnInit {
  public dropdownIsOpen: boolean = false;
  public overallTitle: string = "";
  public overallText: string = "";
  public analysis: AnalysisRefJsonResult[] = [];
  public loading: boolean = false;
  
  constructor(private remoteService: RemoteService) { }
  
  public ngOnInit(): any {
    this.reloadAnalysis();
  }
  
  public reloadAnalysis(): void {
    this.analysis = [];
    this.remoteService.execute(ListAllAnalysisJsonCommandConstants.NAME, null, {
      data: (dataType: string, data: any) => {
        if (dataType === AnalysisRefJsonResultConstants.TYPE) {
          this.analysis.push(<AnalysisRefJsonResult>data);
        }
        return false;
      },
      exception: (msg: string) => {},
      done: () => {} 
    });
  }

  public openAnalysis(analysis: {id: string; name: string; }): void {
    // TODO
  }
}
