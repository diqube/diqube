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

import {Component, OnInit, Input, ElementRef} from "angular2/core";
import * as remoteData from "../../remote/remote";
import {AnalysisSliceComponent} from "./analysis.slice.component";
import {AnalysisService} from "../analysis.service";

@Component({
  selector: "diqube-analysis-slices",
  templateUrl: "diqube/analysis/slice/analysis.slices.html",
  directives: [ AnalysisSliceComponent ]
})
export class AnalysisSlicesComponent {
  @Input("analysis") public analysis: remoteData.UiAnalysis = undefined;
  
  constructor(private analysisService: AnalysisService) {}
  
  public addSlice(): void {
    this.analysisService.addSlice("New slice", "", [ ]);
  }
}