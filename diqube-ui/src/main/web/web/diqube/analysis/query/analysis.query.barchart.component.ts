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

import {Component, OnInit, Input, DoCheck, OnDestroy} from "angular2/core";
import {Control, ControlGroup, FormBuilder, FORM_DIRECTIVES} from "angular2/common";
import {AnalysisService} from "../analysis.service";
import {AnalysisStateService} from "../state/analysis.state.service";
import * as remoteData from "../../remote/remote";
import * as analysisData from "../analysis";
import {DiqubeUtil} from "../../util/diqube.util";
import {DragDropService} from "../drag-drop/drag-drop.service";

@Component({
  selector: "diqube-analysis-query-barchart",
  templateUrl: "diqube/analysis/query/analysis.query.barchart.html",
  directives: [ FORM_DIRECTIVES ]
})
export class AnalysisQueryBarchartComponent implements OnInit, DoCheck, OnDestroy {
  
  @Input("query") public query: remoteData.UiQuery = undefined;
  
  /* ID of the canvas HTML element */
  public chartHtmlId: string;
  public barChartPrimaryHtmlId: string;
  
  public chartWidth: number;
  public chartHeight: number;
  
  private chart: any = undefined;
  
  private initialData: Array<any> = undefined;
  private initialLabels: Array<any> = undefined;
  
  private fieldNameXAxis: string = undefined;
  private previousResults: analysisData.EnhancedTableJsonResult = undefined;
  
  private style: any;
  
  private mouseDownListener: (event: MouseEvent) => void;
  private canvasElement: HTMLElement;
  
  constructor(private dragDropService: DragDropService) {
    this.chartWidth = 600;
    this.chartHeight = 300;
    this.chartHtmlId = DiqubeUtil.newUuid();
    this.barChartPrimaryHtmlId = DiqubeUtil.newUuid();
  }
  
  public ngOnInit(): any {
    this.canvasElement = document.getElementById(this.chartHtmlId);
    var barChartPrimaryElement = document.getElementById(this.barChartPrimaryHtmlId);
    if (!this.canvasElement || !barChartPrimaryElement) {
      // if executed before the ID is inside the html element, try again in next tick.
      setTimeout(() => {
        this.ngOnInit();
      }, 0);
      return;
    }
    
    var primaryColor = window.getComputedStyle(barChartPrimaryElement).color;
    var primaryBackgroundColor = window.getComputedStyle(barChartPrimaryElement).backgroundColor;
    
    var data: Array<any>;
    var labels: Array<any>;
    if (this.initialData) {
      // we have initial data already, so use that!
      data = this.initialData;
      this.initialData = undefined;
      labels = this.initialLabels;
      this.initialLabels = undefined;
    } else {
      // no data available. Use empty data. This will be updated by #updateData.
      data = [];
      labels = [];
    }
    this.chart = DiqubeUtil.newChart(this.canvasElement, {
      type: "bar",
      data: {
        labels: labels,
        datasets: [{
          label: "Value",
          backgroundColor: primaryBackgroundColor,
          data: data
        }]
      },
      options:{
        scales: {
          yAxes: [{
            ticks: {
              beginAtZero:true,
              fontFamily: '"Helvetica Neue",Helvetica,Arial,sans-serif',
              fontColor: primaryColor,
            },
          }],
          xAxes: [{
            ticks: {
              fontFamily: '"Helvetica Neue",Helvetica,Arial,sans-serif',
              fontColor: primaryColor,
            },
          }]
        },
        responsive: true,
        maintainAspectRatio: false
      }
    });

    this.mouseDownListener = (event: MouseEvent) => {
      var el: Array<{ _view: { label: string }}> = this.chart.getElementAtEvent(event);
      if (el && el.length) {
        try {
          var draggedValue =  el[0]._view.label;
          if (draggedValue) {
            this.dragDropService.startDragRestriction(this.fieldNameXAxis, draggedValue);
            
            event.stopPropagation();
            event.preventDefault();
          }
        } catch (err) {
          // swallow, apparently no valid drag operation.
        }
      }
    };
    this.canvasElement.addEventListener("mousedown", this.mouseDownListener);
    
    // initial update
    this.updateData();
  }
  
  public ngDoCheck(): any {
    var curResults: analysisData.EnhancedTableJsonResult = (<analysisData.UiQueryWithResults>this.query).$results; // may be undefined
    
    // TODO this equals check might be a bit slow. Implement listener-like structure?
    if (!DiqubeUtil.equals(this.previousResults, curResults)) {
      this.previousResults = DiqubeUtil.copy(curResults);
      this.updateData();
    }
  }
  
  public ngOnDestroy(): any {
    if (this.chart) {
      this.chart.destroy();
    }
    
    if (this.mouseDownListener) {
      this.canvasElement.removeEventListener("mousedown", this.mouseDownListener);
    }
  }
  
  private updateData(): void {
    var targetData: Array<any> = [];
    var targetLabels: Array<any> = [];
    
    if (analysisData.isUiQueryWithResults(this.query)) {
      var res: analysisData.EnhancedTableJsonResult = (<analysisData.UiQueryWithResults>this.query).$results;
      
      for (var idx in res.rows) {
        targetData.push(res.rows[idx][1]);
        targetLabels.push(res.rows[idx][0]);
      }
      
      this.fieldNameXAxis = res.columnRequests[0];
    } else 
      this.fieldNameXAxis = "";

    if (this.chart) {
      // update chart directly.
      this.chart.data.datasets[0].data = targetData;
      this.chart.data.labels = targetLabels;
      this.chart.update();
    } else {
      // store data in "initialData" in case this is called before we have a chart.
      this.initialData = targetData;
      this.initialLabels = targetLabels;
    }
  }
  
  public getStyle(): any {
    if (!this.style)
      this.style = {
        width: this.chartWidth + "px",
        height: this.chartHeight + "px"
      };
    
    return this.style;
  }
}