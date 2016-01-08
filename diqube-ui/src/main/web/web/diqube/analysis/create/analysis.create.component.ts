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
import {Control, ControlGroup, FormBuilder, FORM_DIRECTIVES} from "angular2/common";
import * as remoteData from "../../remote/remote";
import {RemoteService} from "../../remote/remote.service";
import {LoginStateService} from "../../login-state/login-state.service";
import {AnalysisService} from "../../analysis/analysis.service";
import {AnalysisMainComponent} from "../analysis.main.component";

@Component({
  selector: "diqube-analysis-create",
  templateUrl: "diqube/analysis/create/analysis.create.html",
  directives: [ FORM_DIRECTIVES ]
})
export class AnalysisCreateComponent implements OnInit {
  public newAnalysis: { name: string; table: string } = { name: undefined, table: undefined };
  public nameControl: Control;
  public tableControl: Control;
  public formControlGroup: ControlGroup;
  
  public error: string = undefined;
  public creating: boolean = false;
  
  private validTables:Array<string> = undefined;
  
  constructor(private remoteService: RemoteService, private loginStateService: LoginStateService, formBuilder: FormBuilder, 
              private analysisService: AnalysisService, private router: Router) {
    this.nameControl = new Control("defaultName", AnalysisCreateComponent.nameValidator);
    this.tableControl = new Control("defaultTable", (control: Control) => {
      return AnalysisCreateComponent.tableValidator(this, control);
    });
    
    this.formControlGroup = formBuilder.group({
      nameControl: this.nameControl,
      tableControl: this.tableControl
    });
  }
  
  public static navigate(router: Router) {
    router.navigate([ "/Analysis/Create" ]);
  }
  
  public ngOnInit(): any {
    if (!this.loginStateService.isTicketAvailable())
      this.loginStateService.loginAndReturnHere();
  }
  
  public createAnalysis(): void {
    var me: AnalysisCreateComponent = this;
    this.creating = true;
    console.info("Creating analysis", this.newAnalysis.name, "on table", this.newAnalysis.table);
    var data: remoteData.CreateAnalysisJsonCommand = {
      name: this.newAnalysis.name,
      table: this.newAnalysis.table
    };
    var newAnalysis: remoteData.UiAnalysis = undefined;
    this.remoteService.execute(remoteData.CreateAnalysisJsonCommandConstants.NAME, data, {
      data: (dataType: string, data: any) => {
        if (dataType === remoteData.AnalysisJsonResultConstants.TYPE) {
          var res: remoteData.AnalysisJsonResult = <remoteData.AnalysisJsonResult>data;
          console.info("New analysis is", res.analysis);
          newAnalysis = res.analysis;
        }
        return false;
      },
      exception: (msg: string) => {
        me.error = msg;
        me.creating = false;
      },
      done: () => {
        console.log("Created new analysis");
        me.creating = false;
        this.analysisService.setLoadedAnalysis(newAnalysis);
        AnalysisMainComponent.navigate(this.router, newAnalysis.id, newAnalysis.version);
      }
    });
  }
  
  public getValidTables():Array<string> {
    if (!this.validTables) {
      var me: AnalysisCreateComponent = this;
      
      this.validTables = [];
      this.remoteService.execute(remoteData.ListAllTablesJsonCommandConstants.NAME, null, {
        data: (dataType: string, data: any) => {
          if (dataType === remoteData.TableNameListJsonResultConstants.TYPE) {
            var res: remoteData.TableNameListJsonResult = <remoteData.TableNameListJsonResult>data;
            me.validTables = res.tableNames;
          }
          return false;
        },
        exception: (msg: string) => {
          me.validTables = undefined;
        },
        done: () => {
        }
      });
    }
    return this.validTables;
  }
  
  public static nameValidator(control: Control): { [key: string]: boolean; }   { 
     if (!control.value || control.value.trim() === "") 
       return { "empty": true };
     return null;
   }

  public static tableValidator(me: AnalysisCreateComponent, control: Control): { [key: string]: boolean; }   { 
     if (!control.value || control.value.trim() === "") 
       return { "empty": true };
     if (me.getValidTables().indexOf(control.value) == -1) 
       return { "tableDoesNotExist": true };
     return null;
   }
}