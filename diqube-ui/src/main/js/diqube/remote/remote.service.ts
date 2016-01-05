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
import {DiqubeWebSocket, DiqubeWebSocketEventId, DiqubeWebSocketHandler} from "./websocket";
import {DiqubeUtil} from "../diqube.util";

export interface RemoteServiceExecutionCallback {
  data(dataType: string, data: any): boolean;
  exception(message: string): void;
  done(): void;
}

@Injectable()
export class RemoteService {
  
  private baseUrlWithoutProtocol: string;
  private socketProtocol: string;
  private requestRegistry : { [requestId: string]: RemoteServiceExecutionCallback; } = {};
  private socket: DiqubeWebSocket;
  private nextRequestIdNumber: number = Number.MIN_SAFE_INTEGER;
  
  public execute(commandName: string, commandData: any, resultHandler: RemoteServiceExecutionCallback) : string {
    var requestId = "id" + this.nextRequestIdNumber;
    this.nextRequestIdNumber += 1;
    
    var sock = this.getSocket();
    this.requestRegistry[requestId] = resultHandler;
    
    var cleanCmdData = this.cleanCommandData(DiqubeUtil.copy(commandData));
    
    sock.send({
      requestId: requestId, 
      command: commandName,
      commandData: cleanCmdData //,
 //     ticket: loginStateService.ticket
    });
    
    return requestId;
  }
  
  /**
   * Cancel a specific request.
   * 
   * @param requestId The result of the execute call.
   */
  public cancel(requestId: string) : void {
    console.info("Cancelling request ", requestId);
    this.getSocket().send({
      requestId: requestId,
      command: "cancel"
    });
    this.cleanupRequest(requestId);
  }
  
  /**
   * Removes all properties starting with "$" recursively.
   */
  private cleanCommandData(data: any): any {
    for (var prop in data) {
      if (data.hasOwnProperty(prop)) {
        if (typeof data[prop] === "object") {
          if (Array.isArray(data[prop])) {
            for (var idx in data[prop])
              this.cleanCommandData(data[prop][idx]);
          } else {
            if (prop.startsWith("$"))
              delete data[prop];
            else
              this.cleanCommandData(data[prop]);
          }
        }
      }
    }
    return data;
  }
  
  private getSocket(): DiqubeWebSocket {
    if (this.socket != null)
      return this.socket;
    
    var url = this.socketProtocol + this.baseUrlWithoutProtocol + "/socket";
    this.socket = new DiqubeWebSocket({
      url : url,
      lazy : false,
      reconnect : true,
      reconnectInterval : 2000,
      enqueue : true,
      protocols: undefined
    });
    
    this.socket.on(DiqubeWebSocketEventId.MESSAGE, this.socketMessage);
    this.socket.on(DiqubeWebSocketEventId.CLOSE, this.socketClose);
    
    return this.socket;
  }
        
//  private initialize($location) {
//    me.$baseUrlWithoutProtocol = "://" + $location.host() + ":" + $location.port() + globalContextPath;
//    if ($location.protocol().toLowerCase() === "https")
//      me.$socketProtocol = "wss";
//    else
//      me.$socketProtocol = "ws";
//  }
  
  /**    
   * Called when a message is received on the Websocket.
   * @param resultEnvelope The data from the server.
   * @returns -
   */
  private socketMessage(resultEnvelopePlain: any) {
    if (typeof resultEnvelopePlain === "string")
      // looks like sometimes we get a pure string forwarded here - ignore that.
      return;
    var resultEnvelope: any = resultEnvelopePlain; // TODO
    var requestId: string = resultEnvelope.requestId;
    var status = resultEnvelope.status;
    var data = resultEnvelope.data;
    var dataType = resultEnvelope.dataType;
    
    console.debug("Received message on websocket: ", resultEnvelope);
    
    var res : RemoteServiceExecutionCallback = this.requestRegistry[requestId];
    if (status != "done" && res == null) {
      console.warn("Received data from websocket, but requestId unknown: ", resultEnvelope);
      return;
    } 
    
    if (status === "data") {
      var messageRes = res.data(dataType, data);
      if (messageRes === true)
        this.cleanupRequest(requestId);
    } else if (status === "done") {
      res.done();
      this.cleanupRequest(requestId);
    } else if (status === "exception") {
      console.warn("Exception on request ", requestId, ": ", data);
      res.exception(data.text);
      this.cleanupRequest(requestId);
    } else if (status === "authenticationException") {
      console.warn("Server did not accept our ticket. Executing automatic logout.");
      this.cleanupRequest(requestId);
//      loginStateService.logoutSuccessful();
    }
  }
  
  private socketClose() {
  }
  
  /**
   * Cleans up resources reserved for a specific requestId. 
   */
  private cleanupRequest(requestId: string):void {
    console.debug("Cleaning up request ", requestId);
    delete this.requestRegistry[requestId]
  }

}
