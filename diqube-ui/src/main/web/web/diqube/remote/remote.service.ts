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
import {DiqubeUtil} from "../util/diqube.util";
import {JsonResultEnvelope} from "./remote";
import {LoginStateService} from "../login-state/login-state.service";

/**
 * Callback interface passed to the RemoteService on an execution request: Gets informed as soon as there are results.
 */
export interface RemoteServiceExecutionCallback {
  /**
   * New data is available.
   * 
   * @param dataType: Type of data available.
   * @param data: The data itself.
   * @return "true" signals that the request has been processed fully and leads to the RemoteService to cleanup
   *         everything of the request. If the server sends any additional information, it will not be passed on to the
   *         callback any more.
   */
  data(dataType: string, data: any): boolean;
  
  /**
   * An exception occurred. No more methods will be called for the request on the callback.
   */
  exception(message: string): void;
  
  /**
   * The server informed us that it is done processing the request. The callback will not be called any more. 
   */
  done(): void;
}

@Injectable()
export class RemoteService {
  
  private baseUrlWithoutProtocol: string;
  private socketProtocol: string;
  private requestRegistry : { [requestId: string]: RemoteServiceExecutionCallback; } = {};
  private socket: DiqubeWebSocket;
  private nextRequestIdNumber: number = Number.MIN_SAFE_INTEGER;
  
  constructor(private loginStateService: LoginStateService) {
    this.baseUrlWithoutProtocol = "://" + window.location.host + DiqubeUtil.globalContextPath();
    if (window.location.protocol.toLowerCase() === "https")
      this.socketProtocol = "wss";
    else
      this.socketProtocol = "ws";
  } 
  
  /**
   * Execute the given command, providing the given commandData. The resultHandler will be called as soon as any results arrive. 
   * 
   * Returns the requestId of the new request which can be used to e.g. cancel the request.
   */
  public execute(commandName: string, commandData: any, resultHandler: RemoteServiceExecutionCallback) : string {
    var requestId: string = "id" + this.nextRequestIdNumber;
    this.nextRequestIdNumber += 1;
    
    var sock: DiqubeWebSocket = this.getSocket();
    this.requestRegistry[requestId] = resultHandler;
    
    var cleanCmdData = this.cleanCommandData(DiqubeUtil.copy(commandData));
    
    sock.send({
      requestId: requestId, 
      command: commandName,
      commandData: cleanCmdData,
      ticket: this.loginStateService.ticket
    });
    
    console.debug("Sending request", requestId, ":", commandName, ":", cleanCmdData); 
    
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
  
  /**
   * Get the current Socket/open a new one.
   */
  private getSocket(): DiqubeWebSocket {
    if (this.socket != null)
      return this.socket;
    
    var url: string = this.socketProtocol + this.baseUrlWithoutProtocol + "/socket";
    this.socket = new DiqubeWebSocket({
      url : url,
      lazy : false,
      reconnect : true,
      reconnectInterval : 2000,
      enqueue : true,
      protocols: undefined
    });
    
    this.socket.on(DiqubeWebSocketEventId.MESSAGE, (msg: any) => {
      this.socketMessage(msg);
    });
    this.socket.on(DiqubeWebSocketEventId.CLOSE, () => {
      this.socketClose();
    });
    
    return this.socket;
  }
        
  /**    
   * Called when a message is received on the Websocket.
   * @param resultEnvelope The data from the server.
   * @returns -
   */
  private socketMessage(resultEnvelopePlain: any) {
    if (typeof resultEnvelopePlain === "string")
      // looks like sometimes we get a pure string forwarded here - ignore that.
      return;
    var resultEnvelope: JsonResultEnvelope = <JsonResultEnvelope>resultEnvelopePlain;
    var requestId: string = resultEnvelope.requestId;
    var status: string = resultEnvelope.status;
    var data: any = resultEnvelope.data;
    var dataType: string = resultEnvelope.dataType;
    
    console.debug("Received message on websocket: ", resultEnvelope);
    
    var res : RemoteServiceExecutionCallback = this.requestRegistry[requestId];
    if (status != "done" && res == null) {
      console.warn("Received data from websocket, but requestId unknown: ", resultEnvelope);
      return;
    } 
    
    if (status === "data") {
      var messageRes: boolean = res.data(dataType, data);
      if (messageRes == true)
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
      this.loginStateService.logoutSuccessful();
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
