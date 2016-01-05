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

/**
 * Handles any events received on a DiqubeWebSocket.
 */
export interface DiqubeWebSocketHandler {
  (data: any): void;
}

class DiqubeWebSocketConfig {
  /** Remote URL*/
  public url: string = undefined;
  /** Should the WebSocket connect lazily? If not, the connection will be established by the constructor. */ 
  public lazy: boolean = false; 
  /** Should the websocket re-connect on connection loss? */
  public reconnect: boolean = true; 
  /** Interval after which to check if connection is still up and possibly re-connect. Milliseconds. */
  public reconnectInterval: number = 2000; 
  /** If connection is lost, should messages be enqueued and sent as soon as connection is re-established. */
  public enqueue: boolean = false; 
  /** Sub-protocols that are supported. */
  public protocols: string[] = undefined;
}

enum State {
  CONNECTING, OPEN, CLOSED 
}

export enum DiqubeWebSocketEventId {
  OPEN,
  MESSAGE,
  ERROR,
  CLOSE
}

/**
 * Encapsulation of a websocket that is capable of reconnecting on connection loss and queues all messages in that case.
 * 
 * This is influenced by ng-websocket from https://github.com/wilk/ng-websocket.
 */
export class DiqubeWebSocket {
  /** registered handlers */
  private eventMap: {[eventId: number]: DiqubeWebSocketHandler[]; } = {};
  /** The actual WebSocket */
  private ws: WebSocket = undefined;
  private reconnectTask: any = undefined;
  private reconnectCopy: any = true;
  /** Messages that should be sent as soon as a connection is re-established. */ 
  private queue: any = [];
  private config: DiqubeWebSocketConfig = new DiqubeWebSocketConfig();
  private closedOnPurpose: boolean = false;
  
  constructor(config: DiqubeWebSocketConfig) {
    for (var field in config) {
      if ((<any>config)[field] !== undefined)
        (<any>this.config)[field] = (<any>config)[field];
    }
    
    if (!this.config.lazy) this.doOpen();
  }

  private fireEvent(eventId: DiqubeWebSocketEventId, data?: any): void {
    var handlers: DiqubeWebSocketHandler[] = this.eventMap[eventId.valueOf()];
    if (handlers)
      handlers.forEach(h => {
        h(data);
      });
  }
  
  /**
   * Internal implementation which creates a new WebSocket.
   */
  private doOpen(): void {
    if (this.config.protocols !== undefined) 
      this.ws = new WebSocket(this.config.url, this.config.protocols);
    else 
      this.ws = new WebSocket(this.config.url);
  
    this.closedOnPurpose = false;
    
    this.ws.onmessage = (message: MessageEvent) => {
        try {
          this.fireEvent(DiqubeWebSocketEventId.MESSAGE, JSON.parse(message.data));
        }
        catch (err) {
          this.fireEvent(DiqubeWebSocketEventId.MESSAGE, message.data);
        }
    };

    this.ws.onerror = (error: any) => {
      console.warn("Websocket error", error);
      this.fireEvent(DiqubeWebSocketEventId.ERROR, error);
    };

    this.ws.onopen = () => {
      // Clear the reconnect task if exists
      if (this.reconnectTask) {
          clearInterval(this.reconnectTask);
          delete this.reconnectTask;
      }

      // Flush the message queue
      if (this.config.enqueue && this.queue.length > 0) {
          while (this.queue.length > 0) {
              if (this.ready()) 
                this.send(this.queue.shift());
              else 
                break;
          }
      }

      this.fireEvent(DiqubeWebSocketEventId.OPEN);
    };

    this.ws.onclose = (close: CloseEvent) => {
      if (!this.closedOnPurpose)
        console.warn("Websocket closed unexpectedly!");
      
      if (!this.closedOnPurpose && this.config.reconnect) {
        this.reconnectTask = setInterval(() => {
          if (this.state() === State.CLOSED) 
            this.open();
        }, this.config.reconnectInterval);
      }
      
      this.fireEvent(DiqubeWebSocketEventId.CLOSE);
    };
  }
  
  private state(): State {
    if (this.ws === undefined)
      return State.CLOSED;
    if (this.ws.readyState === 0)
      return State.CONNECTING;
    if (this.ws.readyState === 1)
      return State.OPEN;
    if (this.ws.readyState >= 2)
      return State.CLOSED;
    
    console.warn("Unknown state in websocket!", this.ws);
    return undefined;
  }
  
  /**
   * Register handler on events.
   * 
   */
  public on(eventId: DiqubeWebSocketEventId, handler: DiqubeWebSocketHandler): void {
    this.eventMap[eventId] = this.eventMap[eventId] || [];
    this.eventMap[eventId].push(handler);
  }
 
  /**
   * Send a message to the remote. Message is treated as JavaScript object and will be sent as JSON.
   */
  public send(message: any): void {
    if (this.ready()) 
      this.ws.send(JSON.stringify(message));
    else if (this.config.enqueue) 
      this.queue.push(message);
  }

  /**
   * Opens the websocket. If this websocket is "lazy", this needs to actively be called to open the connection.
   */
  public open(): void {
    if (this.state() !== State.OPEN) 
      this.doOpen();
  }

  /**
   * Close the websocket.
   */
  public close(): void {
    this.closedOnPurpose = true; 
    if (this.state() !== State.CLOSED) 
      this.ws.close();

    if (this.reconnectTask) {
      clearInterval(this.reconnectTask);
      delete this.reconnectTask;
    }
  }

  /**
   * Returns true if this websocket is ready to send data directly (= it is opened).
   */
  public ready(): boolean {
    return this.state() === State.OPEN;
  };

}