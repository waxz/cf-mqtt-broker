import { DurableObject } from "cloudflare:workers";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    const MQTT_PATH = env.MQTT_PATH || "/mqtt";

    if (url.pathname === MQTT_PATH) {
      const subprotocol = request.headers.get('Sec-WebSocket-Protocol');
      const upgradeHeader = request.headers.get('Upgrade');
      console.log(`Subprotocol ${subprotocol}, upgradeHeader: ${upgradeHeader}`);

      if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
        return new Response('Expected WebSocket', { status: 426 });
      }
      const brokerId = env.MQTT_BROKER.idFromName('global-broker');
      const broker = env.MQTT_BROKER.get(brokerId);
      return broker.fetch(request);
    }
    return new Response('MQTT Broker Ready', { status: 200 });
  }
};

// ============================================================================
// DURABLE OBJECT: Max Performance MQTT Broker (Batched I/O)
// ============================================================================

export class MQTTBroker extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.state = state;
    this.MQTT_PATH = env.MQTT_PATH || "/mqtt";
    
    this.clients = new Map(); 
    this.wsToClient = new Map(); 
    this.topics = new Map(); 
    this.buffers = new Map(); 
    
    this.decoder = new TextDecoder();
    this.encoder = new TextEncoder();

    // OPTIMIZATION: Outgoing Batch Queue
    // Map<WebSocket, Array<Uint8Array>>
    this.batchQueue = new Map();

    // STATS
    this.stats = {
      rxBytes: 0, txBytes: 0, rxPackets: 0, txPackets: 0, lastReport: Date.now()
    };
    setInterval(() => this.reportStats(), 10000);
  }
  
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === this.MQTT_PATH) {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.state.acceptWebSocket(server);
      return new Response(null, {
        status: 101, webSocket: client,
        headers: { "Sec-WebSocket-Protocol": request.headers.get('Sec-WebSocket-Protocol') || "mqtt" }
      });
    }
    return new Response('Not found', { status: 404 });
  }

  // ============================================================================
  // STATS
  // ============================================================================
  reportStats() {
    const now = Date.now();
    const seconds = (now - this.stats.lastReport) / 1000;
    if (seconds === 0) return;

    const rxBps = this.stats.rxBytes / seconds;
    const txBps = this.stats.txBytes / seconds;
    const rxRps = this.stats.rxPackets / seconds;
    const txRps = this.stats.txPackets / seconds;

    const fmtBytes = (b) => b > 1048576 ? (b/1048576).toFixed(2)+' MB/s' : (b/1024).toFixed(2)+' KB/s';

    console.log(`[STATS] Clients: ${this.clients.size} | RX: ${fmtBytes(rxBps)} (${rxRps.toFixed(0)}/s) | TX: ${fmtBytes(txBps)} (${txRps.toFixed(0)}/s)`);
    
    this.stats.rxBytes = 0; this.stats.txBytes = 0;
    this.stats.rxPackets = 0; this.stats.txPackets = 0;
    this.stats.lastReport = now;
  }

  // ============================================================================
  // BATCHED I/O SYSTEM (The Bottleneck Fix)
  // ============================================================================

  /**
   * Instead of sending immediately, push to a list.
   * This is extremely fast (push is O(1)).
   */
  enqueue(ws, data) {
    let queue = this.batchQueue.get(ws);
    if (!queue) {
      queue = [];
      this.batchQueue.set(ws, queue);
    }
    queue.push(data);
  }

  /**
   * Called ONCE at the end of webSocketMessage.
   * Combines all small packets into one big WebSocket Frame.
   */
  flushBatches() {
    if (this.batchQueue.size === 0) return;

    for (const [ws, chunks] of this.batchQueue) {
      try {
        // 1. Calculate total size
        let totalLen = 0;
        for (let i = 0; i < chunks.length; i++) totalLen += chunks[i].length;

        // 2. Allocate ONE buffer
        const combined = new Uint8Array(totalLen);
        
        // 3. Merge (Fast memcpy)
        let offset = 0;
        for (let i = 0; i < chunks.length; i++) {
          combined.set(chunks[i], offset);
          offset += chunks[i].length;
        }

        // 4. Send ONCE
        ws.send(combined);

        // Stats
        this.stats.txBytes += totalLen;
        this.stats.txPackets += chunks.length;

      } catch (e) {
        // Socket dead, cleanup happens elsewhere
      }
    }
    
    this.batchQueue.clear();
  }

  // ============================================================================
  // WEBSOCKET HANDLER
  // ============================================================================
  
  async webSocketMessage(ws, message) {
    try {
      // 1. Convert Input
      let chunk;
      if (message instanceof ArrayBuffer) chunk = new Uint8Array(message);
      else if (ArrayBuffer.isView(message)) chunk = new Uint8Array(message.buffer, message.byteOffset, message.byteLength);
      else chunk = this.encoder.encode(message);

      this.stats.rxBytes += chunk.length;

      // 2. Handle Buffering (Reassembly)
      let buffer = chunk;
      const existing = this.buffers.get(ws);
      if (existing) {
        buffer = new Uint8Array(existing.length + chunk.length);
        buffer.set(existing);
        buffer.set(chunk, existing.length);
      }

      // 3. Process All Packets in Buffer
      const remaining = this.processBytes(ws, buffer);
      
      if (remaining) this.buffers.set(ws, remaining);
      else if (existing) this.buffers.delete(ws);

    } catch (error) {
      console.error('Err:', error);
      this.removeClient(ws);
      ws.close();
    } finally {
      // 4. CRITICAL: Flush all queued responses created during processing
      this.flushBatches();
    }
  }

  async webSocketClose(ws) { this.removeClient(ws); }
  async webSocketError(ws) { this.removeClient(ws); }

  // ============================================================================
  // PARSER
  // ============================================================================
  
  processBytes(ws, buffer) {
    let pos = 0;
    const len = buffer.length;

    while (pos < len) {
      if (len - pos < 2) break; // Need at least Type + Len

      // Decode VBI (Length)
      let tempPos = pos + 1;
      let multiplier = 1;
      let bodyLength = 0;
      let shift = 0;
      let malformed = false;

      while (true) {
        if (tempPos >= len) return buffer.slice(pos); // Incomplete
        const byte = buffer[tempPos];
        bodyLength += (byte & 127) * multiplier;
        multiplier *= 128;
        tempPos++;
        shift++;
        if ((byte & 128) === 0) break;
        if (shift > 4) { malformed = true; break; }
      }

      if (malformed) { ws.close(); return null; }

      const totalPacketSize = 1 + shift + bodyLength;
      if (len - pos < totalPacketSize) return buffer.slice(pos); // Incomplete

      // Valid Packet Found
      this.stats.rxPackets++;
      
      // Zero-copy view of the packet
      const packetView = buffer.subarray(pos, pos + totalPacketSize);
      
      this.processPacket(ws, packetView);

      pos += totalPacketSize;
    }

    if (pos === len) return null;
    return buffer.slice(pos);
  }

  processPacket(ws, data) {
    const packetType = (data[0] >> 4) & 0x0F;
    switch (packetType) {
      case 3: this.handlePublish(data, ws); break; // Hot path
      case 1: this.handleConnect(data, ws); break;
      case 8: this.handleSubscribe(data, ws); break;
      case 12: this.enqueue(ws, new Uint8Array([0xD0, 0x00])); break; // PINGRESP
      case 10: this.handleUnsubscribe(data, ws); break;
      case 14: this.removeClient(ws); ws.close(); break;
    }
  }

  // ============================================================================
  // LOGIC
  // ============================================================================

  handleConnect(data, ws) {
    try {
      let pos = 1;
      while ((data[pos++] & 128) !== 0) {}; 
      const protocolLen = (data[pos] << 8) | data[pos+1];
      pos += 2 + protocolLen + 4; 
      const clientIdLen = (data[pos] << 8) | data[pos+1];
      pos += 2;
      
      // Optimization: subarray
      let clientId = this.decoder.decode(data.subarray(pos, pos + clientIdLen));
      if (!clientId) clientId = 'anon_' + Math.random().toString(36).slice(2);

      const existing = this.clients.get(clientId);
      if (existing) {
        this.removeClient(existing.ws); 
        try { existing.ws.close(1000, "Taken"); } catch(e){}
      }

      const clientObj = { ws, id: clientId, subs: new Set() };
      this.clients.set(clientId, clientObj);
      this.wsToClient.set(ws, clientObj);

      // Use Enqueue
      this.enqueue(ws, new Uint8Array([0x20, 0x02, 0x00, 0x00]));
    } catch (e) { ws.close(); }
  }

  handlePublish(data, ws) {
    let pos = 1;
    while ((data[pos++] & 128) !== 0) {}; 

    const topicLen = (data[pos] << 8) | data[pos+1];
    pos += 2;
    // We must decode topic to find subscribers
    const topic = this.decoder.decode(data.subarray(pos, pos + topicLen));
    
    // BROADCAST
    const subscribers = this.topics.get(topic);
    if (subscribers) {
      for (const clientObj of subscribers) {
        // QUEUE IT. Don't send yet.
        // Even if we have 50 subscribers, we just push refs to queue.
        // Extremely fast loop.
        this.enqueue(clientObj.ws, data);
      }
    }
    
    // PUBACK if QoS 1
    if ((data[0] & 0x06) === 0x02) {
        pos += topicLen;
        const pidH = data[pos];
        const pidL = data[pos+1];
        this.enqueue(ws, new Uint8Array([0x40, 0x02, pidH, pidL]));
    }
  }

  handleSubscribe(data, ws) {
    const clientObj = this.wsToClient.get(ws);
    if (!clientObj) return;

    let pos = 1;
    while ((data[pos++] & 128) !== 0) {}; 
    const packetIdH = data[pos];
    const packetIdL = data[pos+1];
    pos += 2;

    const granted = [];
    while (pos < data.length) {
      const topicLen = (data[pos] << 8) | data[pos+1];
      pos += 2;
      const topic = this.decoder.decode(data.subarray(pos, pos + topicLen));
      pos += topicLen;
      pos++; // QoS

      clientObj.subs.add(topic);
      let topicSubs = this.topics.get(topic);
      if (!topicSubs) {
        topicSubs = new Set();
        this.topics.set(topic, topicSubs);
      }
      topicSubs.add(clientObj);
      granted.push(0x00);
    }

    this.enqueue(ws, new Uint8Array([0x90, 2 + granted.length, packetIdH, packetIdL, ...granted]));
  }

  handleUnsubscribe(data, ws) {
    const pidH = data[data.length-2];
    const pidL = data[data.length-1];
    this.enqueue(ws, new Uint8Array([0xB0, 0x02, pidH, pidL]));
  }

  removeClient(ws) {
    const clientObj = this.wsToClient.get(ws);
    if (!clientObj) return;

    for (const topic of clientObj.subs) {
      const topicSubs = this.topics.get(topic);
      if (topicSubs) {
        topicSubs.delete(clientObj);
        if (topicSubs.size === 0) this.topics.delete(topic);
      }
    }

    this.clients.delete(clientObj.id);
    this.wsToClient.delete(ws);
    this.buffers.delete(ws);
    this.batchQueue.delete(ws); // Clear any pending sends
  }
}
