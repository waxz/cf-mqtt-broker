import mqtt from "mqtt";

// Configuration
let HTTP_HOST = "http://localhost:8788";


let MQTT_PATH = "/mqtt_sec1"

const PUBLISH_RATE=1; //2000ms = 2 seconds

console.log("--- Starting MQTT Client ---");

// 1. Optional: Health Check
try {
  const response = await fetch(HTTP_HOST);
  console.log(`Health check [${HTTP_HOST}] status:`, response.status);
  if(response.ok) console.log(`Health check response:`, await response.text());
} catch (e) {
  console.warn("HTTP Health check failed:", e.message);
}

// 2. Construct URL
const targetUrl = new URL(HTTP_HOST);
targetUrl.protocol = targetUrl.protocol === 'https:' ? 'wss:' : 'ws:';
targetUrl.pathname = MQTT_PATH;
const MQTT_URL = targetUrl.toString();
console.log(`Connecting to MQTT URL: ${MQTT_URL}`);

// 3. Connect options
const options = {
  clientId: 'client_' + Math.random().toString(16).substr(2, 8),
  protocolVersion: 4,
  keepalive: 60,
  clean: true,
  reconnectPeriod: 1000,
};

const client = mqtt.connect(MQTT_URL, options);

// Variable to hold the loop timer
let publishInterval = null;
let msgCounter = 0;

// 4. Event Handlers
client.on('connect', () => {
  console.log('✅ Connected to MQTT broker');
  
  // Subscribe first
  client.subscribe('test/topic', (err) => {
    if (!err) {
      console.log('📡 Subscribed to test/topic');
      
      // --- START PUBLISH LOOP ---
      console.log('🔄 Starting publish loop (every 2s)...');
      
      // Clear any existing interval just in case
      if (publishInterval) clearInterval(publishInterval);

      publishInterval = setInterval(() => {
        msgCounter++;
        
        const payload = JSON.stringify({
          id: msgCounter,
          ts: new Date().toLocaleTimeString(),
          msg: "Hello from Client loop! msgCounter: " + msgCounter
        });

        console.log(`📤 Publishing: ${payload}`);
        client.publish('test/topic', payload);
        
      }, PUBLISH_RATE); //
      
    } else {
      console.error('Subscribe error:', err);
    }
  });
});

client.on('message', (topic, message) => {
  // message is Buffer
  console.log(`📩 Received on [${topic}]: ${message.toString()}`);
});

client.on('error', (error) => {
  console.error('❌ Connection error:', error.message);
});

client.on('offline', () => {
  console.log('⚠️ Client is offline');
  // Stop loop if offline so logs don't spam
  if (publishInterval) clearInterval(publishInterval);
});

client.on('close', () => {
  console.log('Connection closed');
  if (publishInterval) clearInterval(publishInterval);
});

// Debuggers (Optional: comment out if too noisy)
client.on('packetsend', (packet) => {
  if (packet.cmd !== 'publish') { // Filter out publish logs to keep console clean
     console.log('>> Sending packet:', packet.cmd);
  }
});

client.on('packetreceive', (packet) => {
  if (packet.cmd !== 'publish') {
    console.log('<< Received packet:', packet.cmd);
  }
});