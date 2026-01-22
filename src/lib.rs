use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use worker::*;

// =============================================================================
// Constants
// =============================================================================

const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const UNSUBSCRIBE: u8 = 10;
const UNSUBACK: u8 = 11;
const PINGREQ: u8 = 12;
const PINGRESP: u8 = 13;
const DISCONNECT: u8 = 14;

const CONN_ACCEPTED: u8 = 0;
const CONN_REFUSED_PROTOCOL: u8 = 1;
const CONN_REFUSED_IDENTIFIER: u8 = 2;

const MAX_TOPIC_LEN: usize = 65535;
const MAX_CLIENT_ID_LEN: usize = 65535;

const DEFAULT_KEEP_ALIVE_MS: u64 = 60_000;
const CLEANUP_INTERVAL_MS: u64 = 30_000;
const KEEP_ALIVE_MULTIPLIER: f64 = 1.5;

const MAX_SESSIONS_PER_SHARD: usize = 10_000;

// =============================================================================
// Data Structures
// =============================================================================

#[derive(Clone, Debug)]
struct ClientSession {
    client_id: String,
    ws: Option<WebSocket>,
    subscriptions: HashSet<String>,
    last_activity: u64,
    keep_alive_ms: u64,
    clean_session: bool,
    protocol_version: u8,
    rx_buffer: Vec<u8>, // IMPORTANT: buffer to reassemble MQTT frames
}

impl ClientSession {
    fn new(client_id: String) -> Self {
        Self {
            client_id,
            ws: None,
            subscriptions: HashSet::new(),
            last_activity: Date::now().as_millis(),
            keep_alive_ms: DEFAULT_KEEP_ALIVE_MS,
            clean_session: true,
            protocol_version: 4,
            rx_buffer: Vec::new(),
        }
    }
}

#[derive(Default)]
struct ShardMetrics {
    total_messages_in: usize,
    total_messages_out: usize,
    total_connections: usize,
    total_subscriptions: usize,
    total_errors: usize,
    bytes_received: usize,
    bytes_sent: usize,
}

#[derive(Clone, Debug)]
struct RetainedMessage {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    timestamp: u64,
}

// HTTP API Structures (optional)
#[derive(Serialize, Deserialize)]
struct DirectPublish {
    topic: String,
    payload: String, // base64
    #[serde(default)]
    qos: u8,
    #[serde(default)]
    retain: bool,
}

#[derive(Serialize)]
struct PublishResponse {
    success: bool,
    delivered: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub fn get_from_secret(env: &Env, key: &str, default: &str) -> String {
    match env.secret(key) {
        Ok(_value) => _value.to_string(),
        Err(_err) => default.to_string(),
    }
}

// =============================================================================
// MQTT Errors / Results
// =============================================================================

#[derive(Debug)]
struct MqttError {
    message: String,
}
impl From<&str> for MqttError {
    fn from(s: &str) -> Self {
        MqttError {
            message: s.to_string(),
        }
    }
}
impl From<String> for MqttError {
    fn from(s: String) -> Self {
        MqttError { message: s }
    }
}
impl std::fmt::Display for MqttError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
type MqttResult<T> = std::result::Result<T, MqttError>;

// =============================================================================
// MQTT Helpers: Remaining Length + frame extraction
// =============================================================================

fn decode_remaining_length_at(data: &[u8]) -> MqttResult<(usize, usize)> {
    if data.len() < 2 {
        return Err("Incomplete remaining length encoding".into());
    }

    let mut multiplier: usize = 1;
    let mut value: usize = 0;
    let mut pos: usize = 1;

    loop {
        if pos >= data.len() {
            return Err("Incomplete remaining length encoding".into());
        }
        let byte = data[pos];
        value += ((byte & 0x7F) as usize) * multiplier;

        if multiplier > 128 * 128 * 128 {
            return Err("Malformed remaining length".into());
        }

        multiplier *= 128;
        pos += 1;

        if (byte & 0x80) == 0 {
            break;
        }
    }
    Ok((value, pos))
}

fn remaining_length_num_bytes(data: &[u8]) -> Option<usize> {
    if data.len() < 2 {
        return None;
    }
    for i in 0..4 {
        let idx = 1 + i;
        if idx >= data.len() {
            return None;
        }
        let b = data[idx];
        if (b & 0x80) == 0 {
            return Some(i + 1);
        }
    }
    None
}

/// Total MQTT frame length (fixed header + remaining length field + body).
/// Returns Ok(None) if not enough bytes yet.
fn mqtt_frame_total_len(data: &[u8]) -> MqttResult<Option<usize>> {
    if data.is_empty() {
        return Ok(None);
    }
    if remaining_length_num_bytes(data).is_none() {
        return Ok(None);
    }
    let (remaining_len, header_len) = decode_remaining_length_at(data)?;
    Ok(Some(header_len + remaining_len))
}

fn encode_remaining_length(mut length: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4);
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        bytes.push(byte);
        if length == 0 {
            break;
        }
    }
    bytes
}

fn read_u16(data: &[u8], pos: &mut usize) -> MqttResult<u16> {
    if *pos + 2 > data.len() {
        return Err("Not enough data for u16".into());
    }
    let value = ((data[*pos] as u16) << 8) | (data[*pos + 1] as u16);
    *pos += 2;
    Ok(value)
}

fn write_u16(value: u16) -> [u8; 2] {
    [(value >> 8) as u8, (value & 0xFF) as u8]
}

fn read_string(data: &[u8], pos: &mut usize) -> MqttResult<String> {
    if *pos + 2 > data.len() {
        return Err("Not enough data for string length".into());
    }
    let len = ((data[*pos] as usize) << 8) | (data[*pos + 1] as usize);
    *pos += 2;
    if *pos + len > data.len() {
        return Err("String exceeds available data".into());
    }
    let s = String::from_utf8_lossy(&data[*pos..*pos + len]).to_string();
    *pos += len;
    Ok(s)
}

fn write_string(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let len = b.len();
    let mut out = Vec::with_capacity(2 + len);
    out.push((len >> 8) as u8);
    out.push((len & 0xFF) as u8);
    out.extend_from_slice(b);
    out
}

// =============================================================================
// Topic wildcard matching (+ and #)
// =============================================================================

fn topic_matches(filter: &str, topic: &str) -> bool {
    let fp: Vec<&str> = filter.split('/').collect();
    let tp: Vec<&str> = topic.split('/').collect();

    let mut fi = 0usize;
    let mut ti = 0usize;

    while fi < fp.len() {
        let f = fp[fi];
        if f == "#" {
            return true;
        }
        if ti >= tp.len() {
            return false;
        }
        if f == "+" {
            fi += 1;
            ti += 1;
            continue;
        }
        if f != tp[ti] {
            return false;
        }
        fi += 1;
        ti += 1;
    }
    fi == fp.len() && ti == tp.len()
}

// =============================================================================
// MQTT Packet Parsing (minimal)
// =============================================================================

#[derive(Debug)]
struct ConnectPacket {
    protocol_name: String,
    protocol_level: u8,
    clean_session: bool,
    keep_alive: u16,
    client_id: String,
}

impl ConnectPacket {
    fn parse(frame: &[u8]) -> MqttResult<Self> {
        // frame includes fixed header
        let (remaining_len, mut pos) = decode_remaining_length_at(frame)?;
        let end = pos + remaining_len;
        if frame.len() < end {
            return Err("CONNECT frame too short".into());
        }

        let protocol_name = read_string(frame, &mut pos)?;
        if protocol_name != "MQTT" && protocol_name != "MQIsdp" {
            return Err(format!("Invalid protocol name: {protocol_name}").into());
        }

        if pos >= end {
            return Err("Missing protocol level".into());
        }
        let protocol_level = frame[pos];
        pos += 1;

        if pos >= end {
            return Err("Missing connect flags".into());
        }
        let flags = frame[pos];
        pos += 1;
        let clean_session = (flags & 0x02) != 0;

        let keep_alive = read_u16(frame, &mut pos)?;
        let client_id = read_string(frame, &mut pos)?;

        Ok(Self {
            protocol_name,
            protocol_level,
            clean_session,
            keep_alive,
            client_id,
        })
    }
}

#[derive(Debug)]
struct PublishPacket {
    qos: u8,
    retain: bool,
    topic: String,
    packet_id: Option<u16>,
    payload: Vec<u8>,
}

impl PublishPacket {
    fn parse(frame: &[u8]) -> MqttResult<Self> {
        if frame.is_empty() {
            return Err("Empty PUBLISH".into());
        }
        let fixed = frame[0];
        let qos = (fixed >> 1) & 0x03;
        let retain = (fixed & 0x01) != 0;

        let (remaining_len, mut pos) = decode_remaining_length_at(frame)?;
        let end = pos + remaining_len;
        if frame.len() < end {
            return Err("PUBLISH frame too short".into());
        }

        let topic = read_string(frame, &mut pos)?;
        if topic.is_empty() || topic.len() > MAX_TOPIC_LEN {
            return Err("Invalid topic".into());
        }
        if topic.contains('+') || topic.contains('#') {
            return Err("Wildcards not allowed in publish topic".into());
        }

        let packet_id = if qos > 0 {
            Some(read_u16(frame, &mut pos)?)
        } else {
            None
        };
        let payload = if pos < end {
            frame[pos..end].to_vec()
        } else {
            Vec::new()
        };

        Ok(Self {
            qos,
            retain,
            topic,
            packet_id,
            payload,
        })
    }

    fn build(
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
        packet_id: Option<u16>,
    ) -> Vec<u8> {
        let mut remaining_len = 2 + topic.as_bytes().len() + payload.len();
        if qos > 0 {
            remaining_len += 2;
        }

        let mut out = Vec::with_capacity(1 + 4 + remaining_len);
        let mut fixed = PUBLISH << 4;
        if retain {
            fixed |= 0x01;
        }
        fixed |= (qos & 0x03) << 1;
        out.push(fixed);

        out.extend(encode_remaining_length(remaining_len));
        out.extend(write_string(topic));
        if qos > 0 {
            out.extend(write_u16(packet_id.unwrap_or(1)));
        }
        out.extend_from_slice(payload);
        out
    }
}

#[derive(Debug)]
struct SubscribePacket {
    packet_id: u16,
    topics: Vec<(String, u8)>,
}
impl SubscribePacket {
    fn parse(frame: &[u8]) -> MqttResult<Self> {
        let (remaining_len, mut pos) = decode_remaining_length_at(frame)?;
        let end = pos + remaining_len;
        if frame.len() < end {
            return Err("SUBSCRIBE too short".into());
        }

        let packet_id = read_u16(frame, &mut pos)?;
        let mut topics = Vec::new();

        while pos < end {
            let filter = read_string(frame, &mut pos)?;
            if pos >= end {
                return Err("Missing requested QoS".into());
            }
            let qos = frame[pos] & 0x03;
            pos += 1;
            topics.push((filter, qos));
        }
        if topics.is_empty() {
            return Err("No topics".into());
        }
        Ok(Self { packet_id, topics })
    }
}

#[derive(Debug)]
struct UnsubscribePacket {
    packet_id: u16,
    topics: Vec<String>,
}
impl UnsubscribePacket {
    fn parse(frame: &[u8]) -> MqttResult<Self> {
        let (remaining_len, mut pos) = decode_remaining_length_at(frame)?;
        let end = pos + remaining_len;
        if frame.len() < end {
            return Err("UNSUBSCRIBE too short".into());
        }

        let packet_id = read_u16(frame, &mut pos)?;
        let mut topics = Vec::new();

        while pos < end {
            let filter = read_string(frame, &mut pos)?;
            topics.push(filter);
        }
        if topics.is_empty() {
            return Err("No topics".into());
        }
        Ok(Self { packet_id, topics })
    }
}

// =============================================================================
// Durable Object: MQTTBroker
// =============================================================================

#[durable_object]
pub struct MQTTBroker {
    state: State,
    sessions: RefCell<HashMap<String, ClientSession>>,
    topic_subscribers: RefCell<HashMap<String, HashSet<String>>>,
    retained: RefCell<HashMap<String, RetainedMessage>>,
    metrics: RefCell<ShardMetrics>,
    mqtt_path: String,
}

impl DurableObject for MQTTBroker {
    fn new(state: State, _env: Env) -> Self {
        console_error_panic_hook::set_once();

        let mqtt_path = get_from_secret(&_env, "MQTT_PATH", "/mqtt_sec1"); //.secret_store("MISSING_SECRET").expect(msg);

        Self {
            state,
            sessions: RefCell::new(HashMap::new()),
            topic_subscribers: RefCell::new(HashMap::new()),
            retained: RefCell::new(HashMap::new()),
            metrics: RefCell::new(ShardMetrics::default()),
            mqtt_path,
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        let path = url.path();
        console_log!("[DO] fetch {} {}", req.method(), path);

        match (req.method(), path) {
            (Method::Get, mqtt_path) => self.handle_websocket_upgrade(req).await,
            (Method::Post, "/publish") => self.handle_http_publish(req).await,
            (Method::Get, "/health") => Response::ok("OK"),
            _ => Response::error("Not Found", 404),
        }
    }

    async fn websocket_message(&self, ws: WebSocket, msg: WebSocketIncomingMessage) -> Result<()> {
        let bytes = match msg {
            WebSocketIncomingMessage::Binary(b) => b,
            WebSocketIncomingMessage::String(s) => {
                console_log!("[DO] ignoring text frame (need binary MQTT): {:?}", s);
                return Ok(());
            }
        };

        self.metrics.borrow_mut().bytes_received += bytes.len();

        let sid = match self.get_client_id(&ws) {
            Some(id) => id,
            None => {
                console_log!("[DO] ws message but no attachment id");
                return Ok(());
            }
        };

        // append bytes to rx buffer
        {
            let mut sessions = self.sessions.borrow_mut();
            if let Some(sess) = sessions.get_mut(&sid) {
                sess.rx_buffer.extend_from_slice(&bytes);
                sess.last_activity = Date::now().as_millis();
            } else {
                console_log!("[DO] unknown sid: {}", sid);
                return Ok(());
            }
        }

        // parse all complete frames
        loop {
            let frame: Option<Vec<u8>> = {
                let mut sessions = self.sessions.borrow_mut();
                let sess = match sessions.get_mut(&sid) {
                    Some(s) => s,
                    None => return Ok(()),
                };

                match mqtt_frame_total_len(&sess.rx_buffer) {
                    Ok(Some(total)) => {
                        if sess.rx_buffer.len() < total {
                            None
                        } else {
                            Some(sess.rx_buffer.drain(0..total).collect())
                        }
                    }
                    Ok(None) => None,
                    Err(e) => {
                        console_log!("[DO] mqtt framing error, dropping buffer: {}", e);
                        sess.rx_buffer.clear();
                        None
                    }
                }
            };

            let Some(frame) = frame else {
                break;
            };

            if let Err(e) = self.process_mqtt_packet(&ws, &frame).await {
                console_log!("[DO] mqtt packet error: {}", e);
                self.metrics.borrow_mut().total_errors += 1;
            }
        }

        Ok(())
    }

    async fn websocket_close(
        &self,
        ws: WebSocket,
        _code: usize,
        _reason: String,
        _was_clean: bool,
    ) -> Result<()> {
        self.handle_disconnect(&ws);
        Ok(())
    }

    async fn websocket_error(&self, ws: WebSocket, error: Error) -> Result<()> {
        console_log!("[DO] websocket error: {:?}", error);
        self.metrics.borrow_mut().total_errors += 1;
        self.handle_disconnect(&ws);
        Ok(())
    }

    async fn alarm(&self) -> Result<Response> {
        self.run_cleanup().await?;
        self.schedule_next_alarm().await?;
        Response::ok("OK")
    }
}

impl MQTTBroker {
    async fn handle_websocket_upgrade(&self, req: Request) -> Result<Response> {
        if self.sessions.borrow().len() >= MAX_SESSIONS_PER_SHARD {
            return Response::error("Server at capacity", 503);
        }

        let pair = WebSocketPair::new()?;
        let server = pair.server;

        self.state.accept_web_socket(&server);

        // create a temporary id for the connection before CONNECT
        let temp_id = format!("pending_{}", Date::now().as_millis());
        server.serialize_attachment(&temp_id)?;

        let mut sess = ClientSession::new(temp_id.clone());
        sess.ws = Some(server);

        self.sessions.borrow_mut().insert(temp_id, sess);
        self.metrics.borrow_mut().total_connections += 1;

        self.schedule_next_alarm().await?;

        // Negotiate subprotocol: must be "mqtt" for MQTT-over-WebSocket
        let offered = req
            .headers()
            .get("Sec-WebSocket-Protocol")?
            .unwrap_or_default();
        let selected = offered
            .split(',')
            .map(|s| s.trim())
            .find(|p| *p == "mqtt")
            .unwrap_or("mqtt");

        let mut resp = Response::from_websocket(pair.client)?;
        resp.headers_mut().set("Sec-WebSocket-Protocol", selected)?;
        Ok(resp)
    }

    async fn process_mqtt_packet(&self, ws: &WebSocket, frame: &[u8]) -> MqttResult<()> {
        if frame.is_empty() {
            return Ok(());
        }

        let packet_type = (frame[0] >> 4) & 0x0F;

        match packet_type {
            CONNECT => self.handle_connect(ws, frame).await,
            PUBLISH => self.handle_publish(ws, frame).await,
            PUBACK => Ok(()), // simplified
            SUBSCRIBE => self.handle_subscribe(ws, frame).await,
            UNSUBSCRIBE => self.handle_unsubscribe(ws, frame).await,
            PINGREQ => self.send_pingresp(ws).await,
            DISCONNECT => {
                self.handle_disconnect(ws);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn handle_connect(&self, ws: &WebSocket, frame: &[u8]) -> MqttResult<()> {
        let connect = match ConnectPacket::parse(frame) {
            Ok(c) => c,
            Err(e) => {
                self.send_connack(ws, CONN_REFUSED_PROTOCOL, false).await?;
                return Err(e);
            }
        };

        if connect.protocol_level != 4 && connect.protocol_level != 3 {
            self.send_connack(ws, CONN_REFUSED_PROTOCOL, false).await?;
            return Ok(());
        }

        let client_id = if connect.client_id.is_empty() {
            if connect.clean_session {
                format!("auto_{}", Date::now().as_millis())
            } else {
                self.send_connack(ws, CONN_REFUSED_IDENTIFIER, false)
                    .await?;
                return Ok(());
            }
        } else {
            connect.client_id.clone()
        };

        if client_id.len() > MAX_CLIENT_ID_LEN {
            self.send_connack(ws, CONN_REFUSED_IDENTIFIER, false)
                .await?;
            return Ok(());
        }

        let temp_id = self.get_client_id(ws).unwrap_or_default();

        // compute keep-alive
        let keep_alive_ms = if connect.keep_alive > 0 {
            (connect.keep_alive as u64 * 1000) as f64 * KEEP_ALIVE_MULTIPLIER
        } else {
            DEFAULT_KEEP_ALIVE_MS as f64
        } as u64;

        // remove old temp session
        if temp_id != client_id {
            self.sessions.borrow_mut().remove(&temp_id);
        }

        // create/update session
        {
            let mut sessions = self.sessions.borrow_mut();
            let s = sessions
                .entry(client_id.clone())
                .or_insert_with(|| ClientSession::new(client_id.clone()));
            s.ws = Some(ws.clone());
            s.client_id = client_id.clone();
            s.last_activity = Date::now().as_millis();
            s.keep_alive_ms = keep_alive_ms;
            s.clean_session = connect.clean_session;
            s.protocol_version = connect.protocol_level;
        }

        ws.serialize_attachment(&client_id)
            .map_err(|e| MqttError::from(format!("attach error: {:?}", e)))?;

        self.send_connack(ws, CONN_ACCEPTED, false).await?;
        Ok(())
    }

    async fn handle_publish(&self, _ws: &WebSocket, frame: &[u8]) -> MqttResult<()> {
        let publish = PublishPacket::parse(frame)?;
        self.metrics.borrow_mut().total_messages_in += 1;

        // retained
        if publish.retain {
            if publish.payload.is_empty() {
                self.retained.borrow_mut().remove(&publish.topic);
            } else {
                self.retained.borrow_mut().insert(
                    publish.topic.clone(),
                    RetainedMessage {
                        topic: publish.topic.clone(),
                        payload: publish.payload.clone(),
                        qos: publish.qos,
                        timestamp: Date::now().as_millis(),
                    },
                );
            }
        }

        // distribute QoS0 only in this simplified broker
        let delivered = self
            .distribute_message(&publish.topic, &publish.payload)
            .await?;
        if delivered > 0 {
            self.metrics.borrow_mut().total_messages_out += delivered;
        }

        Ok(())
    }

    async fn handle_subscribe(&self, ws: &WebSocket, frame: &[u8]) -> MqttResult<()> {
        let sub = SubscribePacket::parse(frame)?;
        let client_id = self
            .get_client_id(ws)
            .ok_or_else(|| MqttError::from("No client id"))?;

        let mut granted = Vec::with_capacity(sub.topics.len());

        for (filter, req_qos) in &sub.topics {
            if filter.is_empty() || filter.len() > MAX_TOPIC_LEN {
                granted.push(0x80);
                continue;
            }

            // store subscription in session
            {
                let mut sessions = self.sessions.borrow_mut();
                if let Some(s) = sessions.get_mut(&client_id) {
                    s.subscriptions.insert(filter.clone());
                }
            }

            // store in reverse map
            self.topic_subscribers
                .borrow_mut()
                .entry(filter.clone())
                .or_insert_with(HashSet::new)
                .insert(client_id.clone());

            self.metrics.borrow_mut().total_subscriptions += 1;

            // grant requested qos but cap at 2
            granted.push((*req_qos).min(2));
        }

        self.send_suback(ws, sub.packet_id, &granted).await?;

        // send retained that match new subscriptions
        self.send_retained_for_new_subs(&client_id, &sub.topics)
            .await?;

        Ok(())
    }

    async fn handle_unsubscribe(&self, ws: &WebSocket, frame: &[u8]) -> MqttResult<()> {
        let unsub = UnsubscribePacket::parse(frame)?;
        let client_id = self
            .get_client_id(ws)
            .ok_or_else(|| MqttError::from("No client id"))?;

        for filter in &unsub.topics {
            // remove from session
            {
                let mut sessions = self.sessions.borrow_mut();
                if let Some(s) = sessions.get_mut(&client_id) {
                    s.subscriptions.remove(filter);
                }
            }

            // remove from reverse map
            let mut map = self.topic_subscribers.borrow_mut();
            if let Some(set) = map.get_mut(filter) {
                set.remove(&client_id);
                if set.is_empty() {
                    map.remove(filter);
                }
            }
        }

        self.send_unsuback(ws, unsub.packet_id).await
    }

    async fn distribute_message(&self, topic: &str, payload: &[u8]) -> MqttResult<usize> {
        let clients: Vec<String> = {
            let map = self.topic_subscribers.borrow();
            let mut out = HashSet::new();
            for (filter, subs) in map.iter() {
                if topic_matches(filter, topic) {
                    for cid in subs {
                        out.insert(cid.clone());
                    }
                }
            }
            out.into_iter().collect()
        };

        let pkt = PublishPacket::build(topic, payload, 0, false, None);

        let mut delivered = 0usize;
        for cid in clients {
            let ws_opt = self.sessions.borrow().get(&cid).and_then(|s| s.ws.clone());
            if let Some(ws) = ws_opt {
                if ws.send_with_bytes(&pkt).is_ok() {
                    delivered += 1;
                    self.metrics.borrow_mut().bytes_sent += pkt.len();
                }
            }
        }
        Ok(delivered)
    }

    async fn send_retained_for_new_subs(
        &self,
        client_id: &str,
        subs: &[(String, u8)],
    ) -> MqttResult<()> {
        let ws_opt = self
            .sessions
            .borrow()
            .get(client_id)
            .and_then(|s| s.ws.clone());
        let Some(ws) = ws_opt else {
            return Ok(());
        };

        let retained_msgs: Vec<RetainedMessage> = {
            let retained = self.retained.borrow();
            let mut out = Vec::new();
            for (filter, _) in subs {
                for m in retained.values() {
                    if topic_matches(filter, &m.topic) {
                        out.push(m.clone());
                    }
                }
            }
            out
        };

        for m in retained_msgs {
            let pkt = PublishPacket::build(&m.topic, &m.payload, 0, true, None);
            let _ = ws.send_with_bytes(&pkt);
            self.metrics.borrow_mut().bytes_sent += pkt.len();
        }

        Ok(())
    }

    async fn send_connack(
        &self,
        ws: &WebSocket,
        code: u8,
        session_present: bool,
    ) -> MqttResult<()> {
        let flags = if session_present { 0x01 } else { 0x00 };
        let pkt = [CONNACK << 4, 2, flags, code];
        ws.send_with_bytes(&pkt)
            .map_err(|e| MqttError::from(format!("{:?}", e)))?;
        self.metrics.borrow_mut().bytes_sent += pkt.len();
        Ok(())
    }

    async fn send_suback(&self, ws: &WebSocket, packet_id: u16, granted: &[u8]) -> MqttResult<()> {
        let remaining_len = 2 + granted.len();
        let mut pkt = vec![SUBACK << 4];
        pkt.extend(encode_remaining_length(remaining_len));
        pkt.extend(write_u16(packet_id));
        pkt.extend_from_slice(granted);
        ws.send_with_bytes(&pkt)
            .map_err(|e| MqttError::from(format!("{:?}", e)))?;
        self.metrics.borrow_mut().bytes_sent += pkt.len();
        Ok(())
    }

    async fn send_unsuback(&self, ws: &WebSocket, packet_id: u16) -> MqttResult<()> {
        let id = write_u16(packet_id);
        let pkt = [UNSUBACK << 4, 2, id[0], id[1]];
        ws.send_with_bytes(&pkt)
            .map_err(|e| MqttError::from(format!("{:?}", e)))?;
        self.metrics.borrow_mut().bytes_sent += pkt.len();
        Ok(())
    }

    async fn send_pingresp(&self, ws: &WebSocket) -> MqttResult<()> {
        let pkt = [PINGRESP << 4, 0];
        ws.send_with_bytes(&pkt)
            .map_err(|e| MqttError::from(format!("{:?}", e)))?;
        self.metrics.borrow_mut().bytes_sent += pkt.len();
        Ok(())
    }

    fn get_client_id(&self, ws: &WebSocket) -> Option<String> {
        ws.deserialize_attachment::<String>().ok().flatten()
    }

    fn handle_disconnect(&self, ws: &WebSocket) {
        if let Some(cid) = self.get_client_id(ws) {
            let clean = self
                .sessions
                .borrow()
                .get(&cid)
                .map(|s| s.clean_session)
                .unwrap_or(true);
            if clean {
                self.sessions.borrow_mut().remove(&cid);
            } else if let Some(s) = self.sessions.borrow_mut().get_mut(&cid) {
                s.ws = None;
            }
        }
        let _ = ws.close(Some(1000), Some("Disconnected"));
    }

    async fn run_cleanup(&self) -> Result<()> {
        let now = Date::now().as_millis();
        let stale: Vec<String> = self
            .sessions
            .borrow()
            .iter()
            .filter(|(_, s)| now > s.last_activity + s.keep_alive_ms)
            .map(|(id, _)| id.clone())
            .collect();

        for id in stale {
            self.sessions.borrow_mut().remove(&id);
        }
        Ok(())
    }

    async fn schedule_next_alarm(&self) -> Result<()> {
        if !self.sessions.borrow().is_empty() {
            let next = Date::now().as_millis() as i64 + CLEANUP_INTERVAL_MS as i64;
            self.state.storage().set_alarm(next).await?;
        }
        Ok(())
    }

    async fn handle_http_publish(&self, mut req: Request) -> Result<Response> {
        let body: DirectPublish = match req.json().await {
            Ok(b) => b,
            Err(e) => {
                return Response::from_json(&PublishResponse {
                    success: false,
                    delivered: 0,
                    error: Some(format!("Invalid JSON: {:?}", e)),
                });
            }
        };

        let payload = match base64_decode(&body.payload) {
            Ok(p) => p,
            Err(e) => {
                return Response::from_json(&PublishResponse {
                    success: false,
                    delivered: 0,
                    error: Some(format!("Invalid base64: {e}")),
                });
            }
        };

        if body.retain {
            if payload.is_empty() {
                self.retained.borrow_mut().remove(&body.topic);
            } else {
                self.retained.borrow_mut().insert(
                    body.topic.clone(),
                    RetainedMessage {
                        topic: body.topic.clone(),
                        payload: payload.clone(),
                        qos: body.qos,
                        timestamp: Date::now().as_millis(),
                    },
                );
            }
        }

        let delivered = self
            .distribute_message(&body.topic, &payload)
            .await
            .unwrap_or(0);
        self.metrics.borrow_mut().total_messages_in += 1;

        Response::from_json(&PublishResponse {
            success: true,
            delivered,
            error: None,
        })
    }
}

// =============================================================================
// Worker entrypoint: route everything to DO
// =============================================================================

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();

    let url = req.url()?;
    let path = url.path();
    if path == "/health" {
        return Response::ok("MQTT Broker Running");
    }

    let mqtt_path = get_from_secret(&env, "MQTT_PATH", "/mqtt_sec1"); //.secret_store("MISSING_SECRET").expect(msg);

    let offered = req
        .headers()
        .get("Sec-WebSocket-Protocol")?
        .unwrap_or_default();
    let UpgradeHeader = req.headers().get("Upgrade")?.unwrap_or_default();

    let selected = offered
        .split(',')
        .map(|s| s.trim())
        .find(|p| *p == "mqtt")
        .unwrap_or("mqtt");

    console_log!(
        "Fetch : selected: {:?}, UpgradeHeader: {:?}",
        selected,
        UpgradeHeader
    );

    if path == mqtt_path && UpgradeHeader == "websocket" {
        // shard id
        let shard_id = url
            .query_pairs()
            .find(|(k, _)| k == "shard")
            .map(|(_, v)| v.to_string())
            .unwrap_or_else(|| "default".to_string());

        // ALWAYS use one shard for now:
        let shard_id = "default";

        let ns = env.durable_object("MQTT_BROKER")?;
        let id = ns.id_from_name(&shard_id)?;
        let stub = id.get_stub()?;

        stub.fetch_with_request(req).await
    } else {
        Response::error("Not Found", 404)
    }
}

#[event(start)]
fn start() {
    console_error_panic_hook::set_once();
}

// =============================================================================
// Simple base64 decode (no deps)
// =============================================================================

fn base64_decode(input: &str) -> std::result::Result<Vec<u8>, String> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    fn val(c: u8) -> std::result::Result<u8, String> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            b'=' => Ok(0),
            _ => Err(format!("Invalid base64 char: {}", c as char)),
        }
    }

    let s = input.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }

    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut i = 0;

    while i < bytes.len() {
        let mut chunk = [0u8; 4];
        let mut valid = 0;

        for j in 0..4 {
            if i + j < bytes.len() && bytes[i + j] != b'=' {
                chunk[j] = val(bytes[i + j])?;
                valid += 1;
            }
        }

        if valid >= 2 {
            out.push((chunk[0] << 2) | (chunk[1] >> 4));
        }
        if valid >= 3 {
            out.push((chunk[1] << 4) | (chunk[2] >> 2));
        }
        if valid >= 4 {
            out.push((chunk[2] << 6) | chunk[3]);
        }

        i += 4;
    }

    Ok(out)
}
