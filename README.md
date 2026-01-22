# cf-mqtt-broker
Minimalist MQTT Broker powered by cloudflare DO&amp;Websocket. 



## Install
```
npm i
```



## 🧪 Local Development

1. **Create `.dev.vars`**
  ```
  MQTT_PATH="/mqtt_sec1"
  ```
2. **Start Server**
  ```bash
  npx wrangler dev -c ./wrangler.workers.toml
  ```

3. **Rust**

```bash
rustup default stable
```

```bash
cargo clean
rm -r .cargo
```

```bash
worker-build --release
```
```bash
npx wrangler dev -c ./wrangler.workers.rust.toml
```


## 🚀 Deployment

1.  **Set the API Key (Recommended):**

    - Workers
    ```bash
    npx wrangler secret put MQTT_PATH -c ./wrangler.workers.toml
    # Enter your desired password when prompted
    ```
    - Pages
    ```bash
    npx wrangler pages secret put MQTT_PATH
    ```

3.  **Deploy:**
    - Workers
    
    ```bash
    npx wrangler deploy -c ./wrangler.workers.toml
    ```
    - Pages

    ```bash
    npx wrangler pages deploy 
    ```

#### Start client

```bash
node ./client.js
```

## Reference
- https://developers.cloudflare.com/network/websockets/
- https://developers.cloudflare.com/durable-objects/best-practices/websockets/#websocket-hibernation-api