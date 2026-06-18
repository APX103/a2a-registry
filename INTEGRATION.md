# 接入 Agent Registry 集群

Registry 是一个**服务发现的"通讯录"**：它记录每个 agent 的地址，并实时探活，
只把**当前能连通**的 agent 返回给调用方。Agent 之间不通过 Registry 转发消息——
拿到地址后**直接点对点（P2P）通信**（走 A2A 协议）。

```
   ┌─────────────┐   注册/探活   ┌──────────────┐
   │   Agent A   │ ────────────► │   Registry   │
   │ (A2A server)│ ◄──────────── │  (通讯录+探活) │
   └─────────────┘   发现(健康列表) └──────────────┘
          │                                ▲
          │  拿到 B 的 url 后, 直接 A2A 调用  │  发现
          ▼                                │
   ┌─────────────┐                    ┌─────────────┐
   │   Agent B   │ ◄──────────────────│   Agent C   │
   │ (A2A server)│                    │ (A2A server)│
   └─────────────┘                    └─────────────┘
```

## 前提：网络可达

本集群假设所有 agent **部署在同一个网络**下（例如同一个 docker network、同一内网、
或互相能路由的公网）。Registry 发现的 URL 必须是**调用方能访问到的地址**：

- docker compose 内：用 service name（如 `http://comedian:8003`）
- 跨机器：用公网 IP / 域名 / 反向代理地址（**不能写 localhost**，因为别人的容器解析不了）

## ⚡ 30 秒接入（极简版）

如果你的 agent 框架支持 MCP（现在主流框架都支持），**只需要一个配置**——
填一段 JSON 指向 Registry 的 MCP 地址，你的 agent 就自动获得「注册自己」+
「发现别人」两个工具，LLM 会自己调用：

```json
{
  "mcpServers": {
    "registry": {
      "url": "http://<registry地址>:8006/sse",
      "transport": "sse"
    }
  }
}
```

把这段配置加进你的 agent 配置（和接其他 MCP Server 一样），就完成了。
连上后你的 LLM 立刻拥有 `register_agent` 和 `list_agents` 两个工具：
- 告诉它「注册你自己到集群」→ 它调 `register_agent(name, url, description)`
- 告诉它「集群里有哪些 agent」→ 它调 `list_agents()` 拿到健康列表

> 你只需要知道**一个地址**（Registry 的 MCP SSE URL），连上即入网。
> 这和接 GLM、Cursor、Claude 的 MCP 是完全一样的体验。

下面是完整版（含 A2A 协议细节、手写实现、流式说明）。

## 两种接入需求

| 你的需求 | 你要做的事 |
|---------|-----------|
| **A. 我只想被别人发现**（最低要求） | 实现 A2A server + 注册自己 |
| **B. 我想调集群里的别人** | 在 A 的基础上，发现别人 + 发 A2A 消息 |

---

## 场景 A：被别人发现（最低要求）

### A1. 实现 A2A server（暴露 agent card + message/send）

你的 agent 必须响应两个东西：

1. **`GET /.well-known/agent-card.json`** — 返回你的名片（Registry 探活也打这个端点）
2. **`POST /`（或 `/jsonrpc`）`message/send`** — 接收别人的消息并返回结果

#### 用 ADK（Python，最简单，3 行）

```python
from google.adk.agents import Agent
from google.adk.a2a.utils.agent_to_a2a import AgentCardBuilder, to_a2a
import asyncio

my_agent = Agent(
    name="weather_bot",
    model="gemini-2.0-flash",          # 或用 LiteLlm 接其他模型
    description="查询天气的 agent",       # ★ 这段会成为别人发现你时的工具描述
    instruction="你是天气助手，回答天气问题。",
)

card = asyncio.run(AgentCardBuilder(
    agent=my_agent, rpc_url="http://我的地址:我的端口"
).build())
app = to_a2a(my_agent, host="0.0.0.0", port=9000, protocol="http", agent_card=card)
```

`to_a2a()` 会自动：暴露 `/.well-known/agent-card.json` + 处理 `message/send`。你的 agent
就具备了被调用的一切。

#### 用 ADK（Node/TypeScript）

```typescript
import { toA2a } from "@google/adk";
// agent 定义略
await toA2a(agent, { app, host: "0.0.0.0", port: 9000, protocol: "http" });
```

#### 手写最小 A2A server（任何语言）

如果不想用 ADK，手写一个 JSON-RPC handler 即可。核心是两个端点：

```python
# 伪代码 / Python 示例
from fastapi import FastAPI
from fastapi.responses import JSONResponse
app = FastAPI()

# 1. 名片（必须，Registry 探活打这个）
@app.get("/.well-known/agent-card.json")
def card():
    return {
        "name": "weather_bot",
        "description": "查询天气的 agent",
        "url": "http://我的地址:9000",
        "version": "1.0.0",
        "capabilities": {},          # streaming: true 可选，见下文"流式"
        "skills": [],                # 可选，当前没人读
    }

# 2. 消息处理（message/send）
@app.post("/")
def handle(message: dict):
    # message 是 JSON-RPC: {"jsonrpc":"2.0","method":"message/send","params":{...}}
    # 解析 params.message.parts[].text 拿到用户输入
    user_text = message["params"]["message"]["parts"][0]["text"]
    reply = my_logic(user_text)      # 你的业务逻辑
    return {
        "jsonrpc": "2.0",
        "id": message["id"],
        "result": {
            "messageId": "...",
            "role": "agent",
            "parts": [{"kind": "text", "text": reply}],
        },
    }
```

> 注意：调用方会先试 `<url>/jsonrpc` 再试 `<url>/`（main_agent 的探测顺序），或反过来
>（eino_agent 的顺序）。两个路径都接、或用 ADK 的 `to_a2a()`（默认挂在 `/`）最省心。

### A2. 注册自己

启动你的 A2A server 后，向 Registry 登记（二选一）：

**方式一：REST（适合脚本 / 运维 / 一次性注册）**
```bash
curl -X POST http://<registry>:8006/agents \
  -H "Content-Type: application/json" \
  -d '{
    "name": "weather_bot",
    "url": "http://我的地址:9000",
    "description": "查询天气的 agent",
    "type": "specialist"
  }'
```

**方式二：MCP 工具（适合 agent 自己注册，LLM 自动调）**

如果你的 agent 框架支持 MCP（主流框架都支持），连上 Registry 的 MCP endpoint，
你的 LLM 就自动获得 `register_agent` 工具：
```
MCP endpoint: http://<registry>:8006/sse
```
连上后，告诉 LLM「注册你自己到集群」，它会调 `register_agent(name, url, description)`。

### A3. 完成

注册后，Registry 会立即探活你的 `/.well-known/agent-card.json`。能连通就算注册成功。
此后你的 agent 出现在所有人的 `list_agents` / `GET /agents` 结果里，别人会直接 A2A 调你。

**探活机制**：Registry 每 60 秒探活一次。如果你临时下线，会从发现列表里**自动消失**
（但记录保留，不删除）；你恢复后**自动重新出现**。无需通知任何人。

---

## 场景 B：调用集群里的别人

### B1. 先完成场景 A（被发现）

你的 agent 必须先是个合格的 A2A server（见上）。

### B2. 发现别人

**用 MCP 工具（推荐）**：连上 Registry 的 MCP，获得 `list_agents` 工具：
```
MCP endpoint: http://<registry>:8006/sse
```
LLM 调 `list_agents()` 拿到所有**当前健康**的 agent（含 name / url / description）。

**用 REST**：
```bash
curl http://<registry>:8006/agents
# → {"agents":[{"name":"...","url":"...","description":"...","type":"..."}, ...]}
```

### B3. 直接调用别人（P2P，不经过 Registry）

拿到别人的 `url` 后，**直接向他发 A2A 消息**。Registry 不参与转发。

**用 ADK（Python，推荐）**：把别的 agent 包成 `AgentTool`，LLM 自动路由调用：
```python
from google.adk.agents.remote_a2a_agent import RemoteA2aAgent
from google.adk.tools.agent_tool import AgentTool

# target_url 是从 list_agents 拿到的别人 url
remote = RemoteA2aAgent(
    name="critic_agent",                    # 别人的 name
    agent_card="http://critic:8004",        # 别人的 url（★ 传 url，ADK 自动 fetch card）
    description="评论员，会给内容打分",        # ★ 影响你的 LLM 是否调它
)
tool = AgentTool(agent=remote)
# 把 tool 加进你的 Agent(tools=[...])，LLM 会按 description 自动决定何时调
```

**手写 JSON-RPC（任何语言）**：
```python
import requests, uuid
def call_agent(base_url: str, message: str) -> str:
    payload = {
        "jsonrpc": "2.0", "id": str(uuid.uuid4()),
        "method": "message/send",
        "params": {"message": {
            "messageId": str(uuid.uuid4()), "role": "user",
            "parts": [{"kind": "text", "text": message}],
        }},
    }
    # 调用方会先试 /jsonrpc 再试 /，或反过来；两个都试最稳
    for path in ("/", "/jsonrpc"):
        r = requests.post(base_url.rstrip("/") + path, json=payload, timeout=600)
        data = r.json()
        if "result" in data or "error" in data:
            result = data["result"]
            # 返回格式兼容三种：result.parts / result.message.parts / result.artifacts[].parts
            for p in result.get("parts", result.get("message", {}).get("parts", [])):
                if "text" in p: return p["text"]
    raise RuntimeError("no valid response")
```

---

## 流式输出

A2A 协议支持流式：在你的 agent card 里声明 `"capabilities": {"streaming": true}`，
并实现 `message/stream`（SSE）方法。

**但注意**：当前集群内的 agent 间调用**全部是非流式**（`message/send`）。
声明 `streaming: true` 不会影响你能否被调用——调用方目前都走 `message/send`，
等完整结果。如果你的 chatbot 想暴露流式能力给外部用户（非 agent 间调用），
可以自行实现 `message/stream`，但这不属于集群内部通信。

---

## Registry API 速查

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/agents` | 所有**健康** agent（探活通过的） |
| GET | `/agents/{name}` | 单个健康 agent；不健康返回 404 |
| POST | `/agents` | 注册（name+url 联合唯一，重复 409） |
| PUT | `/agents/{name}` | 更新 url/description/type |
| DELETE | `/agents/{name}` | 注销 |
| GET | `/health` | `{agents_count, agents_healthy}` |
| POST | `/reload` | 立即触发一次探活 |
| MCP | `/sse` | MCP endpoint（`register_agent` / `list_agents` 工具） |

### 探活与去重规则
- **探活**：每 60s GET 每个 agent 的 `/.well-known/agent-card.json`，2xx = 健康。
  不健康的从 `GET /agents` / `list_agents` 里**临时剔除**（记录保留），恢复后自动回来。
- **去重**：`name + url` 联合唯一。允许同名不同 URL（多实例），禁止完全重复（409）。
- **注册乐观**：注册时默认标记健康，下个探活周期校正。

---

## 最短接入示例（Python + ADK）

```python
# my_agent.py —— 一个完整的可被发现 + 能调别人的 agent
import asyncio
from google.adk.agents import Agent
from google.adk.agents.remote_a2a_agent import RemoteA2aAgent
from google.adk.tools.agent_tool import AgentTool
from google.adk.a2a.utils.agent_to_a2a import AgentCardBuilder, to_a2a

REGISTRY = "http://agent_registry:8006"

# 1. 从 Registry 发现别人（用 REST 拉列表，或配 MCP_SERVERS 让 LLM 自己拉）
import requests
peers = requests.get(f"{REGISTRY}/agents", timeout=10).json()["agents"]
tools = []
for p in peers:
    if p["name"] == "my_agent": continue  # 别调自己
    tools.append(AgentTool(agent=RemoteA2aAgent(
        name=p["name"], agent_card=p["url"], description=p["description"])))

# 2. 定义你自己
my_agent = Agent(
    name="my_agent", model="gemini-2.0-flash",
    description="一句话说清你干什么",   # ★ 别人靠这段决定是否调你
    instruction="...", tools=tools,
)

# 3. 暴露为 A2A server（被发现的前提）
card = asyncio.run(AgentCardBuilder(agent=my_agent, rpc_url="http://my_agent:9000").build())
app = to_a2a(my_agent, host="0.0.0.0", port=9000, protocol="http", agent_card=card)

# 4. 注册自己（一次性，或配 MCP 让 LLM 注册）
requests.post(f"{REGISTRY}/agents", json={
    "name": "my_agent", "url": "http://my_agent:9000",
    "description": "一句话说清你干什么", "type": "specialist"})
```

放到 docker compose 里，加入同一个 network，就接入集群了。
