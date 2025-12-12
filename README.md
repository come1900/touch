# Touch - WebSocket 客户端/服务端工具

## 项目概述

Touch 是一个 WebSocket 客户端和服务端实现工具集，包含两个可执行程序：

- **touch-cli**: WebSocket 客户端，使用原生实现（基于 epoll/timerfd/socket），不依赖 libwebsockets
- **touch-svr**: WebSocket 服务端，基于 libwebsockets 库实现

## 功能特性

### touch-cli（客户端）
- 原生 Linux 实现，轻量级，无外部依赖（除 libezutil）
- 自动重连机制，支持指数退避策略
- 心跳保活功能
- 控制台交互界面
- 支持文本和二进制消息

### touch-svr（服务端）
- 基于 libwebsockets 的高性能实现
- 多客户端管理
- 支持广播和单播消息
- 控制台交互界面
- 客户端列表查看和管理

## 编译

### 前置要求

1. **libezutil** 库已编译并安装
   ```bash
   cd libezutil
   make && make install
   ```

2. **libwebsockets** 库（仅服务端需要）
   - 需要预先安装 libwebsockets 库
   - 库文件位置：`$(HOME)/libs/libwebsockets-$(PLATFORM)/lib/libwebsockets.a`
   - 头文件位置：`$(HOME)/libs/libwebsockets-$(PLATFORM)/include/`

### 编译步骤

```bash
cd touch
make
```

编译成功后生成：
- `touch-cli` - WebSocket 客户端可执行文件
- `touch-svr` - WebSocket 服务端可执行文件

### 清理

```bash
make clean
```

## 使用方法

### touch-svr（服务端）

#### 启动服务端

```bash
# 使用默认配置（端口 54321，协议 come.0）
./touch-svr

# 指定端口
./touch-svr -p 8080

# 指定协议名称
./touch-svr --protocol "my-protocol" -p 8080

# 设置日志级别
./touch-svr -d 31
```

#### 服务端控制台命令

服务端启动后，可以在控制台输入以下命令：

- `clients` - 显示已连接的客户端列表
- `send <id> <msg>` - 向指定客户端发送消息（id 为客户端ID）
- `status` - 显示服务器状态
- `help` - 显示帮助信息
- `quit` 或 `exit` - 退出服务器
- 其他输入 - 广播消息给所有客户端

**示例：**
```bash
srv> clients
  ID  IP              Port    Connect Time
   1  192.168.1.100   54321   2024-01-01 10:00:00

srv> send 1 Hello
Message sent to client 1

srv> Broadcast message
Message broadcasted to all clients
```

### touch-cli（客户端）

#### 启动客户端

```bash
# 连接到默认服务器（localhost:54321）
./touch-cli

# 指定服务器地址和端口
./touch-cli -h 192.168.1.100 -p 8080
# 或使用 -s 参数（等同于 -h）
./touch-cli -s 192.168.1.100 -p 8080

# 指定 URL 路径
./touch-cli -u /ws/echo

# 指定协议名称
./touch-cli --protocol "my-protocol" -h localhost -p 8080
```

#### 命令行参数

| 参数 | 说明 | 默认值 | 示例 |
|------|------|--------|------|
| `-h, --host HOST` | 服务器地址 | `localhost` | `-h 192.168.1.100` |
| `-s, --server HOST` | 服务器地址（等同于 -h） | `localhost` | `-s 192.168.1.100` |
| `-p, --port PORT` | 服务器端口 | `54321` | `-p 8080` |
| `-u, --url PATH` | WebSocket URL 路径 | `/come` | `-u /ws/echo` |
| `--protocol PROTOCOL` | WebSocket 子协议 | `come.0` | `--protocol my-protocol` |

#### 客户端控制台命令

客户端连接成功后，可以在控制台输入：

- `status` - 显示 WebSocket 连接状态
- `help` - 显示帮助信息
- `quit` - 退出程序
- 其他输入 - 发送消息到服务器

**示例：**
```bash
ws> status
Connected to localhost:54321/come
Protocol: come.0

ws> Hello, Server!
Message sent

ws> quit
```

## 使用示例

### 基本使用流程

1. **启动服务端**
   ```bash
   $ ./touch-svr -p 8080
   Server listening on port 8080
   srv> 
   ```

2. **启动客户端**
   ```bash
   $ ./touch-cli -h localhost -p 8080
   Connecting to localhost:8080/come
   === Connection established ===
   ws> 
   ```

3. **发送消息**
   - 客户端：直接输入消息并按回车发送
   - 服务端：输入消息广播给所有客户端，或使用 `send <id> <msg>` 发送给指定客户端

## 默认配置

### 客户端默认值
- 服务器地址：`localhost`
- 端口：`54321`
- URL 路径：`/come`
- 协议：`come.0`
- 重连间隔：500ms
- 最大重连次数：0（无限重连）

### 服务端默认值
- 监听端口：`54321`
- 协议：`come.0`
- URL 路径：`/come`

## 依赖说明

### touch-cli 依赖
- `libezutil` - 基础工具库（包含 ez_websocket_parser）

### touch-svr 依赖
- `libezutil` - 基础工具库
- `libwebsockets` - WebSocket 服务端库

## 注意事项

1. 编译前确保已安装 `libezutil` 库（执行 `make install`）
2. 服务端需要预先安装 `libwebsockets` 库
3. 客户端和服务端需要使用相同的协议名称才能正常通信
4. 使用 Ctrl+C 可以安全退出程序
