# Touch - WebSocket Client/Server Tools

## Description

Touch is a comprehensive WebSocket client and server implementation toolkit, providing three executable programs for WebSocket communication testing and development. The toolkit includes:
- A lightweight native WebSocket client implementation (zero external dependencies except libezutil)
- A high-performance libwebsockets-based server implementation
- A high-performance native server implementation using epoll (no libwebsockets dependency)

All implementations support real-time bidirectional communication, automatic reconnection, message broadcasting, server-side ping/pong keepalive, and interactive console interfaces for testing and debugging WebSocket applications.

## Quick Start

### Requirements

- **Linux environment** with GCC compiler
- **libezutil** library (must be compiled and installed first)
  ```bash
  cd ../libezutil
  make && make install
  ```
- **libwebsockets** library (required only for `touch-svr-libwebsocket`)
  - Pre-installed libwebsockets library
  - Library location: `$(HOME)/libs/libwebsockets-$(PLATFORM)/lib/libwebsockets.a`
  - Include location: `$(HOME)/libs/libwebsockets-$(PLATFORM)/include/`
  - Note: `touch-svr-native` does not require libwebsockets

### Installation

```bash
cd touch
make
```

This will generate (with PLATFORM suffix, e.g., `linux`, `linux-mipsel-openwrt-linux`):
- `touch-cli-native-$(PLATFORM)` - WebSocket client executable (native implementation)
- `touch-svr-libwebsocket-$(PLATFORM)` - WebSocket server executable (libwebsockets-based)
- `touch-svr-native-$(PLATFORM)` - WebSocket server executable (native implementation, no libwebsockets dependency)

For convenience, you can create symbolic links:
```bash
ln -s touch-cli-native-$(PLATFORM) touch-cli
ln -s touch-svr-libwebsocket-$(PLATFORM) touch-svr
```

### Running Tests

```bash
# Start server in background (using libwebsockets version)
./touch-svr-libwebsocket-$(PLATFORM) -p 8080 &

# Or use native server (no libwebsockets dependency)
# ./touch-svr-native-$(PLATFORM) -p 8080 &

# Start client and connect to server
./touch-cli-native-$(PLATFORM) -h localhost -p 8080

# Send test messages
# Server console commands: clients, send <id> <msg>, status, help, quit/exit
# Client console commands: status, help, quit, or any message to send
```

## Usage

### touch-svr-libwebsocket (Server - libwebsockets-based)

Start the WebSocket server with default configuration:

```bash
./touch-svr-libwebsocket-$(PLATFORM)
```

Available command-line options:
- `-p PORT` - Server port (default: 54321)
- `--protocol PROTOCOL` - WebSocket subprotocol (default: come.0)
- `-d LEVEL` - Log level (default: 7)
- `-o` - Once mode (optional)

Server console commands after startup:
- `clients` - Show connected clients list
- `send <id> <msg>` - Send message to specific client by ID
- `status` - Show server status
- `help` - Show help information
- `quit/exit` - Shutdown server
- Other input - Broadcast message to all clients

### touch-svr-native (Server - Native Implementation)

Alternative server implementation without libwebsockets dependency:

```bash
./touch-svr-native-$(PLATFORM) -p 8080
```

This version has the same console commands as the libwebsockets version but does not require the libwebsockets library.

### touch-cli-native (Client)

Connect to WebSocket server:

```bash
./touch-cli-native-$(PLATFORM) -h localhost -p 8080
```

Available command-line options:
- `-h, --host HOST` - Server hostname (default: localhost)
- `-s, --server HOST` - Alias for --host
- `-p, --port PORT` - Server port (default: 54321)
- `-u, --url PATH` - WebSocket URL path (default: /come)
- `--protocol PROTOCOL` - WebSocket subprotocol (default: come.0)

Client console commands after connection:
- `status` - Show connection status
- `help` - Show help information
- `quit` - Exit program
- Other input - Send message to server

### Programmatic Usage

Include the corresponding header files in your projects:

For client (native implementation):
```c
#include "ez_wsclient-native.h"
```

For server (libwebsockets-based):
```c
#include "ez_wsserver-libwebsocket.h"
```

For server (native implementation, no libwebsockets dependency):
```c
#include "ez_wsserver-native.h"
```

## Architecture

### touch-cli-native (Client)
- **Native Linux implementation** using epoll/timerfd/socket
- **Zero external dependencies** (except libezutil)
- **Automatic reconnection** with exponential backoff
- **Heartbeat keep-alive** functionality
- **Interactive console** interface

### touch-svr-libwebsocket (Server - libwebsockets-based)
- **libwebsockets-based** high-performance implementation
- **Multi-client management** with connection tracking
- **Broadcast and unicast messaging** support
- **Client list management** and monitoring
- **Interactive console** interface for server control
- **Server-side ping/pong** keepalive with configurable intervals and jitter
- **Idle timeout** for connection cleanup

### touch-svr-native (Server - Native Implementation)
- **High-performance native Linux implementation** using epoll/timerfd/socket
- **Zero external dependencies** (except libezutil, no libwebsockets required)
- **Multi-client management** with connection tracking
- **Broadcast and unicast messaging** support
- **Client list management** and monitoring
- **Interactive console** interface for server control
- **Server-side ping/pong** keepalive with configurable intervals and jitter
- **Idle timeout** for connection cleanup

## License
MIT License

