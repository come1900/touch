# Touch - WebSocket Client/Server Tools

## Description

Touch is a comprehensive WebSocket client and server implementation toolkit, providing two executable programs for WebSocket communication testing and development. The toolkit includes a lightweight native WebSocket client implementation and a high-performance libwebsockets-based server, supporting real-time bidirectional communication, automatic reconnection, message broadcasting, and interactive console interfaces for testing and debugging WebSocket applications.

## Quick Start

### Requirements

- **Linux environment** with GCC compiler
- **libezutil** library (must be compiled and installed first)
  ```bash
  cd ../libezutil
  make && make install
  ```
- **libwebsockets** library (required only for server)
  - Pre-installed libwebsockets library
  - Library location: `$(HOME)/libs/libwebsockets-$(PLATFORM)/lib/libwebsockets.a`
  - Include location: `$(HOME)/libs/libwebsockets-$(PLATFORM)/include/`

### Installation

```bash
cd touch
make
```

This will generate:
- `touch-cli` - WebSocket client executable
- `touch-svr` - WebSocket server executable

### Running Tests

```bash
# Start server in background
./touch-svr -p 8080 &

# Start client and connect to server
./touch-cli -h localhost -p 8080

# Send test messages
# Server console commands: clients, send <id> <msg>, status, quit
# Client console commands: status, help, quit, or any message to send
```

## Usage

### touch-svr (Server)

Start the WebSocket server with default configuration:

```bash
./touch-svr
```

Available command-line options:
- `-p PORT` - Server port (default: 54321)
- `--protocol PROTOCOL` - WebSocket subprotocol (default: come.0)
- `-d LEVEL` - Log level (default: 7)

Server console commands after startup:
- `clients` - Show connected clients list
- `send <id> <msg>` - Send message to specific client
- `status` - Show server status
- `help` - Show help information
- `quit/exit` - Shutdown server
- Other input - Broadcast message to all clients

### touch-cli (Client)

Connect to WebSocket server:

```bash
./touch-cli -h localhost -p 8080
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

## Architecture

### touch-cli (Client)
- **Native Linux implementation** using epoll/timerfd/socket
- **Zero external dependencies** (except libezutil)
- **Automatic reconnection** with exponential backoff
- **Heartbeat keep-alive** functionality
- **Interactive console** interface

### touch-svr (Server)
- **libwebsockets-based** high-performance implementation
- **Multi-client management** with connection tracking
- **Broadcast and unicast messaging** support
- **Client list management** and monitoring
- **Interactive console** interface for server control

## License
MIT License

