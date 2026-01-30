# Touch - Device Management Framework

## Description

Touch is a device management framework that provides WebSocket-based communication for device registration and remote management. The framework includes:

- **WebSocket Communication Layer**: Reliable WebSocket client/server implementation
- **Device Registration**: Automatic device registration functionality
- **Client/Server Applications**: Test programs for device registration workflow

The framework supports automatic reconnection, device authentication, and uses the come.1 JSON protocol for message exchange.

## Quick Start

### Requirements

- C++11 or higher compiler (GCC 4.8+ / Clang 3.3+)
- **libezThread** library (must be compiled and installed first)
- **libezsocket** library (must be compiled and installed first)
- **libezutil** library (must be compiled and installed first)
- **come.1** protocol library (located at `../come.1/`)
- **nlohmann/json** library (located at `$(HOME)/libs/json-hpp/`)

### Installation

```bash
cd touch
make
```

This will generate two executables:
- `touchCli-$(PLATFORM)` - Client program
- `touchSvr-$(PLATFORM)` - Server program

## Usage

### Running the Server

```bash
./touchSvr-$(PLATFORM) [-p|--port PORT]
```

Default port: 54321

### Running the Client

```bash
./touchCli-$(PLATFORM) [server_addr] [port] [device_id] [device_key] [device_type]
```

Example:
```bash
# Terminal 1: Start server
./touchSvr-$(PLATFORM)

# Terminal 2: Start client
./touchCli-$(PLATFORM) localhost 54321 device001 key001 touch
```

The client automatically connects to the server and sends device registration requests. The server validates device information and returns registration results.

## License

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later version.

