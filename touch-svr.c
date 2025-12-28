/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * touch-svr.c - WebSocket Server Example Program
 *
 * Copyright (C) 2011 ezlibs.com, All Rights Reserved.
 *
 * $Id: touch-svr.c 1 2011-12-27 20:00:00Z WHF $
 *
 * Explain:
 *     WebSocket server example program using ez_wsserver-libwebsocket component.
 *
 * Update:
 *     2011-12-27 20:00:00 WHF Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>

#include <libwebsockets.h>
#include <ezutil/ez_system_api.h>
#include "ez_wsserver-libwebsocket.h"

/* 控制台提示符 */
#ifndef LWS_MIN_SERVER_PROMPT
#define LWS_MIN_SERVER_PROMPT "srv> "
#endif

/* WebSocket协议和路径配置 */
#ifndef WS_SERVER_PROTOCOL
#define WS_SERVER_PROTOCOL "come.0"
#endif

#ifndef WS_SERVER_PATH_PREFIX
#define WS_SERVER_PATH_PREFIX "/come"
#endif

/* Console句柄 - 作为websocket的外部使用方 */
struct console_handle {
	pthread_t console_thread;
	int console_running;
	int *interrupted;  /* 指向全局中断标志 */
	
	/* 持有websocket句柄，用于调用websocket API */
	struct ez_ws_server_handle *ws_handle;
};

static int interrupted = 0;
static pthread_t ws_thread;

/* WebSocket 服务端线程函数 */
static void* ws_server_thread_func(void *arg) {
	struct ez_ws_server_handle *handle = (struct ez_ws_server_handle *)arg;
	int n = 0;

	lwsl_user("[ws_thread] WebSocket thread started\n");

	while (n >= 0 && !interrupted) {
		n = ez_ws_server_service_exec(handle, 0);
	}

	lwsl_user("[ws_thread] WebSocket thread exiting\n");
	return NULL;
}

/* WebSocket 连接建立回调函数 */
static void ws_server_on_connected(int client_id, const char *ip, int port, void *user_data) {
    lwsl_user("[WebSocket] Client #%d connected from %s:%d\n", client_id, ip ? ip : "unknown", port);
}

/* WebSocket 断开连接回调函数 */
static void ws_server_on_disconnected(int client_id, void *user_data) {
    lwsl_user("[WebSocket] Client #%d disconnected\n", client_id);
}

/* WebSocket 接收数据回调函数 */
static void ws_server_on_receive(int client_id, const void *data, size_t len, int is_binary, void *user_data) {
    lwsl_user("[WebSocket] RECEIVE CALLBACK: client_id=%d, len=%zu, is_binary=%d\n", client_id, len, is_binary);

    if (!data || !len) {
        lwsl_user("[WebSocket] No data received\n");
        return;
    }

    /* 打印接收到的数据 */
    lwsl_user("[WebSocket] Received from client #%d: ", client_id);

    if (is_binary) {
        lwsl_user("(binary data, %zu bytes)\n", len);
    } else {
        /* 文本数据，打印内容 */
        size_t print_len = len > 256 ? 256 : len;
        char buf[257];
        memcpy(buf, data, print_len);
        buf[print_len] = '\0';

        /* 替换控制字符，但保留换行符和制表符 */
        for (size_t i = 0; i < print_len; i++) {
            if (buf[i] < 32 && buf[i] != '\n' && buf[i] != '\r' && buf[i] != '\t') {
                buf[i] = '.';
            }
        }

        lwsl_user("'%s'%s\n", buf, len > 256 ? "..." : "");
    }
}

/* Console线程函数 - 完全独立于websocket */
static void *
console_thread_func(void *arg)
{
    struct console_handle *console = (struct console_handle *)arg;
    char line[4096];
    int interactive = isatty(STDIN_FILENO);

    console->console_running = 1;
    if (interactive) {
        lwsl_user("Server console ready. Commands:\n");
        lwsl_user("  clients       - Show connected clients list\n");
        lwsl_user("  send <id> <msg> - Send message to specific client\n");
        lwsl_user("  status        - Show server status\n");
        lwsl_user("  help          - Show help information\n");
        lwsl_user("  quit          - Exit server\n");
        lwsl_user("  other         - Broadcast to all clients\n");
    }
    
    while (console->console_running) {
        if (interactive) {
            fputs(LWS_MIN_SERVER_PROMPT, stdout);
            fflush(stdout);
        }
        if (!fgets(line, sizeof(line), stdin)) {
            ez_usleep(100000);
            continue;
        }

        size_t len = strlen(line);
        while (len && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (!len)
            continue;

        /* 处理quit/exit命令 */
        if (!strcmp(line, "quit") || !strcmp(line, "exit")) {
            if (console->interrupted)
                *console->interrupted = 1;
            break;
        }

        /* 处理clients命令 - 显示已连接的客户端列表 */
        if (!strcmp(line, "clients")) {
            if (console->ws_handle) {
                int count = ez_ws_server_get_client_count(console->ws_handle);
                lwsl_user("\n=== Connected Clients (%d) ===\n", count);
                if (count == 0) {
                    lwsl_user("  No clients connected\n");
                } else {
                    lwsl_user("  (Client list details not available in abstract API)\n");
                }
                lwsl_user("\n");
            } else {
                lwsl_user("WebSocket not initialized\n");
            }
            continue;
        }

        /* 处理status命令 */
        if (!strcmp(line, "status")) {
            if (console->ws_handle) {
                int count = ez_ws_server_get_client_count(console->ws_handle);
                int running = ez_ws_server_is_running(console->ws_handle);
                lwsl_user("\nServer Status:\n");
                lwsl_user("  Port: listening\n");
                lwsl_user("  Connected clients: %d\n", count);
                lwsl_user("  Active: %s\n", running ? "running" : "stopped");
            } else {
                lwsl_user("WebSocket not initialized\n");
            }
            continue;
        }

        /* 处理help命令 */
        if (!strcmp(line, "help")) {
            lwsl_user("\nAvailable commands:\n");
            lwsl_user("  clients          - Show connected clients list\n");
            lwsl_user("  send <id> <msg>  - Send message to specific client by ID\n");
            lwsl_user("  status           - Show server status\n");
            lwsl_user("  help             - Show this help message\n");
            lwsl_user("  quit/exit        - Stop server and exit\n");
            lwsl_user("  other input      - Broadcast message to all connected clients\n");
            lwsl_user("\nExample:\n");
            lwsl_user("  send 1 Hello     - Send 'Hello' to client #1\n");
            lwsl_user("\n");
            continue;
        }

        /* 处理send命令 - 向指定客户端发送消息 */
        if (strncmp(line, "send ", 5) == 0) {
            if (!console->ws_handle) {
                lwsl_user("[server console] WebSocket not initialized\n");
                continue;
            }
            
            /* 解析命令：send <client_id> <message> */
            char *cmd_ptr = line + 5;  /* 跳过 "send " */
            while (*cmd_ptr == ' ') cmd_ptr++;  /* 跳过空格 */
            
            if (!*cmd_ptr) {
                lwsl_user("[server console] Usage: send <client_id> <message>\n");
                continue;
            }
            
            /* 解析客户端ID */
            char *endptr;
            long client_id = strtol(cmd_ptr, &endptr, 10);
            
            if (endptr == cmd_ptr || client_id < 0) {
                lwsl_user("[server console] Invalid client ID\n");
                continue;
            }
            
            /* 跳过ID后的空格，找到消息内容 */
            cmd_ptr = endptr;
            while (*cmd_ptr == ' ') cmd_ptr++;
            
            if (!*cmd_ptr) {
                lwsl_user("[server console] Message cannot be empty\n");
                continue;
            }
            
            /* 发送消息到指定客户端 */
            int ret = ez_ws_server_send_text(console->ws_handle, (int)client_id, cmd_ptr, 0);
            
            if (ret == EZ_WS_SERVER_OK) {
                lwsl_user("[server console] Message sent to client #%d\n", (int)client_id);
            } else if (ret == EZ_WS_SERVER_ERR_CLIENT_NOT_FOUND) {
                lwsl_user("[server console] Client #%d not found\n", (int)client_id);
            } else {
                lwsl_user("[server console] Send failed, error code: %d\n", ret);
            }
            continue;
        }

        /* 发送消息 - 使用websocket发送接口（广播） */
        if (console->ws_handle) {
            /* 默认广播到所有客户端 */
            lwsl_user("[server console] Broadcasting message: '%s' (%zu bytes)\n", line, len);
            int ret = ez_ws_server_send_text(console->ws_handle, -1, line, len);
            if (ret == EZ_WS_SERVER_OK) {
                lwsl_user("[server console] Message broadcasted successfully\n");
            } else if (ret == EZ_WS_SERVER_ERR_QUEUE_FULL) {
                lwsl_user("[server console] Broadcast failed, queue full\n");
            } else if (ret == EZ_WS_SERVER_ERR_CLIENT_NOT_FOUND) {
                lwsl_user("[server console] Client not found\n");
            } else {
                lwsl_user("[server console] Broadcast failed, error code: %d\n", ret);
            }
        } else {
            lwsl_user("[server console] WebSocket not initialized\n");
        }
    }

    console->console_running = 0;
    return NULL;
}

/*
 * 初始化Console（作为websocket的外部使用方）
 */
static struct console_handle*
console_init(struct ez_ws_server_handle *ws_handle, int *interrupted_flag)
{
	struct console_handle *console;
	
	if (!ws_handle || !interrupted_flag)
		return NULL;
	
	console = (struct console_handle *)malloc(sizeof(struct console_handle));
	if (!console)
		return NULL;
	
	memset(console, 0, sizeof(*console));
	
	console->interrupted = interrupted_flag;
	console->console_running = 0;
	console->ws_handle = ws_handle;
	
	return console;
}

/*
 * 启动Console线程
 */
static int
console_start(struct console_handle *console)
{
	if (!console)
		return -1;
	
	console->console_running = 1;
	if (pthread_create(&console->console_thread, NULL, console_thread_func, console) != 0) {
		lwsl_err("Failed to create console thread\n");
		console->console_running = 0;
		return -1;
	}
	
	return 0;
}

/*
 * 停止Console线程
 */
static void
console_stop(struct console_handle *console)
{
	if (!console)
		return;
	
	/* 停止console线程 */
	console->console_running = 0;
	pthread_join(console->console_thread, NULL);
}

/*
 * 清理Console
 */
static void
console_cleanup(struct console_handle *console)
{
	if (!console)
		return;
	free(console);
}

void sigint_handler(int sig)
{
	(void)sig;
	interrupted = 1;
}

int main(int argc, const char **argv)
{
	struct ez_ws_server_handle *server_handle = NULL;
	struct console_handle *console = NULL;
	struct ez_ws_server_config config;
	const char *p;
	int logs = LLL_USER | LLL_ERR | LLL_WARN | LLL_NOTICE;

	/* 1. 解析命令行参数 */
	if ((p = lws_cmdline_option(argc, argv, "-d")))
		logs = atoi(p);

	lws_set_log_level(logs, NULL);
	lwsl_user("LWS minimal ws server echo + permessage-deflate\n");
	lwsl_user("   lws-minimal-ws-server-echo [-p port] [--protocol name] [-o (once)]\n");

	/* 2. 配置WebSocket服务端参数 */
	memset(&config, 0, sizeof(config));
	config.port = 54321;  /* 默认端口 */
	config.protocol = WS_SERVER_PROTOCOL;  /* 默认协议名称 */
	config.path_prefix = WS_SERVER_PATH_PREFIX;  /* 默认路径前缀 */
	config.options = 0;

	if ((p = lws_cmdline_option(argc, argv, "-p")))
		config.port = atoi(p);
	if ((p = lws_cmdline_option(argc, argv, "--protocol")))
		config.protocol = p;
	if (lws_cmdline_option(argc, argv, "-o"))
		config.options |= 1;

	lwsl_user("Server listening on port %d\n", config.port);

	/* 3. 注册信号处理 */
	signal(SIGINT, sigint_handler);

	/* 4. 设置WebSocket回调函数 */
	struct ez_ws_server_callbacks ws_callbacks = {
		.on_receive = ws_server_on_receive,
		.on_connected = ws_server_on_connected,
		.on_disconnected = ws_server_on_disconnected,
		.user_data = NULL
	};

	/* 5. 初始化WebSocket服务端 */
	server_handle = ez_ws_server_handle_create(&config, &ws_callbacks);
	if (!server_handle) {
		lwsl_err("WebSocket server init failed\n");
		return 1;
	}

	/* 6. 初始化Console（作为websocket的外部使用方） */
	console = console_init(server_handle, &interrupted);
	if (!console) {
		lwsl_err("Console init failed\n");
		ez_ws_server_cleanup(server_handle);
		return 1;
	}

	/* 7. 启动WebSocket线程 */
	if (pthread_create(&ws_thread, NULL, ws_server_thread_func, server_handle) != 0) {
		lwsl_err("Failed to create WebSocket thread\n");
		console_cleanup(console);
		ez_ws_server_cleanup(server_handle);
		return 1;
	}

	/* 8. 启动Console线程 */
	if (console_start(console) != 0) {
		lwsl_err("Failed to start console thread\n");
		/* 停止WebSocket线程 */
		interrupted = 1;
		pthread_join(ws_thread, NULL);
		console_cleanup(console);
		ez_ws_server_cleanup(server_handle);
		return 1;
	}

	lwsl_user("All threads started, entering main loop...\n");

	/* 8. 主循环：等待中断信号 */
	while (!interrupted) {
		sleep(1);  /* 每秒检查一次 */
	}

	/* 9. 清理资源 */
	lwsl_user("Shutting down server...\n");
	
	/* 停止线程 */
	console_stop(console);

	/* 停止WebSocket服务 */
	interrupted = 1;

	/* 等待WebSocket线程结束 */
	pthread_join(ws_thread, NULL);

	/* 清理资源 */
	console_cleanup(console);
	ez_ws_server_cleanup(server_handle);

	lwsl_user("Server shutdown completed\n");

	return 0;
}
