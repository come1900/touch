/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * touch-cli.c - WebSocket Client Example Program
 *
 * Copyright (C) 2011 ezlibs.com, All Rights Reserved.
 *
 * $Id: touch-cli.c 1 2011-12-27 20:00:00Z WHF $
 *
 * Explain:
 *     WebSocket client example program using ez_wsclient-native component.
 *
 * Update:
 *     2011-12-27 20:00:00 WHF Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#define _POSIX_C_SOURCE 200809L
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <getopt.h>
#include <errno.h>
#include <sys/select.h>
#include <sys/time.h>
#include <time.h>

#include <ezutil/ez_system_api.h>

#include "ez_wsclient-native.h"
#include "__def_debug.h"

/* WebSocket协议和路径配置 */
#ifndef EZ_WS_CLIENT_PROTOCOL
#define EZ_WS_CLIENT_PROTOCOL "come.0"
#endif

#ifndef EZ_WS_CLIENT_PATH
#define EZ_WS_CLIENT_PATH "/come"
#endif

/* 控制台提示符 */
#ifndef EZ_WS_CLIENT_PROMPT
#define EZ_WS_CLIENT_PROMPT "ws> "
#endif

/* 断线重连配置 */
#ifndef RECONNECT_INTERVAL_MS
#define RECONNECT_INTERVAL_MS (1*500)
#endif

#ifndef RECONNECT_MAX_RETRIES
#define RECONNECT_MAX_RETRIES 0
#endif

#ifndef RECONNECT_BACKOFF_ENABLE
#define RECONNECT_BACKOFF_ENABLE 1
#endif

#if RECONNECT_BACKOFF_ENABLE
#ifndef RECONNECT_BACKOFF_MIN_RETRIES
#define RECONNECT_BACKOFF_MIN_RETRIES 2
#endif

#ifndef RECONNECT_BACKOFF_HIGH_THRESHOLD
#define RECONNECT_BACKOFF_HIGH_THRESHOLD 5
#endif
#endif

/* Console句柄 */
struct console_handle {
	pthread_t console_thread;
	int console_running;
	int *interrupted;
	
	/* Console到websocket的发送队列 */
	char **pending_lines;
	size_t pending_cap;
	size_t pending_count;
	pthread_mutex_t pending_lock;
	
	struct ez_ws_client_handle *ws_handle;
};


/* Console线程函数 */
static void *console_thread_func(void *arg) {
	struct console_handle *console = (struct console_handle *)arg;
	char line[4096];
	int interactive = isatty(STDIN_FILENO);
	
	console->console_running = 1;
	if (interactive) {
		printf("Console ready. Commands:\n");
		printf("  status  - Show WebSocket status\n");
		printf("  help    - Show help information\n");
		printf("  quit    - Exit program\n");
		printf("  other   - Send to server\n");
	}
	
	/* 只在交互模式下才打印初始提示符 */
	if (interactive) {
		fputs(EZ_WS_CLIENT_PROMPT, stdout);
		fflush(stdout);
	}
	
	while (console->console_running) {
		/* 检查中断标志 */
		if (console->interrupted && *console->interrupted) {
			break;
		}
		
		/* 使用 select 来检查 stdin 是否有数据，同时可以定期检查中断标志 */
		fd_set readfds;
		struct timeval timeout;
		FD_ZERO(&readfds);
		FD_SET(STDIN_FILENO, &readfds);
		timeout.tv_sec = 0;
		timeout.tv_usec = 100000; /* 100ms */
		
		int ret = select(STDIN_FILENO + 1, &readfds, NULL, NULL, &timeout);
		if (ret < 0) {
			if (errno == EINTR)
				continue;
			break;
		}
		
		/* 再次检查中断标志 */
		if (console->interrupted && *console->interrupted) {
			break;
		}
		
		/* 如果没有数据可读，继续循环（不打印提示符） */
		if (ret == 0 || !FD_ISSET(STDIN_FILENO, &readfds)) {
			continue;
		}
		
		/* 有数据可读，使用 fgets 读取 */
		if (!fgets(line, sizeof(line), stdin)) {
			/* EOF 或错误 */
			if (feof(stdin)) {
				break;
			}
			continue;
		}
		
		size_t len = strlen(line);
		while (len && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
			line[--len] = '\0';
		}
		if (!len) {
			/* 空行，重新打印提示符 */
			if (interactive) {
				fputs(EZ_WS_CLIENT_PROMPT, stdout);
				fflush(stdout);
			}
			continue;
		}
		
		/* 处理quit/exit命令 */
		if (!strcmp(line, "quit") || !strcmp(line, "exit")) {
			if (console->interrupted)
				*console->interrupted = 1;
			break;
		}
		
		/* 处理status命令 */
		if (!strcmp(line, "status")) {
			if (console->ws_handle) {
				enum ez_ws_state state = ez_ws_get_state(console->ws_handle);
				printf("\nWebSocket Status:\n");
				printf("  Connection: %s\n", 
				       state == EZ_WS_STATE_CONNECTED ? "Connected" :
				       state == EZ_WS_STATE_CONNECTING ? "Connecting" :
				       state == EZ_WS_STATE_HANDSHAKING ? "Handshaking" :
				       "Disconnected");
				/* 注意：retry_count 和 current_retry_delay 现在是内部状态，无法直接访问 */
			} else {
				printf("WebSocket not initialized\n");
			}
			/* 处理完命令后，打印下一个提示符 */
			if (interactive) {
				fputs(EZ_WS_CLIENT_PROMPT, stdout);
				fflush(stdout);
			}
			continue;
		}
		
		/* 处理help命令 */
		if (!strcmp(line, "help")) {
			printf("\nAvailable commands:\n");
			printf("  status       - Show WebSocket connection status\n");
			printf("  help         - Show this help message\n");
			printf("  quit/exit    - Exit program\n");
			printf("  other input  - Send as message to server\n");
			printf("\n");
			/* 处理完命令后，打印下一个提示符 */
			if (interactive) {
				fputs(EZ_WS_CLIENT_PROMPT, stdout);
				fflush(stdout);
			}
			continue;
		}
		
		/* 发送消息 */
		if (console->ws_handle) {
			int ret = ez_ws_send_text(console->ws_handle, line, len);
			if (ret != EZ_WS_OK) {
				printf("[console] Send failed: error code: %d\n", ret);
			}
		} else {
			printf("[console] WebSocket not initialized\n");
		}
		
		/* 处理完命令后，打印下一个提示符 */
		if (interactive) {
			fputs(EZ_WS_CLIENT_PROMPT, stdout);
			fflush(stdout);
		}
	}
	
	console->console_running = 0;
	return NULL;
}

/* 初始化Console */
struct console_handle *console_init(struct ez_ws_client_handle *ws_handle, int *interrupted_flag) {
	struct console_handle *console = calloc(1, sizeof(struct console_handle));
	if (!console)
		return NULL;
	
	console->interrupted = interrupted_flag;
	console->ws_handle = ws_handle;
	console->pending_cap = 64;
	console->pending_lines = calloc(console->pending_cap, sizeof(char *));
	if (!console->pending_lines) {
		free(console);
		return NULL;
	}
	
	pthread_mutex_init(&console->pending_lock, NULL);
	return console;
}

/* 启动Console线程 */
int console_start(struct console_handle *console) {
	if (!console)
		return -1;
	
	console->console_running = 1;
	if (pthread_create(&console->console_thread, NULL, console_thread_func, console) != 0) {
		console->console_running = 0;
		return -1;
	}
	
	return 0;
}

/* 停止Console线程 */
void console_stop(struct console_handle *console) {
	if (!console)
		return;
	
	/* 设置停止标志 */
	console->console_running = 0;
	
	/* 如果设置了中断标志，也设置它 */
	if (console->interrupted) {
		*console->interrupted = 1;
	}
	
	/* 等待线程退出（最多等待 200ms，因为 select 超时是 100ms） */
	if (console->console_thread) {
		/* 使用 pthread_timedjoin_np 或简单的轮询等待 */
		for (int i = 0; i < 3 && console->console_running; i++) {
			ez_usleep(100000); /* 100ms */
		}
		
		/* 如果线程还在运行，强制 join（此时应该已经退出了） */
		if (console->console_thread) {
			pthread_join(console->console_thread, NULL);
		}
	}
}

/* 清理Console */
void console_cleanup(struct console_handle *console) {
	if (!console)
		return;
	
	pthread_mutex_destroy(&console->pending_lock);
	
	if (console->pending_lines) {
		for (size_t i = 0; i < console->pending_cap; ++i) {
			free(console->pending_lines[i]);
		}
		free(console->pending_lines);
	}
	
	free(console);
}

/* 示例回调函数 */
static void example_on_receive(const void *data, size_t len, int is_binary, void *user_data) {
	LOG_INFO("\n[Callback] Received %s data: %zu bytes\n", 
	       is_binary ? "binary" : "text", len);
	
	if (!is_binary && len < 1024) {
		char buf[1024];
		memcpy(buf, data, len);
		buf[len] = '\0';
		LOG_INFO("[Callback] Content: %s\n", buf);
	}
}

static void example_on_connected(void *user_data) {
	LOG_INFO("\n[Callback] WebSocket connected\n");
}

static void example_on_disconnected(void *user_data) {
	LOG_INFO("\n[Callback] WebSocket disconnected\n");
}

static void example_on_sent(const void *data, size_t len, void *user_data) {
	LOG_INFO("[Callback] Data sent: %zu bytes\n", len);
}

/* 信号处理 */
static int g_interrupted = 0;
void sigint_handler(int sig) {
	g_interrupted = 1;
}

/* WebSocket服务线程函数 - 在外部线程中运行 */
static void *ws_service_thread_func(void *arg) {
	struct ez_ws_client_handle *ws = (struct ez_ws_client_handle *)arg;
	
	while (!g_interrupted) {
		int ret = ez_ws_service_exec(ws, 0);
		if (ret < 0) {
			/* 错误或需要停止 */
			break;
		}
	}
	
	return NULL;
}

/* 主函数 */
int main(int argc, const char **argv) {
	struct ez_ws_client_handle *client_handle = NULL;
	struct console_handle *console = NULL;
	pthread_t ws_service_thread = 0;
	struct ez_ws_callbacks callbacks;
	struct ez_ws_client_config config = {0};

	/* 设置默认配置 */
	config.server_addr = "localhost";
	config.port = 54321;
	config.url_path = EZ_WS_CLIENT_PATH;
	config.protocol = EZ_WS_CLIENT_PROTOCOL;
	config.reconnect_interval_ms = RECONNECT_INTERVAL_MS;
	config.reconnect_max_retries = RECONNECT_MAX_RETRIES;
	config.reconnect_backoff_enable = RECONNECT_BACKOFF_ENABLE;
	config.reconnect_backoff_min_retries = RECONNECT_BACKOFF_MIN_RETRIES;
	config.reconnect_backoff_high_threshold = RECONNECT_BACKOFF_HIGH_THRESHOLD;
	
	/* 解析命令行参数 */
	int opt;
	int option_index = 0;
	const char *optstring = "h:p:u:s:";
	struct option long_options[] = {
		{"host",     required_argument, 0, 'h'},
		{"port",     required_argument, 0, 'p'},
		{"url",      required_argument, 0, 'u'},
		{"server",   required_argument, 0, 's'},
		{"protocol", required_argument, 0, 0},
		{0, 0, 0, 0}
	};
	
	while ((opt = getopt_long(argc, (char *const *)argv, optstring, long_options, &option_index)) != -1) {
		switch (opt) {
		case 'h':
		case 's':
			config.server_addr = optarg;
			break;
		case 'p':
			config.port = (unsigned short)atoi(optarg);
			break;
		case 'u':
			config.url_path = optarg;
			break;
		case 0:
			/* 处理长选项 --protocol */
			if (strcmp(long_options[option_index].name, "protocol") == 0) {
				config.protocol = optarg;
			}
			break;
		default:
			printf("Usage: %s [-h|--host HOST] [-p|--port PORT] [-u|--url PATH] [--protocol PROTOCOL]\n", argv[0]);
			printf("  -h, --host HOST      Server hostname or IP address (default: localhost)\n");
			printf("  -s, --server HOST    Alias for --host\n");
			printf("  -p, --port PORT     Server port (default: 54321)\n");
			printf("  -u, --url PATH      WebSocket URL path (default: /come)\n");
			printf("      --protocol PROTOCOL  WebSocket subprotocol (default: come.0)\n");
			return 1;
		}
	}
	
	printf("Native WebSocket Client (using epoll/timerfd/socket)\n");
	printf("Connecting to %s:%hu%s\n", config.server_addr, config.port, config.url_path);
	
	/* 注册信号处理 */
	signal(SIGINT, sigint_handler);
	
	/* 设置回调 */
	callbacks.on_receive = example_on_receive;
	callbacks.on_connected = example_on_connected;
	callbacks.on_disconnected = example_on_disconnected;
	callbacks.on_sent = example_on_sent;
	callbacks.user_data = NULL;
	
	/* 初始化并启动WebSocket客户端 */
	client_handle = ez_ws_client_handle_create(&config, &callbacks);
	if (!client_handle) {
		printf("WebSocket client init failed\n");
		return 1;
	}
	
	/* 初始化Console */
	console = console_init(client_handle, &g_interrupted);
	if (!console) {
		printf("Console init failed\n");
		ez_ws_client_cleanup(client_handle);
		return 1;
	}
	
	/* 创建并启动WebSocket服务线程（外部管理） */
	if (pthread_create(&ws_service_thread, NULL, ws_service_thread_func, client_handle) != 0) {
		printf("Failed to create WebSocket service thread\n");
		console_cleanup(console);
		ez_ws_client_cleanup(client_handle);
		return 1;
	}
	
	/* 启动Console线程 */
	if (console_start(console) != 0) {
		printf("Failed to start console thread\n");
		if (ws_service_thread) {
			pthread_join(ws_service_thread, NULL);
		}
		console_cleanup(console);
		ez_ws_client_cleanup(client_handle);
		return 1;
	}
	
	printf("All threads started, entering main loop...\n");
	
	/* 主循环 */
	while (!g_interrupted) {
		sleep(1);
	}
	
	/* 清理资源 */
	printf("Shutting down...\n");
	
	console_stop(console);
	
	/* 等待WebSocket服务线程退出 */
	if (ws_service_thread) {
		pthread_join(ws_service_thread, NULL);
	}
	
	console_cleanup(console);
	/* cleanup 会自动停止连接 */
	ez_ws_client_cleanup(client_handle);
	
	printf("Completed\n");
	return 0;
}
