/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * ez_wsserver-native.c - Native Linux WebSocket Server Implementation
 *
 * Copyright (C) 2011 ezlibs.com, All Rights Reserved.
 *
 * $Id: ez_wsserver-native.c 1 2011-12-27 20:00:00Z WHF $
 *
 * Explain:
 *     Native Linux WebSocket server component implementation.
 *     Uses epoll, timerfd, socket and other Linux basic functions.
 *     Does not depend on libwebsockets library.
 *
 * Update:
 *     2011-12-27 20:00:00 WHF Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <time.h>
#include <unistd.h>

#include <ezutil/base64.h>
#include <ezutil/ez_websocket_parser.h>
#include <ezutil/http_parser.h>
#include <ezutil/sha1.h>
#include <ezutil/ez_system_api.h>

#include "ez_wsserver-native.h"

/* HTTP 握手请求缓冲区大小（字节）
 * WebSocket 握手请求通常 < 1KB，可根据实际情况调整
 * 如果遇到缓冲区溢出错误日志，可以适当增大此值
 */
#ifndef EZ_WS_HANDSHAKE_REQUEST_BUFFER_SIZE
#define EZ_WS_HANDSHAKE_REQUEST_BUFFER_SIZE  1024
#endif

/* WebSocket 接收缓冲区配置
 * 初始大小：用于接收 TCP 数据的初始缓冲区大小
 * 最大大小：防止 DoS 攻击，限制单个连接的最大缓冲区大小
 * 
 * 初始大小选择建议：
 * - 256字节：适合信令消息场景（大多数消息 < 256字节），内存占用最小，第一次接收时按需扩展
 * - 1KB：适合小消息场景（大多数消息 < 1KB），内存占用小，性能好
 * - 64KB：适合通用场景（消息大小不确定），减少扩展次数，但内存占用大
 * 
 * 注意：
 * - WebSocket 帧理论上可达 2^63-1 字节，但实际应用中大消息应使用分片（fragmentation）传输
 * - 1MB 最大限制适合大多数应用场景（JSON 消息、文本聊天、小文件传输 < 100KB）
 * - 如果客户端需要传输 >1MB 的单帧，应使用 WebSocket 分片机制（RFC 6455 Section 5.4）
 * - 此缓冲区仅用于临时存储未完整解析的数据，解析完的数据会立即移除
 * - 缓冲区采用按需扩展策略（每次扩展一倍），对于偶尔超过初始大小的消息，扩展开销可接受
 */
#ifndef EZ_WS_RECV_BUFFER_INITIAL_SIZE
#define EZ_WS_RECV_BUFFER_INITIAL_SIZE  256  /* 256字节初始大小，适合信令消息场景，按需扩展 */
#endif

#ifndef EZ_WS_RECV_BUFFER_MAX_SIZE
#define EZ_WS_RECV_BUFFER_MAX_SIZE  (1 * 1024 * 1024)  /* 1MB 最大限制 */
#endif

/* 安全释放内存宏 */
#define SAFE_FREE(ptr) do { \
	if (ptr) { \
		free(ptr); \
		ptr = NULL; \
	} \
} while (0)

/* 获取当前时间（毫秒） */
static uint64_t ez_ws_server_now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

/* 生成带抖动的 ping 间隔（毫秒） */
static uint32_t ez_ws_server_get_ping_interval_with_jitter(uint32_t base_interval_ms, uint32_t jitter_percent)
{
	if (base_interval_ms == 0)
		return 0;
	
	if (jitter_percent == 0)
		return base_interval_ms;
	
	uint32_t jitter_ms = (base_interval_ms * jitter_percent) / 100;
	uint32_t jitter_range = jitter_ms * 2;
	uint32_t random_offset = (uint32_t)(rand() % (jitter_range + 1));
	return base_interval_ms - jitter_ms + random_offset;
}

/* 客户端连接状态 */
enum client_state {
	CLIENT_STATE_HANDSHAKING,  /* 握手中 */
	CLIENT_STATE_CONNECTED,    /* 已连接 */
	CLIENT_STATE_CLOSING,      /* 关闭中 */
	CLIENT_STATE_CLOSED        /* 已关闭 */
};

/* 客户端连接信息 */
struct client_connection {
	int sockfd;                    /* socket 文件描述符 */
	enum client_state state;       /* 连接状态 */
	
	/* 指向服务器句柄 */
	struct ez_ws_server_handle *server;
	
	/* 客户端信息（使用 ez_ws_client_info 结构体整合） */
	struct ez_ws_client_info client_info;
	
	/* HTTP 握手相关 */
	char handshake_request[EZ_WS_HANDSHAKE_REQUEST_BUFFER_SIZE];   /* HTTP 握手请求缓冲区 */
	size_t handshake_request_len;  /* 已接收的握手请求长度 */
	http_parser http_parser;
	http_parser_settings http_parser_settings;
	int http_handshake_complete;
	char *http_header_field;
	char *http_header_value;
	char *http_sec_websocket_key;
	char *http_sec_websocket_protocol;
	char *http_url_path;
	
	/* WebSocket 解析器 */
	ez_websocket_parser ws_parser;
	ez_websocket_parser_settings ws_parser_settings;
	
	/* 接收缓冲区（动态扩展） */
	uint8_t *recv_buffer;
	size_t recv_buffer_size;  /* 缓冲区容量 */
	size_t recv_buffer_len;   /* 已接收的数据长度 */
	
	/* 当前帧数据收集器 */
	struct {
		uint8_t *buffer;
		size_t buffer_size;
		size_t buffer_cap;
		int opcode;
		int is_binary;
	} current_frame;
	
	/* 发送队列 */
	struct send_queue_node {
		uint8_t *data;
		size_t len;
		struct send_queue_node *next;
	} *send_queue_head, *send_queue_tail;
	pthread_mutex_t send_queue_lock;
	size_t send_queue_size;
	
	/* 发送缓冲区（用于部分发送） */
	uint8_t *pending_send_data;
	size_t pending_send_len;
	size_t pending_send_sent;
	
	/* 保活相关 */
	uint64_t last_rx_time_ms;      /* 最后接收时间 */
	uint64_t last_tx_time_ms;       /* 最后发送时间 */
	/* 注意：last_activity_ms 已整合到 client_info 中 */
	uint64_t last_ping_time_ms;     /* 最后发送 ping 时间 */
	int awaiting_pong;             /* 是否等待 pong */
	uint32_t current_ping_interval_ms; /* 当前使用的 ping 间隔（带抖动） */
	
	/* 链表指针 */
	struct client_connection *next;
};

/* WebSocket 服务端句柄（完整定义） */
struct ez_ws_server_handle {
	/* 配置 */
	struct ez_ws_server_config config;
	
	/* 回调函数 */
	struct ez_ws_server_callbacks callbacks;
	
	/* TCP 服务器 */
	int listen_sockfd;             /* 监听 socket */
	int epollfd;                   /* epoll 文件描述符 */
	
	/* 定时器 */
	int timerfd;                   /* 定时器文件描述符（用于 ping/pong 和超时检测） */
	
	/* 客户端连接列表 */
	struct client_connection *client_list;
	pthread_mutex_t client_list_lock;
	int client_count;
	int next_client_id;
	
	/* 状态 */
	int ready;
	int interrupted;
	
	/* 中断标志指针（外部传入） */
	int *interrupted_ptr;
};

/* 设置 socket 为非阻塞 */
static int set_nonblocking(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0)
		return -1;
	return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* 设置 socket 选项 */
static int set_socket_options(int sockfd)
{
	int opt = 1;
	
	/* 允许地址重用 */
	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
		return -1;
	
	/* 禁用 Nagle 算法（降低延迟） */
	if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0)
		return -1;
	
	return 0;
}

/* 创建定时器 */
static int create_timerfd(uint32_t interval_ms)
{
	int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
	if (timerfd < 0)
		return -1;
	
	struct itimerspec timer_spec;
	timer_spec.it_value.tv_sec = interval_ms / 1000;
	timer_spec.it_value.tv_nsec = (interval_ms % 1000) * 1000000;
	timer_spec.it_interval.tv_sec = timer_spec.it_value.tv_sec;
	timer_spec.it_interval.tv_nsec = timer_spec.it_value.tv_nsec;
	
	if (timerfd_settime(timerfd, 0, &timer_spec, NULL) < 0) {
		close(timerfd);
		return -1;
	}
	
	return timerfd;
}


/* ================== 前向声明 ================== */
/* HTTP 解析器回调函数 */
static int on_http_url(http_parser *parser, const char *at, size_t length);
static int on_http_header_field(http_parser *parser, const char *at, size_t length);
static int on_http_header_value(http_parser *parser, const char *at, size_t length);
static int on_http_headers_complete(http_parser *parser);

/* WebSocket 解析器回调函数 */
static int on_ws_frame_begin(ez_websocket_parser *parser);
static int on_ws_frame_payload(ez_websocket_parser *parser, const char *at, size_t length);
static int on_ws_frame_complete(ez_websocket_parser *parser);

/* 发送队列处理函数 */
static int process_send_queue(struct client_connection *client);

/* ================== 步骤 a: 创建经典的 TCP 服务器 ================== */

/* 创建并绑定 TCP 监听 socket */
static int create_listen_socket(int port)
{
	int sockfd;
	
	
	/* 创建 socket */
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
		return -1;
	}
	
	/* 设置 socket 选项 */
	if (set_socket_options(sockfd) < 0) {
		close(sockfd);
		return -1;
	}
	
	/* 设置为非阻塞 */
	if (set_nonblocking(sockfd) < 0) {
		close(sockfd);
		return -1;
	}
	
	/* 绑定地址 */
	struct sockaddr_in addr = {.sin_family = AF_INET, .sin_addr.s_addr = INADDR_ANY, .sin_port = htons(port)};
	if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		fprintf(stderr, "Failed to bind to port %d: %s\n", port, strerror(errno));
		close(sockfd);
		return -1;
	}
	
	/* 开始监听 */
	if (listen(sockfd, SOMAXCONN) < 0) {
		fprintf(stderr, "Failed to listen: %s\n", strerror(errno));
		close(sockfd);
		return -1;
	}
	
	return sockfd;
}

/* 接受新连接 */
static int accept_new_connection(struct ez_ws_server_handle *server)
{
	struct sockaddr_in client_addr;
	socklen_t addr_len = sizeof(client_addr);
	int client_sockfd;
	
	/* 接受连接 */
	client_sockfd = accept(server->listen_sockfd, (struct sockaddr *)&client_addr, &addr_len);
	if (client_sockfd < 0) {
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			fprintf(stderr, "Failed to accept connection: %s\n", strerror(errno));
		}
		return -1;
	}
	
	/* 设置 socket 选项 */
	if (set_socket_options(client_sockfd) < 0) {
		close(client_sockfd);
		return -1;
	}
	
	/* 设置为非阻塞 */
	if (set_nonblocking(client_sockfd) < 0) {
		close(client_sockfd);
		return -1;
	}
	
	/* 创建客户端连接结构 */
	struct client_connection *client = calloc(1, sizeof(struct client_connection));
	if (!client) {
		close(client_sockfd);
		return -1;
	}
	
	/* 初始化客户端连接 */
	client->sockfd = client_sockfd;
	client->state = CLIENT_STATE_HANDSHAKING;
	client->server = server;
	
	/* 初始化接收缓冲区（动态分配） */
	client->recv_buffer = malloc(EZ_WS_RECV_BUFFER_INITIAL_SIZE);
	if (!client->recv_buffer) {
		free(client);
		close(client_sockfd);
		return -1;
	}
	client->recv_buffer_size = EZ_WS_RECV_BUFFER_INITIAL_SIZE;
	client->recv_buffer_len = 0;
	
	/* 初始化客户端信息（calloc 已将所有字段初始化为0） */
	client->client_info.connect_time = time(NULL);
	
	/* 获取客户端地址 */
	inet_ntop(AF_INET, &client_addr.sin_addr, client->client_info.ip, sizeof(client->client_info.ip));
	client->client_info.port = ntohs(client_addr.sin_port);
	
	/* 分配客户端 ID */
	pthread_mutex_lock(&server->client_list_lock);
	client->client_info.id = server->next_client_id++;
	pthread_mutex_unlock(&server->client_list_lock);
	
	/* 初始化发送队列锁 */
	pthread_mutex_init(&client->send_queue_lock, NULL);
	
	/* 初始化 HTTP 解析器 */
	http_parser_init(&client->http_parser, HTTP_REQUEST);
	client->http_parser.data = client;
	
	/* 设置 HTTP 解析器回调（calloc 已将字段初始化为0） */
	client->http_parser_settings.on_url = on_http_url;
	client->http_parser_settings.on_header_field = on_http_header_field;
	client->http_parser_settings.on_header_value = on_http_header_value;
	client->http_parser_settings.on_headers_complete = on_http_headers_complete;
	
	/* 初始化 WebSocket 解析器 */
	ez_websocket_parser_init(&client->ws_parser);
	ez_websocket_parser_settings_init(&client->ws_parser_settings);
	client->ws_parser.data = client;
	
	/* 设置 WebSocket 解析器回调 */
	client->ws_parser_settings.on_frame_begin = on_ws_frame_begin;
	client->ws_parser_settings.on_frame_payload = on_ws_frame_payload;
	client->ws_parser_settings.on_frame_complete = on_ws_frame_complete;
	
	/* 初始化时间戳 */
	uint64_t now_ms = ez_ws_server_now_ms();
	client->last_rx_time_ms = now_ms;
	client->last_tx_time_ms = now_ms;
	client->client_info.last_activity_ms = now_ms;
	
	/* 添加到客户端列表 */
	pthread_mutex_lock(&server->client_list_lock);
	client->next = server->client_list;
	server->client_list = client;
	server->client_count++;
	pthread_mutex_unlock(&server->client_list_lock);
	
	/* 添加到 epoll */
	struct epoll_event ev;
	ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
	ev.data.ptr = client;
	if (epoll_ctl(server->epollfd, EPOLL_CTL_ADD, client_sockfd, &ev) < 0) {
		fprintf(stderr, "Failed to add client to epoll: %s\n", strerror(errno));
		/* 清理客户端连接 */
		pthread_mutex_lock(&server->client_list_lock);
		if (server->client_list == client) {
			server->client_list = client->next;
		} else {
			struct client_connection *prev = server->client_list;
			while (prev && prev->next != client)
				prev = prev->next;
			if (prev)
				prev->next = client->next;
		}
		server->client_count--;
		pthread_mutex_unlock(&server->client_list_lock);
		close(client_sockfd);
		free(client);
		return -1;
	}
	
	/* 调用连接回调 */
	if (server->callbacks.on_connected) {
		server->callbacks.on_connected(client->client_info.id, client->client_info.ip, client->client_info.port, 
		                               server->callbacks.user_data);
	}
	
	return 0;
}

/* 查找客户端连接 */
static struct client_connection *find_client(struct ez_ws_server_handle *server, int client_id)
{
	struct client_connection *client;
	
	pthread_mutex_lock(&server->client_list_lock);
	client = server->client_list;
	while (client) {
		if (client->client_info.id == client_id)
			break;
		client = client->next;
	}
	pthread_mutex_unlock(&server->client_list_lock);
	
	return client;
}

/* 移除客户端连接 */
static void remove_client(struct ez_ws_server_handle *server, struct client_connection *client)
{
	if (!server || !client)
		return;
	
	/* 从 epoll 中移除 */
	epoll_ctl(server->epollfd, EPOLL_CTL_DEL, client->sockfd, NULL);
	
	/* 关闭 socket */
	close(client->sockfd);
	
	/* 从客户端列表中移除 */
	pthread_mutex_lock(&server->client_list_lock);
	if (server->client_list == client) {
		server->client_list = client->next;
	} else {
		struct client_connection *prev = server->client_list;
		while (prev && prev->next != client)
			prev = prev->next;
		if (prev)
			prev->next = client->next;
	}
	server->client_count--;
	pthread_mutex_unlock(&server->client_list_lock);
	
	/* 调用断开回调 */
	if (server->callbacks.on_disconnected) {
		server->callbacks.on_disconnected(client->client_info.id, server->callbacks.user_data);
	}
	
	/* 清理资源 */
	SAFE_FREE(client->http_header_field);
	SAFE_FREE(client->http_header_value);
	SAFE_FREE(client->http_sec_websocket_key);
	SAFE_FREE(client->http_sec_websocket_protocol);
	SAFE_FREE(client->http_url_path);
	SAFE_FREE(client->pending_send_data);
	SAFE_FREE(client->recv_buffer);
	
	/* 清理发送队列 */
	pthread_mutex_lock(&client->send_queue_lock);
	while (client->send_queue_head) {
		struct send_queue_node *node = client->send_queue_head;
		client->send_queue_head = node->next;
		SAFE_FREE(node->data);
		free(node);
	}
	client->send_queue_tail = NULL;
	client->send_queue_size = 0;
	pthread_mutex_unlock(&client->send_queue_lock);
	
	pthread_mutex_destroy(&client->send_queue_lock);
	
	/* 释放客户端结构 */
	free(client);
}

/* ================== 步骤 b: 使用 http_parser 做 HTTP 请求处理 ================== */

/* 计算 WebSocket Accept 值 */
static int compute_websocket_accept(const char *key, char *accept)
{
	char combined[256];
	snprintf(combined, sizeof(combined), "%s%s", key, EZ_WEBSOCKET_MAGIC_STRING);
	
	char hash[SHA1HashSize] = {0};
	SHA1Context sha;
	SHA1Reset(&sha);
	SHA1Input(&sha, (const unsigned char *)combined, strlen(combined));
	SHA1Result(&sha, (unsigned char *)hash);
	
	char base64_accept[32] = {0};
	ez_base64encode(base64_accept, (const char *)hash, SHA1HashSize);
	snprintf(accept, 32, "%s", base64_accept);
	return 0;
}

/* HTTP 解析器回调：URL */
static int on_http_url(http_parser *parser, const char *at, size_t length)
{
	struct client_connection *client = (struct client_connection *)parser->data;
	
	SAFE_FREE(client->http_url_path);
	client->http_url_path = malloc(length + 1);
	if (!client->http_url_path)
		return -1;
	memcpy(client->http_url_path, at, length);
	client->http_url_path[length] = '\0';
	
	return 0;
}

/* HTTP 解析器回调：头部字段 */
static int on_http_header_field(http_parser *parser, const char *at, size_t length)
{
	struct client_connection *client = (struct client_connection *)parser->data;
	
	SAFE_FREE(client->http_header_field);
	client->http_header_field = malloc(length + 1);
	if (!client->http_header_field)
		return -1;
	memcpy(client->http_header_field, at, length);
	client->http_header_field[length] = '\0';
	
	return 0;
}

/* HTTP 解析器回调：头部值 */
static int on_http_header_value(http_parser *parser, const char *at, size_t length)
{
	struct client_connection *client = (struct client_connection *)parser->data;
	
	SAFE_FREE(client->http_header_value);
	client->http_header_value = malloc(length + 1);
	if (!client->http_header_value)
		return -1;
	memcpy(client->http_header_value, at, length);
	client->http_header_value[length] = '\0';
	
	/* 检查重要的 WebSocket 头部 */
	if (client->http_header_field) {
		if (strcasecmp(client->http_header_field, "Sec-WebSocket-Key") == 0) {
			SAFE_FREE(client->http_sec_websocket_key);
			client->http_sec_websocket_key = strdup(client->http_header_value);
		} else if (strcasecmp(client->http_header_field, "Sec-WebSocket-Protocol") == 0) {
			SAFE_FREE(client->http_sec_websocket_protocol);
			client->http_sec_websocket_protocol = strdup(client->http_header_value);
		}
	}
	
	return 0;
}

/* HTTP 解析器回调：头部完成 */
static int on_http_headers_complete(http_parser *parser)
{
	struct client_connection *client = (struct client_connection *)parser->data;
	struct ez_ws_server_handle *server = client ? client->server : NULL;
	
	if (!client || !server)
		return -1;
	
	/* 检查方法必须是 GET */
	if (parser->method != 1) { /* HTTP_GET = 1 */
		return -1;
	}
	
	/* 检查 Upgrade 和 Connection 标志 */
	if (!(parser->flags & F_UPGRADE) || !(parser->flags & F_CONNECTION_UPGRADE)) {
		return -1;
	}
	
	/* 检查 Sec-WebSocket-Key */
	if (!client->http_sec_websocket_key) {
		return -1;
	}
	
	/* 检查路径前缀（如果配置了） */
	if (server->config.path_prefix) {
		if (!client->http_url_path || 
		    strncmp(client->http_url_path, server->config.path_prefix, 
		            strlen(server->config.path_prefix)) != 0) {
			/* 路径不匹配 */
			return -1;
		}
	}
	
	/* 检查协议（如果配置了） */
	if (server->config.protocol) {
		if (!client->http_sec_websocket_protocol) {
			/* 服务器要求协议但客户端未提供 */
			return -1;
		}
		/* 检查协议是否匹配 */
		if (strcmp(client->http_sec_websocket_protocol, server->config.protocol) != 0) {
			/* 协议不匹配 */
			return -1;
		}
	}
	
	/* 所有检查通过，标记握手完成 */
	client->http_handshake_complete = 1;
	return 0;
}

/* 生成 WebSocket 握手响应 */
static int generate_websocket_handshake_response(struct client_connection *client,
                                                  struct ez_ws_server_handle *server,
                                                  char *response, size_t response_size)
{
	char accept_key[64] = {0};
	
	/* 计算 Accept 值 */
	if (compute_websocket_accept(client->http_sec_websocket_key, accept_key) < 0) {
		return -1;
	}
	
	/* 注意：路径和协议验证已经在 on_http_headers_complete 中完成 */
	/* 如果到达这里，说明验证已经通过 */
	
	/* 生成成功响应 */
	/* 注意：HTTP 响应必须以 "HTTP/1.1" 开头，不能有任何前导字符 */
	int len = snprintf(response, response_size,
		"HTTP/1.1 101 Switching Protocols\r\n"
		"Upgrade: websocket\r\n"
		"Connection: Upgrade\r\n"
		"Sec-WebSocket-Accept: %s\r\n",
		accept_key);
	
	if (len < 0 || len >= (int)response_size) {
		return -1;
	}
	
	/* 如果配置了协议，返回协议 */
	if (server->config.protocol && client->http_sec_websocket_protocol) {
		int protocol_len = snprintf(response + len, response_size - len,
			"Sec-WebSocket-Protocol: %s\r\n",
			server->config.protocol);
		if (protocol_len < 0 || len + protocol_len >= (int)response_size) {
			return -1;
		}
		len += protocol_len;
	}
	
	/* 添加结束的 \r\n */
	int end_len = snprintf(response + len, response_size - len, "\r\n");
	if (end_len < 0 || len + end_len >= (int)response_size) {
		return -1;
	}
	len += end_len;
	
	/* 确保响应以 \0 结尾（用于调试） */
	if (len < (int)response_size) {
		response[len] = '\0';
	}
	
	return len;
}

/* 处理 HTTP 握手（只处理缓冲区中的数据，不接收） */
static int handle_handshake(struct ez_ws_server_handle *server, struct client_connection *client)
{
	/* 检查是否有数据需要处理 */
	if (client->handshake_request_len == 0)
		return 0;
	
	/* 使用 http_parser 解析 HTTP 请求 */
	/* 注意：http_parser 会从上次解析的位置继续 */
	size_t parsed = http_parser_execute(&client->http_parser, &client->http_parser_settings,
	                                    client->handshake_request,
	                                    client->handshake_request_len);
	
	/* 检查解析错误 */
	enum http_errno err = HTTP_PARSER_ERRNO(&client->http_parser);
	if (err != HPE_OK && err != HPE_PAUSED) {
		/* HTTP 解析错误 */
		return -1;
	}
	
	/* 移除已解析的数据 */
	if (parsed > 0 && parsed <= client->handshake_request_len) {
		if (parsed < client->handshake_request_len) {
			/* 部分解析，移动未解析的数据到缓冲区开头 */
			memmove(client->handshake_request,
			        client->handshake_request + parsed,
			        client->handshake_request_len - parsed);
		}
		client->handshake_request_len -= parsed;
	}
	
	/* 检查握手是否完成 */
	if (client->http_handshake_complete) {
		/* 生成握手响应 */
		char response[2048];
		int response_len = generate_websocket_handshake_response(client, server, response, sizeof(response));
		
		if (response_len < 0) {
			return -1;
		}
		
		/* 握手响应必须完整发送，不能分片 */
		/* 直接发送响应，确保完整性 */
		ssize_t total_sent = 0;
		while (total_sent < response_len) {
			ssize_t sent = send(client->sockfd, response + total_sent, 
			                   response_len - total_sent, 0);
			if (sent < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					/* 发送缓冲区满，需要等待 EPOLLOUT */
					/* 将剩余数据加入发送队列 */
					size_t remaining = response_len - total_sent;
					struct send_queue_node *node = malloc(sizeof(struct send_queue_node));
					if (!node)
						return -1;
					
					node->data = (uint8_t *)malloc(remaining);
					if (!node->data) {
						free(node);
						return -1;
					}
					
					memcpy(node->data, response + total_sent, remaining);
					node->len = remaining;
					node->next = NULL;
					
					pthread_mutex_lock(&client->send_queue_lock);
					if (client->send_queue_tail) {
						client->send_queue_tail->next = node;
					} else {
						client->send_queue_head = node;
					}
					client->send_queue_tail = node;
					client->send_queue_size++;
					pthread_mutex_unlock(&client->send_queue_lock);
					
					/* 确保监听 EPOLLOUT 事件 */
					struct epoll_event ev;
					ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
					ev.data.ptr = client;
					epoll_ctl(client->server->epollfd, EPOLL_CTL_MOD, client->sockfd, &ev);
					
					break; /* 等待 EPOLLOUT 继续发送 */
				}
				return -1; /* 发送错误 */
			}
			total_sent += sent;
		}
		
		/* 如果全部发送完成，更新发送时间 */
		if (total_sent >= response_len) {
			uint64_t now_ms = ez_ws_server_now_ms();
			client->last_tx_time_ms = now_ms;
			client->client_info.last_activity_ms = now_ms;
		}
		
		/* 握手成功，切换到已连接状态 */
		client->state = CLIENT_STATE_CONNECTED;
		
		/* 更新活动时间 */
		uint64_t now_ms = ez_ws_server_now_ms();
		client->client_info.last_activity_ms = now_ms;
		client->last_rx_time_ms = now_ms;
		client->last_tx_time_ms = now_ms;
		
		/* 初始化 ping 间隔（如果启用了 ping） */
		if (server->config.ping_interval_ms > 0) {
			client->current_ping_interval_ms = ez_ws_server_get_ping_interval_with_jitter(
				server->config.ping_interval_ms,
				server->config.ping_jitter_percent);
		}
		
		/* 清理握手请求缓冲区（不再需要） */
		memset(client->handshake_request, 0, sizeof(client->handshake_request));
		client->handshake_request_len = 0;
		
		return 1; /* 握手完成 */
	}
	
	return 0; /* 继续等待更多数据 */
}

/* ================== 步骤 c: 使用 ez_websocket_parser 做 WebSocket 请求处理 ================== */

/* 创建 WebSocket 帧（服务端不需要 mask） */
static int create_ws_frame_server(const uint8_t *payload, size_t payload_len,
                                   int opcode, int fin, uint8_t *frame, size_t *frame_len)
{
	size_t pos = 0;
	
	/* 第一个字节：FIN + RSV + Opcode */
	frame[pos++] = (fin ? 0x80 : 0x00) | (opcode & 0x0F);
	
	/* 第二个字节：MASK + Payload length（服务端不设置 MASK 位） */
	if (payload_len < 126) {
		frame[pos++] = payload_len;
	} else if (payload_len < 65536) {
		frame[pos++] = 126;
		frame[pos++] = (payload_len >> 8) & 0xFF;
		frame[pos++] = payload_len & 0xFF;
	} else {
		frame[pos++] = 127;
		int i;
		for (i = 7; i >= 0; i--) {
			frame[pos++] = (payload_len >> (i * 8)) & 0xFF;
		}
	}
	
	/* 复制 payload（服务端不需要 mask） */
	if (payload && payload_len > 0) {
		memcpy(frame + pos, payload, payload_len);
		pos += payload_len;
	}
	
	*frame_len = pos;
	return 0;
}

/* WebSocket 解析器回调：帧开始 */
static int on_ws_frame_begin(ez_websocket_parser *parser)
{
	struct client_connection *client = (struct client_connection *)parser->data;
	
	/* 清理上一个帧的缓冲区 */
	SAFE_FREE(client->current_frame.buffer);
	
	/* 初始化帧数据收集器 */
	client->current_frame.opcode = parser->opcode;
	client->current_frame.is_binary = (parser->opcode == EZ_WS_OPCODE_BINARY);
	client->current_frame.buffer_size = 0;
	client->current_frame.buffer_cap = 65536;
	client->current_frame.buffer = malloc(client->current_frame.buffer_cap);
	if (!client->current_frame.buffer) {
		client->current_frame.buffer_cap = 0;
		return -1;
	}
	
	return 0;
}

/* WebSocket 解析器回调：帧负载 */
static int on_ws_frame_payload(ez_websocket_parser *parser, const char *at, size_t length)
{
	struct client_connection *client = (struct client_connection *)parser->data;
	
	if (!client || !client->current_frame.buffer)
		return -1;
	
	/* 检查缓冲区容量 */
	if (client->current_frame.buffer_size + length > client->current_frame.buffer_cap) {
		/* 扩展缓冲区 */
		size_t new_cap = client->current_frame.buffer_cap * 2;
		while (new_cap < client->current_frame.buffer_size + length)
			new_cap *= 2;
		
		uint8_t *new_buffer = realloc(client->current_frame.buffer, new_cap);
		if (!new_buffer)
			return -1;
		
		client->current_frame.buffer = new_buffer;
		client->current_frame.buffer_cap = new_cap;
	}
	
	/* 复制数据 */
	memcpy(client->current_frame.buffer + client->current_frame.buffer_size, at, length);
	client->current_frame.buffer_size += length;
	
	return 0;
}

/* WebSocket 解析器回调：帧完成 */
static int on_ws_frame_complete(ez_websocket_parser *parser)
{
	struct client_connection *client = (struct client_connection *)parser->data;
	struct ez_ws_server_handle *server = client ? client->server : NULL;
	
	if (!server || !client)
		return -1;
	
	/* 更新活动时间 */
	uint64_t now_ms = ez_ws_server_now_ms();
	client->last_rx_time_ms = now_ms;
	client->client_info.last_activity_ms = now_ms;
	
	/* 处理不同类型的帧 */
	switch (parser->opcode) {
	case EZ_WS_OPCODE_TEXT:
	case EZ_WS_OPCODE_BINARY: {
		/* 文本或二进制数据帧 */
		/* 数据已经在 on_frame_payload 中收集到 current_frame.buffer */
		if (server->callbacks.on_receive && client->current_frame.buffer) {
			server->callbacks.on_receive(client->client_info.id,
			                            client->current_frame.buffer,
			                            client->current_frame.buffer_size,
			                            client->current_frame.is_binary,
			                            server->callbacks.user_data);
		}
		/* 清理帧缓冲区 */
		SAFE_FREE(client->current_frame.buffer);
		client->current_frame.buffer_size = 0;
		client->current_frame.buffer_cap = 0;
		break;
	}
	case EZ_WS_OPCODE_PING:
		/* 收到 ping，发送 pong */
		/* 注意：pong 的 payload 应该与 ping 的 payload 相同 */
		/* 这里简化处理，发送空的 pong */
		{
			uint8_t pong_frame[2];
			size_t pong_len;
			create_ws_frame_server(NULL, 0, EZ_WS_OPCODE_PONG, 1, pong_frame, &pong_len);
			send(client->sockfd, pong_frame, pong_len, 0);
		}
		break;
	case EZ_WS_OPCODE_PONG:
		/* 收到 pong，清除等待标志 */
		client->awaiting_pong = 0;
		break;
	case EZ_WS_OPCODE_CLOSE:
		/* 收到关闭帧 */
		parser->close_received = 1;
		client->state = CLIENT_STATE_CLOSING;
		break;
	default:
		break;
	}
	
	return 0;
}

/* 处理 WebSocket 数据（只处理缓冲区中的数据，不接收） */
static int handle_websocket_data(struct ez_ws_server_handle *server, struct client_connection *client)
{
	/* 检查是否有数据需要处理 */
	if (client->recv_buffer_len == 0)
		return 0;
	
	/* 更新接收时间 */
	uint64_t now_ms = ez_ws_server_now_ms();
	client->last_rx_time_ms = now_ms;
	client->client_info.last_activity_ms = now_ms;
	
	/* 使用 ez_websocket_parser 解析 WebSocket 数据帧 */
	/* 注意：ez_websocket_parser_execute 会解析数据并调用回调函数 */
	/* 回调函数会收集帧数据并在 frame_complete 时处理 */
	size_t parsed = ez_websocket_parser_execute(&client->ws_parser,
	                                           &client->ws_parser_settings,
	                                           (const char *)client->recv_buffer,
	                                           client->recv_buffer_len);
	
	/* 检查是否收到关闭帧 */
	if (client->ws_parser.close_received) {
		return -1; /* 关闭连接 */
	}
	
	/* 检查解析错误 */
	enum ez_ws_errno ws_err = EZ_WEBSOCKET_PARSER_ERRNO(&client->ws_parser);
	if (ws_err != EZ_WSE_OK) {
		/* WebSocket 解析错误 */
		return -1;
	}
	
	/* 移除已处理的数据（正确处理 TCP 流式特性） */
	if (parsed > 0 && parsed <= client->recv_buffer_len) {
		if (parsed < client->recv_buffer_len) {
			/* 部分解析，移动未解析的数据到缓冲区开头 */
			/* 这是 TCP 流式特性的体现：数据可能分片到达，需要保留未完整的数据 */
			memmove(client->recv_buffer,
			        client->recv_buffer + parsed,
			        client->recv_buffer_len - parsed);
		}
		client->recv_buffer_len -= parsed;
	} else if (parsed == 0 && client->recv_buffer_len > 0) {
		/* 没有解析任何数据，可能是数据不完整，保留在缓冲区中 */
		/* 检查缓冲区是否需要扩展 */
		if (client->recv_buffer_len >= client->recv_buffer_size) {
			/* 缓冲区已满，尝试扩展 */
			if (client->recv_buffer_size >= EZ_WS_RECV_BUFFER_MAX_SIZE) {
				/* 已达到最大限制，关闭连接 */
				fprintf(stderr, "[ERROR] Recv buffer reached maximum size (%d bytes). "
				        "Client #%d from %s:%d. Frame may be too large or malformed.\n",
				        EZ_WS_RECV_BUFFER_MAX_SIZE,
				        client->client_info.id, client->client_info.ip, client->client_info.port);
				return -1;
			}
			
			/* 扩展缓冲区（每次扩展一倍，但不超过最大限制） */
			size_t new_size = client->recv_buffer_size * 2;
			if (new_size > EZ_WS_RECV_BUFFER_MAX_SIZE) {
				new_size = EZ_WS_RECV_BUFFER_MAX_SIZE;
			}
			
			uint8_t *new_buffer = realloc(client->recv_buffer, new_size);
			if (!new_buffer) {
				fprintf(stderr, "[ERROR] Failed to expand recv buffer from %zu to %zu bytes. "
				        "Client #%d from %s:%d.\n",
				        client->recv_buffer_size, new_size,
				        client->client_info.id, client->client_info.ip, client->client_info.port);
				return -1;
			}
			
			client->recv_buffer = new_buffer;
			client->recv_buffer_size = new_size;
			
			fprintf(stderr, "[INFO] Expanded recv buffer from %zu to %zu bytes for client #%d from %s:%d.\n",
			        client->recv_buffer_size / 2, client->recv_buffer_size,
			        client->client_info.id, client->client_info.ip, client->client_info.port);
		}
	}
	
	return 0;
}

/* ================== 步骤 d 和 e: 发送队列、保活机制、主事件循环 ================== */

/* 发送数据到客户端（内部函数，完全异步） */
static int send_to_client_internal(struct client_connection *client, const uint8_t *data, size_t len, int opcode)
{
	if (!client || client->state != CLIENT_STATE_CONNECTED)
		return -1;
	
	/* 创建 WebSocket 帧 */
	size_t frame_cap = len + EZ_WS_MAX_FRAME_OVERHEAD;
	uint8_t *frame = malloc(frame_cap);
	if (!frame)
		return -1;
	
	size_t frame_len;
	if (create_ws_frame_server(data, len, opcode, 1, frame, &frame_len) < 0) {
		free(frame);
		return -1;
	}
	
	/* 完全异步发送：直接将数据加入发送队列，由 EPOLLOUT 事件驱动发送 */
	struct send_queue_node *node = malloc(sizeof(struct send_queue_node));
	if (!node) {
		free(frame);
		return -1;
	}
	
	node->data = frame;
	node->len = frame_len;
	node->next = NULL;
	
	int need_epollout = 0;
	pthread_mutex_lock(&client->send_queue_lock);
	if (client->send_queue_tail) {
		client->send_queue_tail->next = node;
	} else {
		client->send_queue_head = node;
		/* 如果队列为空，现在有数据了，需要监听 EPOLLOUT */
		need_epollout = 1;
	}
	client->send_queue_tail = node;
	client->send_queue_size++;
	pthread_mutex_unlock(&client->send_queue_lock);
	
	/* 如果有待发送数据且队列之前为空，需要启用 EPOLLOUT 监听 */
	if (need_epollout && !client->pending_send_data) {
		struct epoll_event ev;
		ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
		ev.data.ptr = client;
		epoll_ctl(client->server->epollfd, EPOLL_CTL_MOD, client->sockfd, &ev);
	}
	
	/* 尝试立即发送（如果 socket 可写且没有待发送数据） */
	/* 这样可以减少延迟，同时正确处理部分发送的情况 */
	if (!client->pending_send_data) {
		process_send_queue(client);
	}
	
	return 0;
}

/* 处理发送队列 */
static int process_send_queue(struct client_connection *client)
{
	if (!client || client->state != CLIENT_STATE_CONNECTED)
		return -1;
	
	/* 先处理 pending_send_data */
	if (client->pending_send_data) {
		ssize_t sent = send(client->sockfd,
		                   client->pending_send_data + client->pending_send_sent,
		                   client->pending_send_len - client->pending_send_sent,
		                   0);
		if (sent < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK)
				return -1;
			return 0; /* 继续等待 */
		}
		
		client->pending_send_sent += sent;
		if (client->pending_send_sent >= client->pending_send_len) {
			/* 发送完成 */
			SAFE_FREE(client->pending_send_data);
			client->pending_send_len = 0;
			client->pending_send_sent = 0;
		} else {
			return 0; /* 继续等待 */
		}
	}
	
	/* 处理发送队列 */
	pthread_mutex_lock(&client->send_queue_lock);
	while (client->send_queue_head) {
		struct send_queue_node *node = client->send_queue_head;
		
		ssize_t sent = send(client->sockfd, node->data, node->len, 0);
		if (sent < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				/* 发送缓冲区满，等待下次 */
				break;
			}
			/* 发送错误，移除节点 */
			client->send_queue_head = node->next;
			if (!client->send_queue_head)
				client->send_queue_tail = NULL;
			client->send_queue_size--;
			SAFE_FREE(node->data);
			free(node);
			pthread_mutex_unlock(&client->send_queue_lock);
			return -1;
		}
		
		/* 更新发送时间 */
		uint64_t now_ms = ez_ws_server_now_ms();
		client->last_tx_time_ms = now_ms;
		client->client_info.last_activity_ms = now_ms;
		
		if (sent >= (ssize_t)node->len) {
			/* 发送完成，移除节点 */
			client->send_queue_head = node->next;
			if (!client->send_queue_head)
				client->send_queue_tail = NULL;
			client->send_queue_size--;
			SAFE_FREE(node->data);
			free(node);
		} else {
			/* 部分发送，保存剩余数据到 pending_send_data */
			client->pending_send_data = malloc(node->len - sent);
			if (!client->pending_send_data) {
				client->send_queue_head = node->next;
				if (!client->send_queue_head)
					client->send_queue_tail = NULL;
				client->send_queue_size--;
				SAFE_FREE(node->data);
				free(node);
				pthread_mutex_unlock(&client->send_queue_lock);
				return -1;
			}
			memcpy(client->pending_send_data, node->data + sent, node->len - sent);
			client->pending_send_len = node->len - sent;
			client->pending_send_sent = 0;
			
			client->send_queue_head = node->next;
			if (!client->send_queue_head)
				client->send_queue_tail = NULL;
			client->send_queue_size--;
			SAFE_FREE(node->data);
			free(node);
			break; /* 等待下次 EPOLLOUT */
		}
	}
		pthread_mutex_unlock(&client->send_queue_lock);
	
	/* 如果发送队列为空且没有待发送数据，可以移除 EPOLLOUT 监听以节省资源 */
	if (!client->send_queue_head && !client->pending_send_data) {
		struct epoll_event ev;
		ev.events = EPOLLIN | EPOLLET;
		ev.data.ptr = client;
		epoll_ctl(client->server->epollfd, EPOLL_CTL_MOD, client->sockfd, &ev);
	}
	
	return 0;
}

/* 处理定时器事件（ping/pong 和超时检测） */
static void process_timer_events(struct ez_ws_server_handle *server)
{
	uint64_t now_ms = ez_ws_server_now_ms();
	
	pthread_mutex_lock(&server->client_list_lock);
	struct client_connection *client = server->client_list;
	struct client_connection *next;
	
	while (client) {
		next = client->next;
		
		if (client->state == CLIENT_STATE_CONNECTED) {
			/* 检查空闲超时 */
			if (server->config.idle_timeout_ms > 0) {
				if (client->client_info.last_activity_ms > 0 &&
				    now_ms - client->client_info.last_activity_ms > server->config.idle_timeout_ms) {
					/* 空闲超时，关闭连接 */
					pthread_mutex_unlock(&server->client_list_lock);
					remove_client(server, client);
					pthread_mutex_lock(&server->client_list_lock);
					client = next;
					continue;
				}
			}
			
			/* 检查 ping/pong */
			if (server->config.ping_interval_ms > 0) {
				/* 检查是否需要发送 ping */
				if (client->last_ping_time_ms == 0 ||
				    (now_ms - client->last_ping_time_ms >= client->current_ping_interval_ms)) {
					/* 发送 ping */
					uint8_t ping_frame[2];
					size_t ping_len;
					create_ws_frame_server(NULL, 0, EZ_WS_OPCODE_PING, 1, ping_frame, &ping_len);
					send(client->sockfd, ping_frame, ping_len, 0);
					
					client->last_ping_time_ms = now_ms;
					client->awaiting_pong = 1;
					
					/* 更新 ping 间隔（带抖动） */
					client->current_ping_interval_ms = ez_ws_server_get_ping_interval_with_jitter(
						server->config.ping_interval_ms,
						server->config.ping_jitter_percent);
				}
				
				/* 检查 pong 超时 */
				if (server->config.ping_timeout_ms > 0 && client->awaiting_pong) {
					if (now_ms - client->last_ping_time_ms > server->config.ping_timeout_ms) {
						/* pong 超时，关闭连接 */
						pthread_mutex_unlock(&server->client_list_lock);
						remove_client(server, client);
						pthread_mutex_lock(&server->client_list_lock);
						client = next;
						continue;
					}
				}
			}
		}
		
		client = next;
	}
	
	pthread_mutex_unlock(&server->client_list_lock);
}

/* 主事件循环处理 */
static int process_events(struct ez_ws_server_handle *server, int timeout_ms)
{
	struct epoll_event events[64];
	
	/* 如果 timeout_ms 为 0，使用默认超时 100ms，避免忙等待 */
	/* 如果 timeout_ms 为 -1，表示无限等待（阻塞） */
	if (timeout_ms == 0) {
		timeout_ms = 100;  /* 默认 100ms 超时，平衡响应性和 CPU 占用 */
	}
	
	int nfds = epoll_wait(server->epollfd, events, 64, timeout_ms);
	
	if (nfds < 0) {
		if (errno == EINTR)
			return 0;
		return -1;
	}
	
	for (int i = 0; i < nfds; i++) {
		if (events[i].data.fd == server->listen_sockfd) {
			/* 新连接 - 使用 EPOLLET 边缘触发模式，只需接受一次 */
			/* 如果还有更多连接，epoll 会再次触发事件 */
			accept_new_connection(server);
		} else if (events[i].data.fd == server->timerfd) {
			/* 定时器事件 */
			uint64_t expirations;
			read(server->timerfd, &expirations, sizeof(expirations));
			process_timer_events(server);
		} else {
			/* 客户端连接事件 */
			struct client_connection *client = (struct client_connection *)events[i].data.ptr;
			
			if (events[i].events & (EPOLLERR | EPOLLHUP)) {
				/* 连接错误或关闭 */
				remove_client(server, client);
				continue;
			}
			
			if (events[i].events & EPOLLOUT) {
				/* 可写，处理发送队列 */
				if (client->state == CLIENT_STATE_CONNECTED) {
					process_send_queue(client);
				}
			}
			
			if (events[i].events & EPOLLIN) {
				/* 可读 - 只负责接收数据到缓冲区 */
				ssize_t n;
				
				if (client->state == CLIENT_STATE_HANDSHAKING) {
					/* 接收握手请求数据 */
					n = recv(client->sockfd,
					        client->handshake_request + client->handshake_request_len,
					        sizeof(client->handshake_request) - client->handshake_request_len - 1,
					        0);
					if (n < 0) {
						if (errno == EAGAIN || errno == EWOULDBLOCK)
							continue; /* 继续等待 */
						remove_client(server, client);
						continue;
					}
					if (n == 0) {
						/* 连接关闭 */
						remove_client(server, client);
						continue;
					}
					
					/* 检查缓冲区是否溢出 */
					if (client->handshake_request_len + n >= EZ_WS_HANDSHAKE_REQUEST_BUFFER_SIZE) {
						// WHF todo
						fprintf(stderr, "[ERROR] Handshake request buffer overflow: received %zu bytes (current: %zu + new: %zd), "
						        "buffer size is %d bytes. Client #%d from %s:%d. "
						        "Consider increasing EZ_WS_HANDSHAKE_REQUEST_BUFFER_SIZE.\n",
						        client->handshake_request_len + n, client->handshake_request_len, n,
						        EZ_WS_HANDSHAKE_REQUEST_BUFFER_SIZE,
						        client->client_info.id, client->client_info.ip, client->client_info.port);
						remove_client(server, client);
						continue;
					}
					
					/* 更新接收时间 */
					uint64_t now_ms = ez_ws_server_now_ms();
					client->last_rx_time_ms = now_ms;
					
					/* 将接收到的数据添加到缓冲区 */
					client->handshake_request_len += n;
					client->handshake_request[client->handshake_request_len] = '\0';
					
					/* 处理握手数据（只处理缓冲区中的数据） */
					int ret = handle_handshake(server, client);
					if (ret < 0) {
						remove_client(server, client);
						continue;
					}
				} else if (client->state == CLIENT_STATE_CONNECTED) {
					/* 接收 WebSocket 数据 */
					/* 检查缓冲区空间是否足够 */
					if (client->recv_buffer_len >= client->recv_buffer_size) {
						/* 缓冲区已满，尝试扩展 */
						if (client->recv_buffer_size >= EZ_WS_RECV_BUFFER_MAX_SIZE) {
							/* 已达到最大限制，关闭连接 */
							fprintf(stderr, "[ERROR] Recv buffer reached maximum size (%d bytes). "
							        "Client #%d from %s:%d. Cannot receive more data.\n",
							        EZ_WS_RECV_BUFFER_MAX_SIZE,
							        client->client_info.id, client->client_info.ip, client->client_info.port);
							remove_client(server, client);
							continue;
						}
						
						/* 扩展缓冲区（每次扩展一倍，但不超过最大限制） */
						size_t new_size = client->recv_buffer_size * 2;
						if (new_size > EZ_WS_RECV_BUFFER_MAX_SIZE) {
							new_size = EZ_WS_RECV_BUFFER_MAX_SIZE;
						}
						
						uint8_t *new_buffer = realloc(client->recv_buffer, new_size);
						if (!new_buffer) {
							fprintf(stderr, "[ERROR] Failed to expand recv buffer from %zu to %zu bytes. "
							        "Client #%d from %s:%d.\n",
							        client->recv_buffer_size, new_size,
							        client->client_info.id, client->client_info.ip, client->client_info.port);
							remove_client(server, client);
							continue;
						}
						
						client->recv_buffer = new_buffer;
						client->recv_buffer_size = new_size;
						
						fprintf(stderr, "[INFO] Expanded recv buffer from %zu to %zu bytes for client #%d from %s:%d.\n",
						        client->recv_buffer_size / 2, client->recv_buffer_size,
						        client->client_info.id, client->client_info.ip, client->client_info.port);
					}
					
					n = recv(client->sockfd,
					        client->recv_buffer + client->recv_buffer_len,
					        client->recv_buffer_size - client->recv_buffer_len,
					        0);
					if (n < 0) {
						if (errno == EAGAIN || errno == EWOULDBLOCK)
							continue; /* 继续等待 */
						remove_client(server, client);
						continue;
					}
					if (n == 0) {
						/* 连接关闭 */
						remove_client(server, client);
						continue;
					}
					
					/* 更新接收时间 */
					uint64_t now_ms = ez_ws_server_now_ms();
					client->last_rx_time_ms = now_ms;
					client->client_info.last_activity_ms = now_ms;
					
					/* 将接收到的数据添加到缓冲区（TCP 流式特性：数据可能分片到达） */
					client->recv_buffer_len += n;
					
					/* 处理 WebSocket 数据（只处理缓冲区中的数据） */
					/* 循环处理，直到没有完整帧可解析 */
					while (client->recv_buffer_len > 0) {
						size_t old_buffer_len = client->recv_buffer_len;
						int ret = handle_websocket_data(server, client);
						if (ret < 0) {
							remove_client(server, client);
							break;
						}
						/* 如果缓冲区长度没有变化，说明没有解析任何数据，数据不完整，等待更多数据 */
						if (client->recv_buffer_len == old_buffer_len) {
							break;
						}
					}
				}
			}
		}
	}
	
	return 0;
}

/* ================== 公共 API 实现 ================== */

/* 创建 WebSocket 服务端句柄 */
struct ez_ws_server_handle *ez_ws_server_handle_create(struct ez_ws_server_config *config,
                                                        struct ez_ws_server_callbacks *callbacks)
{
	struct ez_ws_server_handle *server = calloc(1, sizeof(struct ez_ws_server_handle));
	if (!server)
		return NULL;
	
	/* 复制配置 */
	if (config) {
		memcpy(&server->config, config, sizeof(struct ez_ws_server_config));
	} else {
		memset(&server->config, 0, sizeof(struct ez_ws_server_config));
		server->config.port = 54321;
	}
	
	/* 检查必需配置 */
	if (!server->config.protocol) {
		fprintf(stderr, "Protocol must be specified\n");
		free(server);
		return NULL;
	}
	
	/* 复制回调函数 */
	if (callbacks) {
		memcpy(&server->callbacks, callbacks, sizeof(struct ez_ws_server_callbacks));
	} else {
		memset(&server->callbacks, 0, sizeof(struct ez_ws_server_callbacks));
	}
	
	/* 初始化客户端列表锁 */
	pthread_mutex_init(&server->client_list_lock, NULL);
	server->client_list = NULL;
	server->client_count = 0;
	server->next_client_id = 1;
	
	/* 创建监听 socket */
	server->listen_sockfd = create_listen_socket(server->config.port);
	if (server->listen_sockfd < 0) {
		pthread_mutex_destroy(&server->client_list_lock);
		free(server);
		return NULL;
	}
	
	/* 创建 epoll */
	server->epollfd = epoll_create1(EPOLL_CLOEXEC);
	if (server->epollfd < 0) {
		close(server->listen_sockfd);
		pthread_mutex_destroy(&server->client_list_lock);
		free(server);
		return NULL;
	}
	
	/* 将监听 socket 添加到 epoll */
	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.fd = server->listen_sockfd;
	if (epoll_ctl(server->epollfd, EPOLL_CTL_ADD, server->listen_sockfd, &ev) < 0) {
		close(server->epollfd);
		close(server->listen_sockfd);
		pthread_mutex_destroy(&server->client_list_lock);
		free(server);
		return NULL;
	}
	
	/* 创建定时器（如果配置了） */
	if (server->config.timer_interval_ms > 0) {
		server->timerfd = create_timerfd(server->config.timer_interval_ms);
		if (server->timerfd >= 0) {
			ev.events = EPOLLIN;
			ev.data.fd = server->timerfd;
			epoll_ctl(server->epollfd, EPOLL_CTL_ADD, server->timerfd, &ev);
		}
	} else {
		server->timerfd = -1;
	}
	
	/* 设置 HTTP 解析器回调（为所有客户端准备） */
	/* 注意：每个客户端连接时会初始化自己的解析器 */
	
	server->ready = 1;
	
	return server;
}

/* 清理 WebSocket 服务端 */
void ez_ws_server_cleanup(struct ez_ws_server_handle *server)
{
	if (!server)
		return;
	
	server->ready = 0;
	server->interrupted = 1;
	if (server->interrupted_ptr)
		*server->interrupted_ptr = 1;
	
	/* 关闭所有客户端连接 */
	pthread_mutex_lock(&server->client_list_lock);
	while (server->client_list) {
		struct client_connection *client = server->client_list;
		server->client_list = client->next;
		pthread_mutex_unlock(&server->client_list_lock);
		remove_client(server, client);
		pthread_mutex_lock(&server->client_list_lock);
	}
	pthread_mutex_unlock(&server->client_list_lock);
	
	/* 关闭定时器 */
	if (server->timerfd >= 0) {
		close(server->timerfd);
		server->timerfd = -1;
	}
	
	/* 关闭 epoll */
	if (server->epollfd >= 0) {
		close(server->epollfd);
		server->epollfd = -1;
	}
	
	/* 关闭监听 socket */
	if (server->listen_sockfd >= 0) {
		close(server->listen_sockfd);
		server->listen_sockfd = -1;
	}
	
	/* 销毁锁 */
	pthread_mutex_destroy(&server->client_list_lock);
	
	/* 释放服务器结构 */
	free(server);
}

/* WebSocket 服务执行函数 */
int ez_ws_server_service_exec(struct ez_ws_server_handle *ws, int timeout_ms)
{
	if (!ws || !ws->ready)
		return -1;
	
	if (ws->interrupted || (ws->interrupted_ptr && *ws->interrupted_ptr))
		return -1;
	
	/* 处理事件 */
	if (process_events(ws, timeout_ms) < 0)
		return -1;
	
	return 0;
}

/* 发送文本消息 */
int ez_ws_server_send_text(struct ez_ws_server_handle *ws, int client_id, const char *data, size_t len)
{
	if (!ws || !data)
		return EZ_WS_SERVER_ERR_INVALID_PARAM;
	
	/* 计算长度 */
	if (len == 0)
		len = strlen(data);
	
	/* 广播到所有客户端 */
	if (client_id == -1) {
		pthread_mutex_lock(&ws->client_list_lock);
		struct client_connection *client = ws->client_list;
		while (client) {
			if (client->state == CLIENT_STATE_CONNECTED) {
				send_to_client_internal(client, (const uint8_t *)data, len, EZ_WS_OPCODE_TEXT);
			}
			client = client->next;
		}
		pthread_mutex_unlock(&ws->client_list_lock);
		return EZ_WS_SERVER_OK;
	}
	
	/* 发送到指定客户端 */
	struct client_connection *client = find_client(ws, client_id);
	if (!client)
		return EZ_WS_SERVER_ERR_CLIENT_NOT_FOUND;
	
	if (send_to_client_internal(client, (const uint8_t *)data, len, EZ_WS_OPCODE_TEXT) < 0)
		return EZ_WS_SERVER_ERR_QUEUE_FULL;
	
	return EZ_WS_SERVER_OK;
}

/* 发送二进制消息 */
int ez_ws_server_send_binary(struct ez_ws_server_handle *ws, int client_id, const void *data, size_t len)
{
	if (!ws || !data || len == 0)
		return EZ_WS_SERVER_ERR_INVALID_PARAM;
	
	/* 广播到所有客户端 */
	if (client_id == -1) {
		pthread_mutex_lock(&ws->client_list_lock);
		struct client_connection *client = ws->client_list;
		while (client) {
			if (client->state == CLIENT_STATE_CONNECTED) {
				send_to_client_internal(client, (const uint8_t *)data, len, EZ_WS_OPCODE_BINARY);
			}
			client = client->next;
		}
		pthread_mutex_unlock(&ws->client_list_lock);
		return EZ_WS_SERVER_OK;
	}
	
	/* 发送到指定客户端 */
	struct client_connection *client = find_client(ws, client_id);
	if (!client)
		return EZ_WS_SERVER_ERR_CLIENT_NOT_FOUND;
	
	if (send_to_client_internal(client, (const uint8_t *)data, len, EZ_WS_OPCODE_BINARY) < 0)
		return EZ_WS_SERVER_ERR_QUEUE_FULL;
	
	return EZ_WS_SERVER_OK;
}

/* 获取客户端数量 */
int ez_ws_server_get_client_count(struct ez_ws_server_handle *ws)
{
	if (!ws)
		return -1;
	
	int count;
	pthread_mutex_lock(&ws->client_list_lock);
	count = ws->client_count;
	pthread_mutex_unlock(&ws->client_list_lock);
	
	return count;
}

/* 检查服务是否运行 */
int ez_ws_server_is_ready(struct ez_ws_server_handle *ws)
{
	if (!ws)
		return 0;
	
	return ws->ready ? 1 : 0;
}

/* 遍历客户端 */
int ez_ws_server_foreach_client(struct ez_ws_server_handle *ws,
                                 ez_ws_server_foreach_client_cb callback,
                                 void *user_data)
{
	if (!ws || !callback)
		return EZ_WS_SERVER_ERR_INVALID_PARAM;
	
	pthread_mutex_lock(&ws->client_list_lock);
	struct client_connection *client = ws->client_list;
	
	while (client) {
		struct ez_ws_client_info info;
		/* 使用 memcpy 直接复制整个 client_info 结构体（性能更高） */
		memcpy(&info, &client->client_info, sizeof(info));
		
		pthread_mutex_unlock(&ws->client_list_lock);
		int ret = callback(&info, user_data);
		pthread_mutex_lock(&ws->client_list_lock);
		
		if (ret != 0) {
			/* 回调返回非0，停止遍历 */
			break;
		}
		
		client = client->next;
	}
	
	pthread_mutex_unlock(&ws->client_list_lock);
	
	return EZ_WS_SERVER_OK;
}

