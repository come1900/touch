/*
 * ez_wsclient-native.c
 *
 * 原生 Linux WebSocket 客户端组件实现
 * 使用 epoll、timerfd、socket 等 Linux 基本功能
 * 不依赖 libwebsockets 库
 */

#include "ez_wsclient-native.h"

#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>

#include "ezutil/base64.h"
#include "ezutil/sha1.h"
#include "ezutil/str_opr.h"
#include "ezutil/http_parser.h"

#include "ezutil/ez_websocket_parser.h"
#include "__def_debug.h"

/* ================== 链路保活配置 ================== */
#define EZ_WS_CLIENT_PING_INTERVAL_MS      (30 * 1000)   /* 无业务流量时发送 ping */
#define EZ_WS_CLIENT_PONG_TIMEOUT_MS       (10 * 1000)   /* 等待 pong 最长毫秒数 */
#define EZ_WS_CLIENT_IDLE_TIMEOUT_MS       (120 * 1000)  /* 超过该时间无任何业务流量则判定失效 */
#define EZ_WS_CLIENT_HEARTBEAT_TICK_MS     5000  /* 心跳检测周期 */

/* 默认重连配置 */
#ifndef RECONNECT_INTERVAL_MS
#define RECONNECT_INTERVAL_MS (1*500)
#endif

#ifndef RECONNECT_MAX_RETRIES
#define RECONNECT_MAX_RETRIES 0
#endif

#ifndef RECONNECT_BACKOFF_ENABLE
#define RECONNECT_BACKOFF_ENABLE 1
#endif

#define LOG_DEBUG_ENABLED 0

#if RECONNECT_BACKOFF_ENABLE
#ifndef RECONNECT_BACKOFF_MIN_RETRIES
#define RECONNECT_BACKOFF_MIN_RETRIES 2
#endif

#ifndef RECONNECT_BACKOFF_HIGH_THRESHOLD
#define RECONNECT_BACKOFF_HIGH_THRESHOLD 5
#endif
#endif

/* ez_ws_client_handle_reset 标志位 */
#define EZ_WS_RESET_HTTP       (1u << 0)
#define EZ_WS_RESET_WS         (1u << 1)
#define EZ_WS_RESET_HEARTBEAT  (1u << 2)

/* 安全释放内存宏 */
#define SAFE_FREE(ptr) do { \
	if (ptr) { \
		free(ptr); \
		ptr = NULL; \
	} \
} while (0)

/* 内部结构定义 */

/* WebSocket 帧数据收集器 */
struct ez_ws_frame_data {
	uint8_t *buffer;
	size_t buffer_size;
	size_t buffer_cap;
	int opcode;
	int is_binary;
};

/* 发送消息队列节点 */
struct send_queue_node {
	char *data;
	size_t len;
	int opcode;
	struct send_queue_node *next;
};

/* 发送缓冲区 */
struct send_buffer {
	uint8_t *frame_data;
	size_t frame_len;
	size_t sent_len;
	int is_active;
};

static int ez_ws_send_control_frame(struct ez_ws_client_handle *ws, int opcode,
				 const void *payload, size_t len);

static uint64_t
ez_ws_client_now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

/* WebSocket客户端句柄（完整定义） */
struct ez_ws_client_handle {
	/* 配置 */
	struct ez_ws_client_config config;
	
	/* 连接信息 */
	char *server_addr;
	unsigned short port;
	char *url_path;
	char *protocol;
	
	/* Socket */
	int sockfd;
	int epollfd;
	
	/* 定时器 */
	int reconnect_timerfd;
	int heartbeat_timerfd;
	
	/* 状态 */
	enum ez_ws_state state;
	int interrupted;
	
	/* 重连状态 */
	uint32_t retry_count;
	uint32_t current_retry_delay;
	
	/* 握手相关 */
	char *handshake_key;
	char handshake_response[4096];
	size_t handshake_response_len;
	
	/* HTTP 解析器 */
	http_parser http_parser;
	http_parser_settings http_parser_settings;
	int http_handshake_complete;
	char *http_header_field;
	char *http_header_value;
	char *http_sec_websocket_accept;
	
	/* WebSocket 解析器 */
	websocket_parser ws_parser;
	websocket_parser_settings ws_parser_settings;
	struct ez_ws_frame_data current_frame;
	
	/* 接收缓冲区 */
	uint8_t recv_buffer[65536];
	size_t recv_buffer_len;
	
	/* 发送队列 */
	struct send_queue_node *send_queue_head;
	struct send_queue_node *send_queue_tail;
	pthread_mutex_t send_queue_lock;
	size_t send_queue_size;
	size_t send_queue_max;
	
	/* 发送缓冲区 */
	struct send_buffer pending_send;

	/* 回调 */
	struct ez_ws_callbacks callbacks;
	
	/* 线程 */
	pthread_t ws_thread;
	int ws_running;

	uint64_t last_rx_time_ms;
	uint64_t last_tx_time_ms;
	uint64_t last_activity_time_ms;
	uint64_t last_ping_time_ms;
	int awaiting_pong;
};

/* 生成WebSocket握手密钥 */
static void generate_websocket_key(char *key, size_t key_size) {
	unsigned char random_bytes[16];
	FILE *urandom = fopen("/dev/urandom", "rb");
	if (urandom) {
		fread(random_bytes, 1, 16, urandom);
		fclose(urandom);
	} else {
		/* 如果无法读取随机数，使用时间戳 */
		time_t t = time(NULL);
		memcpy(random_bytes, &t, sizeof(t));
		memcpy(random_bytes + sizeof(t), &t, 16 - sizeof(t));
	}
	
	char base64_key[32] = {0};
	// base64_encode(random_bytes, 16, base64_key);
	ez_base64encode(base64_key, (const char *)random_bytes, 16);
	snprintf(key, key_size, "%s", base64_key);
}

/* 计算WebSocket Accept值 */
static int compute_accept(const char *key, char *accept) {
	char combined[256];
	snprintf(combined, sizeof(combined), "%s%s", key, WEBSOCKET_MAGIC_STRING);
	
	char hash2[SHA1HashSize] = {0};

	SHA1Context sha;
    SHA1Reset(&sha);
	SHA1Input(&sha,		(const unsigned char *) combined,		strlen(combined));
	SHA1Result(&sha, (unsigned char *)hash2);
	
	char base64_accept[32] = {0};
	ez_base64encode(base64_accept, (const char *)hash2, SHA1HashSize);
	snprintf(accept, 32, "%s", base64_accept);
	return 0;
}

/* 创建WebSocket握手请求 */
static int ez_ws_create_handshake_request(struct ez_ws_client_handle *ws, char *buffer, size_t buffer_size) {
	char key[32] = {0};
	generate_websocket_key(key, sizeof(key));
	
	SAFE_FREE(ws->handshake_key);
	ws->handshake_key = strdup(key);
	
	char host[256] = {0};
	snprintf(host, sizeof(host), "%s:%hu", ws->server_addr, ws->port);
	
	return snprintf(buffer, buffer_size,
		"GET %s HTTP/1.1\r\n"
		"Host: %s\r\n"
		"Upgrade: websocket\r\n"
		"Connection: Upgrade\r\n"
		"Sec-WebSocket-Key: %s\r\n"
		"Sec-WebSocket-Version: 13\r\n"
		"Sec-WebSocket-Protocol: %s\r\n"
		"\r\n",
		ws->url_path, host, key, ws->protocol ? ws->protocol : "");
}

/* HTTP 解析器回调：状态行 */
static int on_http_status(http_parser *parser, const char *at, size_t length) {
	return 0;
}

/* HTTP 解析器回调：头部字段 */
static int on_http_header_field(http_parser *parser, const char *at, size_t length) {
	struct ez_ws_client_handle *ws = (struct ez_ws_client_handle *)parser->data;
	
	if (ws->http_header_field)
		free(ws->http_header_field);
	ws->http_header_field = malloc(length + 1);
	if (!ws->http_header_field)
		return -1;
	memcpy(ws->http_header_field, at, length);
	ws->http_header_field[length] = '\0';
	
	return 0;
}

/* HTTP 解析器回调：头部值 */
static int on_http_header_value(http_parser *parser, const char *at, size_t length) {
	struct ez_ws_client_handle *ws = (struct ez_ws_client_handle *)parser->data;
	
	if (ws->http_header_value)
		free(ws->http_header_value);
	ws->http_header_value = malloc(length + 1);
	if (!ws->http_header_value)
		return -1;
	memcpy(ws->http_header_value, at, length);
	ws->http_header_value[length] = '\0';
	
	/* 检查是否是 Sec-WebSocket-Accept */
	if (ws->http_header_field && 
	    strcasecmp(ws->http_header_field, "Sec-WebSocket-Accept") == 0) {
		if (ws->http_sec_websocket_accept)
			free(ws->http_sec_websocket_accept);
		ws->http_sec_websocket_accept = strdup(ws->http_header_value);
	}
	
	return 0;
}

/* HTTP 解析器回调：头部完成 */
static int on_http_headers_complete(http_parser *parser) {
	struct ez_ws_client_handle *ws = (struct ez_ws_client_handle *)parser->data;
	
	/* 检查状态码 */
	if (parser->status_code != 101) {
		LOG_ERROR("WebSocket handshake failed: invalid status code %d\n", parser->status_code);
		return -1;
	}
	
	/* 检查 Upgrade 和 Connection 标志 */
	if (!(parser->flags & F_UPGRADE) || !(parser->flags & F_CONNECTION_UPGRADE)) {
		LOG_ERROR("WebSocket handshake failed: missing Upgrade or Connection: Upgrade header\n");
		return -1;
	}
	
	/* 验证 Sec-WebSocket-Accept */
	if (!ws->http_sec_websocket_accept) {
		LOG_ERROR("WebSocket handshake failed: missing Sec-WebSocket-Accept header\n");
		return -1;
	}
	
	char expected_accept[64];
	compute_accept(ws->handshake_key, expected_accept);
	if (strcmp(ws->http_sec_websocket_accept, expected_accept) != 0) {
		LOG_ERROR("WebSocket handshake failed: invalid Sec-WebSocket-Accept\n");
		LOG_ERROR("  Expected: %s\n", expected_accept);
		LOG_ERROR("  Received: %s\n", ws->http_sec_websocket_accept);
		return -1;
	}
	
	ws->http_handshake_complete = 1;
	return 0;
}

/* 设置socket为非阻塞 */
static int set_nonblocking(int fd) {
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0)
		return -1;
	return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* 创建定时器 */
static int create_timerfd(int interval_ms) {
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

/* 更新定时器 */
static int update_timerfd(int timerfd, int interval_ms) {
	struct itimerspec timer_spec;
	timer_spec.it_value.tv_sec = interval_ms / 1000;
	timer_spec.it_value.tv_nsec = (interval_ms % 1000) * 1000000;
	timer_spec.it_interval.tv_sec = timer_spec.it_value.tv_sec;
	timer_spec.it_interval.tv_nsec = timer_spec.it_value.tv_nsec;
	
	return timerfd_settime(timerfd, 0, &timer_spec, NULL);
}

/* 创建WebSocket帧 */
static int create_ws_frame(const uint8_t *payload, size_t payload_len, 
                          int opcode, int fin, uint8_t *frame, size_t *frame_len) {
	size_t pos = 0;
	
	/* 第一个字节：FIN + RSV + Opcode */
	frame[pos++] = (fin ? 0x80 : 0x00) | (opcode & 0x0F);
	
	/* 第二个字节：MASK + Payload length */
	/* 客户端必须设置 MASK 位 (0x80) */
	if (payload_len < 126) {
		frame[pos++] = 0x80 | payload_len;  /* 客户端必须mask */
	} else if (payload_len < 65536) {
		frame[pos++] = 0x80 | 126;
		frame[pos++] = (payload_len >> 8) & 0xFF;
		frame[pos++] = payload_len & 0xFF;
	} else {
		frame[pos++] = 0x80 | 127;
		int i;
		for (i = 7; i >= 0; i--) {
			frame[pos++] = (payload_len >> (i * 8)) & 0xFF;
		}
	}
	
	/* 生成masking key */
	uint8_t masking_key[4];
	FILE *urandom = fopen("/dev/urandom", "rb");
	if (urandom) {
		fread(masking_key, 1, 4, urandom);
		fclose(urandom);
	} else {
		time_t t = time(NULL);
		memcpy(masking_key, &t, 4);
	}
	
	memcpy(frame + pos, masking_key, 4);
	pos += 4;
	
	/* 应用mask并复制payload */
	for (size_t i = 0; i < payload_len; i++) {
		frame[pos + i] = payload[i] ^ masking_key[i % 4];
	}
	pos += payload_len;
	
	*frame_len = pos;
	return 0;
}

/* WebSocket 解析器回调：帧开始 */
static int on_ws_frame_begin(websocket_parser *parser) {
	struct ez_ws_client_handle *ws = (struct ez_ws_client_handle *)parser->data;
	
	/* 调试：打印帧开始信息 */
	LOG_DEBUG("on_ws_frame_begin: opcode=%d, fin=%d, has_mask=%d\n",
	          parser->opcode, parser->fin, parser->has_mask);
	
	/* 清理上一个帧的缓冲区（如果存在） */
	SAFE_FREE(ws->current_frame.buffer);
	
	/* 初始化帧数据收集器 */
	ws->current_frame.opcode = parser->opcode;
	ws->current_frame.is_binary = (parser->opcode == WS_OPCODE_BINARY);
	ws->current_frame.buffer_size = 0;
	ws->current_frame.buffer_cap = 65536;
	ws->current_frame.buffer = malloc(ws->current_frame.buffer_cap);
	if (!ws->current_frame.buffer) {
		return -1;
	}
	
	/* 确保缓冲区初始化为0，避免旧数据残留 */
	memset(ws->current_frame.buffer, 0, ws->current_frame.buffer_cap);
	
	return 0;
}

/* WebSocket 解析器回调：帧负载 */
static int on_ws_frame_payload(websocket_parser *parser, const char *at, size_t length) {
	struct ez_ws_client_handle *ws = (struct ez_ws_client_handle *)parser->data;
	
	/* 调试：打印接收到的原始数据 */
	if (LOG_DEBUG_ENABLED)
	{
		char debug_buf[256];
		size_t show_len = length < sizeof(debug_buf) - 1 ? length : sizeof(debug_buf) - 1;
		memcpy(debug_buf, at, show_len);
		debug_buf[show_len] = '\0';
		LOG_DEBUG("on_ws_frame_payload: opcode=%d, has_mask=%d, length=%zu, data=%s\n", 
		          parser->opcode, parser->has_mask, length, debug_buf);
	}
	
	/* 扩展缓冲区如果需要 */
	if (ws->current_frame.buffer_size + length > ws->current_frame.buffer_cap) {
		size_t new_cap = ws->current_frame.buffer_cap * 2;
		while (new_cap < ws->current_frame.buffer_size + length)
			new_cap *= 2;
		uint8_t *new_buf = realloc(ws->current_frame.buffer, new_cap);
		if (!new_buf)
			return -1;
		ws->current_frame.buffer = new_buf;
		ws->current_frame.buffer_cap = new_cap;
	}
	
	/* 复制数据（已经应用了 mask） */
	memcpy(ws->current_frame.buffer + ws->current_frame.buffer_size, at, length);
	ws->current_frame.buffer_size += length;
	
	return 0;
}

/* WebSocket 解析器回调：帧完成 */
static int on_ws_frame_complete(websocket_parser *parser) {
	struct ez_ws_client_handle *ws = (struct ez_ws_client_handle *)parser->data;
	
	/* 处理不同类型的帧 */
	switch (ws->current_frame.opcode) {
	case WS_OPCODE_TEXT:
	case WS_OPCODE_BINARY:
		{
			LOG_DEBUG("on_ws_frame_complete: opcode=%d, buffer_size=%zu\n", 
			          ws->current_frame.opcode, ws->current_frame.buffer_size);
			size_t show = ws->current_frame.buffer_size > 512 ? 512 : ws->current_frame.buffer_size;
			char buf[513];
			if (show)
				memcpy(buf, ws->current_frame.buffer, show);
			buf[show] = '\0';
			LOG_DEBUG("Buffer content before callback: %s\n", buf);
			LOG_INFO("RECV[%zu]: %s%s\n", ws->current_frame.buffer_size, buf,
			         ws->current_frame.buffer_size > show ? "..." : "");
		}
		
		/* 调用接收回调 */
		if (ws->callbacks.on_receive && ws->current_frame.buffer_size > 0) {
			/* 复制数据，确保回调函数安全使用 */
			uint8_t *data_copy = malloc(ws->current_frame.buffer_size);
			if (data_copy) {
				memcpy(data_copy, ws->current_frame.buffer, ws->current_frame.buffer_size);
				ws->callbacks.on_receive(
					data_copy,
					ws->current_frame.buffer_size,
					ws->current_frame.is_binary,
					ws->callbacks.user_data
				);
				free(data_copy);
			} else {
				/* 如果分配失败，直接使用原缓冲区（回调是同步的） */
				ws->callbacks.on_receive(
					ws->current_frame.buffer,
					ws->current_frame.buffer_size,
					ws->current_frame.is_binary,
					ws->callbacks.user_data
				);
			}
		}
		break;
		
	case WS_OPCODE_CLOSE:
		LOG_INFO("=== Connection closed by server ===\n");
		/* 关闭帧是合法的，不需要返回错误 */
		/* 解析器会通过 close_received 标志来处理 */
		break;
		
	case WS_OPCODE_PING:
		/* 发送 Pong 响应 */
		{
			if (ez_ws_send_control_frame(ws, WS_OPCODE_PONG,
						  ws->current_frame.buffer,
						  ws->current_frame.buffer_size) < 0) {
				LOG_WARN("Failed to reply pong, closing connection soon if problem persists\n");
			}
		}
		break;
		
	case WS_OPCODE_PONG:
		/* 心跳响应 */
		ws->awaiting_pong = 0;
		{
			uint64_t now_ms = ez_ws_client_now_ms();
			ws->last_rx_time_ms = now_ms;
			ws->last_activity_time_ms = now_ms;
		}
		break;
		
	default:
		break;
	}
	
	/* 清理帧数据 */
	SAFE_FREE(ws->current_frame.buffer);
	ws->current_frame.buffer_size = 0;
	
	return 0;
}

/* 连接到服务器 */
static int ez_ws_connect(struct ez_ws_client_handle *ws) {
	/* 如果已中断，不尝试连接 */
	if (ws->interrupted || !ws->ws_running) {
		return -1;
	}
	
	struct addrinfo hints, *result, *rp;
	char port_str[16];
	
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	
	snprintf(port_str, sizeof(port_str), "%hu", ws->port);
	
	int ret = getaddrinfo(ws->server_addr, port_str, &hints, &result);
	if (ret != 0) {
		/* 在关闭时，不打印错误日志 */
		if (!ws->interrupted) {
			LOG_ERROR("getaddrinfo failed: %s\n", gai_strerror(ret));
		}
		return -1;
	}
	
	for (rp = result; rp != NULL; rp = rp->ai_next) {
		/* 再次检查中断标志 */
		if (ws->interrupted || !ws->ws_running) {
			freeaddrinfo(result);
			return -1;
		}
		
		ws->sockfd = socket(rp->ai_family,
		                    rp->ai_socktype,
		                    rp->ai_protocol);
		if (ws->sockfd < 0)
			continue;
		
		if (connect(ws->sockfd, rp->ai_addr, rp->ai_addrlen) == 0)
			break;
		
		close(ws->sockfd);
		ws->sockfd = -1;
	}
	
	freeaddrinfo(result);
	
	if (ws->sockfd < 0) {
		/* 在关闭时，不打印错误日志 */
		if (!ws->interrupted) {
			LOG_WARN("Failed to connect to %s:%d\n", ws->server_addr, ws->port);
		}
		return -1;
	}
	
	/* 设置为非阻塞 */
	set_nonblocking(ws->sockfd);
	
	/* 重置接收缓冲区和解析器 */
	ws->recv_buffer_len = 0;
	websocket_parser_init(&ws->ws_parser);
	ws->ws_parser.data = ws;
	SAFE_FREE(ws->current_frame.buffer);
	ws->current_frame.buffer_size = 0;
	
	/* 添加到epoll */
	struct epoll_event ev;
	ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
	ev.data.fd = ws->sockfd;
	if (epoll_ctl(ws->epollfd, EPOLL_CTL_ADD, ws->sockfd, &ev) < 0) {
		close(ws->sockfd);
		ws->sockfd = -1;
		return -1;
	}
	
	ws->state = EZ_WS_STATE_HANDSHAKING;
	
	/* 发送握手请求 */
	char handshake[1024];
	int handshake_len = ez_ws_create_handshake_request(ws, handshake, sizeof(handshake));
	if (send(ws->sockfd, handshake, handshake_len, 0) < 0) {
		LOG_ERROR("Failed to send handshake\n");
		return -1;
	}
	
	LOG_INFO("Connecting to %s:%d%s\n", ws->server_addr, ws->port, ws->url_path);
	return 0;
}

/* 处理握手响应 */
static int ez_ws_handle_handshake(struct ez_ws_client_handle *ws) {
	/* 检查缓冲区是否已满 */
	if (ws->handshake_response_len >= sizeof(ws->handshake_response) - 1) {
		LOG_ERROR("WebSocket handshake failed: response buffer overflow\n");
		return -1;
	}
	
	ssize_t n = recv(ws->sockfd, ws->handshake_response + ws->handshake_response_len,
	                sizeof(ws->handshake_response) - ws->handshake_response_len - 1, 0);
	if (n < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return 0; /* 继续等待 */
		LOG_ERROR("WebSocket handshake failed: recv error: %s\n", strerror(errno));
		return -1;
	}
	
	if (n == 0) {
		LOG_ERROR("WebSocket handshake failed: connection closed by server\n");
		return -1;
	}
	
	/* 使用 http_parser 解析新接收的数据 */
	size_t old_len = ws->handshake_response_len;
	ws->handshake_response_len += n;
	ws->handshake_response[ws->handshake_response_len] = '\0';
	
	/* 只解析新接收的数据部分 */
	(void)http_parser_execute(&ws->http_parser, &ws->http_parser_settings,
	                         ws->handshake_response + old_len,
	                         n);
	
	if (HTTP_PARSER_ERRNO(&ws->http_parser) != HPE_OK) {
		LOG_ERROR("WebSocket handshake failed: HTTP parse error: %s\n",
		          http_errno_description(HTTP_PARSER_ERRNO(&ws->http_parser)));
		return -1;
	}
	
	/* 检查握手是否完成 */
	if (ws->http_handshake_complete) {
			ws->state = EZ_WS_STATE_CONNECTED;
			ws->retry_count = 0;
			ws->current_retry_delay = ws->config.reconnect_interval_ms;
			ws->awaiting_pong = 0;
			{
				uint64_t now_ms = ez_ws_client_now_ms();
				ws->last_rx_time_ms = now_ms;
				ws->last_tx_time_ms = now_ms;
				ws->last_activity_time_ms = now_ms;
				ws->last_ping_time_ms = now_ms;
			}
			
		LOG_INFO("=== Connection established ===\n");
			
			if (ws->callbacks.on_connected)
				ws->callbacks.on_connected(ws->callbacks.user_data);
			
			/* 启动心跳定时器 */
			if (ws->heartbeat_timerfd >= 0) {
				update_timerfd(ws->heartbeat_timerfd, EZ_WS_CLIENT_HEARTBEAT_TICK_MS);
			}
		
		/* 清空握手响应缓冲区 */
		ws->handshake_response_len = 0;
			
			return 1; /* 握手完成 */
	}
	
	/* 继续等待更多数据 */
	return 0;
}

/* 处理WebSocket数据 */
static int ez_ws_handle_data(struct ez_ws_client_handle *ws) {
	ssize_t n = recv(ws->sockfd, ws->recv_buffer + ws->recv_buffer_len,
	                sizeof(ws->recv_buffer) - ws->recv_buffer_len, 0);
	if (n < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return 0;
		return -1;
	}
	if (n == 0)
		return -1; /* 连接关闭 */
	
	ws->recv_buffer_len += n;
	{
		uint64_t now_ms = ez_ws_client_now_ms();
		ws->last_rx_time_ms = now_ms;
		ws->last_activity_time_ms = now_ms;
	}
	
	/* 使用 websocket_parser 解析数据（从缓冲区开始解析） */
	size_t parsed = websocket_parser_execute(&ws->ws_parser, &ws->ws_parser_settings,
	                                        (const char *)ws->recv_buffer,
	                                        ws->recv_buffer_len);
	
	/* 检查是否收到关闭帧 */
	if (ws->ws_parser.close_received) {
		/* 收到关闭帧，正常关闭连接 */
		return -1;
	}
	
	if (WEBSOCKET_PARSER_ERRNO(&ws->ws_parser) != WSE_OK) {
		LOG_ERROR("WebSocket parse error: %s\n",
		          websocket_errno_description(WEBSOCKET_PARSER_ERRNO(&ws->ws_parser)));
			return -1;
		}
		
		/* 移除已处理的数据 */
	if (parsed > 0 && parsed <= ws->recv_buffer_len) {
		if (parsed < ws->recv_buffer_len) {
		memmove(ws->recv_buffer, ws->recv_buffer + parsed, ws->recv_buffer_len - parsed);
		}
		ws->recv_buffer_len -= parsed;
	}
	
	return 0;
}

/**
 * ez_ws_send_internal - 发送 WebSocket 帧的内部实现
 *
 * 该函数负责将已经编码为 WebSocket 帧的数据写入 socket，
 * 并处理内核发送缓冲区不足时的部分发送、重试等细节。
 *
 * @ws:      WebSocket 客户端句柄，必须已经处于已连接状态
 * @data:    待发送的应用层负载数据指针
 * @len:     数据长度（字节数）；若为 0 则由调用方确保意义
 * @binary:  非零表示二进制帧，0 表示文本帧
 *
 * Return: EZ_WS_OK 表示发送完成；
 *         EZ_WS_ERR_QUEUE_FULL 表示需要等待 EPOLLOUT 继续发送；
 *         其他负值为错误码（如未连接、内存不足等）。
 */
static int ez_ws_send_internal(struct ez_ws_client_handle *ws, const void *data, size_t len, int opcode) {
	if (ws->state != EZ_WS_STATE_CONNECTED)
		return EZ_WS_ERR_NOT_CONNECTED;
	
	/* 如果已有待发送的数据，先尝试发送它 */
	if (ws->pending_send.is_active) {
		ssize_t sent = send(ws->sockfd, 
		                    ws->pending_send.frame_data + ws->pending_send.sent_len,
		                    ws->pending_send.frame_len - ws->pending_send.sent_len, 0);
		if (sent < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				/* 发送缓冲区满，等待 EPOLLOUT 事件 */
				return EZ_WS_ERR_QUEUE_FULL;
			}
			/* 发送错误，清理缓冲区 */
			free(ws->pending_send.frame_data);
			ws->pending_send.frame_data = NULL;
			ws->pending_send.is_active = 0;
			return -1;
		}
		
		if (sent > 0) {
			uint64_t now_ms = ez_ws_client_now_ms();
			ws->last_tx_time_ms = now_ms;
			ws->last_activity_time_ms = now_ms;
		}
		ws->pending_send.sent_len += sent;
		
		/* 检查是否全部发送完成 */
		if (ws->pending_send.sent_len >= ws->pending_send.frame_len) {
			/* 发送完成，清理缓冲区 */
			free(ws->pending_send.frame_data);
			ws->pending_send.frame_data = NULL;
			ws->pending_send.is_active = 0;
			ws->pending_send.sent_len = 0;
			ws->pending_send.frame_len = 0;
		} else {
			/* 部分发送，等待下次 EPOLLOUT */
			return EZ_WS_ERR_QUEUE_FULL;
		}
	}
	
	/* 创建新的帧（按需分配缓冲） */
	size_t frame_cap = len + WS_MAX_FRAME_OVERHEAD;
	uint8_t *frame = malloc(frame_cap);
	size_t frame_len;
	
	if (!frame)
		return EZ_WS_ERR_NO_MEMORY;
	
	if (create_ws_frame((const uint8_t *)data, len, opcode, 1, frame, &frame_len) < 0) {
		free(frame);
		return EZ_WS_ERR_NO_MEMORY;
	}
	
	/* 尝试发送新帧 */
	ssize_t sent = send(ws->sockfd, frame, frame_len, 0);
	if (sent < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			/* 发送缓冲区满，保存到发送缓冲区 */
			ws->pending_send.frame_data = malloc(frame_len);
			if (!ws->pending_send.frame_data) {
				free(frame);
				return EZ_WS_ERR_NO_MEMORY;
			}
			memcpy(ws->pending_send.frame_data, frame, frame_len);
			ws->pending_send.frame_len = frame_len;
			ws->pending_send.sent_len = 0;
			ws->pending_send.is_active = 1;
			free(frame);
			return EZ_WS_ERR_QUEUE_FULL;
		}
		free(frame);
		return -1;
	}
	
	if (sent > 0) {
		uint64_t now_ms = ez_ws_client_now_ms();
		ws->last_tx_time_ms = now_ms;
		ws->last_activity_time_ms = now_ms;
	}

	/* 检查是否全部发送完成 */
	if (sent < (ssize_t)frame_len) {
		/* 部分发送，保存剩余数据到发送缓冲区 */
		ws->pending_send.frame_data = malloc(frame_len - sent);
		if (!ws->pending_send.frame_data) {
			free(frame);
			return EZ_WS_ERR_NO_MEMORY;
		}
		memcpy(ws->pending_send.frame_data, frame + sent, frame_len - sent);
		ws->pending_send.frame_len = frame_len - sent;
		ws->pending_send.sent_len = 0;
		ws->pending_send.is_active = 1;
		free(frame);
		return EZ_WS_ERR_QUEUE_FULL;
	}
	
	free(frame);
	
	/* 全部发送完成 */
	if (ws->callbacks.on_sent)
		ws->callbacks.on_sent(data, len, ws->callbacks.user_data);
	
	{
		size_t show = len > 512 ? 512 : len;
		char buf[513];
		if (show)
			memcpy(buf, data, show);
		buf[show] = '\0';
		LOG_INFO("SEND[%zu]: %s%s\n", len, buf, len > show ? "..." : "");
	}
	
	return EZ_WS_OK;
}

/* 发送控制帧（例如Ping） */
static int ez_ws_send_control_frame(struct ez_ws_client_handle *ws, int opcode,
				 const void *payload, size_t len)
{
	if (!ws || ws->state != EZ_WS_STATE_CONNECTED)
		return -1;
	if (len && !payload)
		return -1;

	size_t frame_cap = len + WS_MAX_FRAME_OVERHEAD;
	uint8_t stack_buf[WS_MAX_FRAME_OVERHEAD + 128];
	uint8_t *frame = stack_buf;

	if (frame_cap > sizeof(stack_buf)) {
		frame = malloc(frame_cap);
		if (!frame)
			return -1;
	}

	size_t frame_len = 0;
	if (create_ws_frame(payload ? (const uint8_t *)payload : NULL, len, opcode,
	                    1, frame, &frame_len) < 0) {
		if (frame != stack_buf)
			free(frame);
		return -1;
	}

	ssize_t sent = send(ws->sockfd, frame, frame_len, 0);
	if (frame != stack_buf)
		free(frame);

	if (sent < 0)
		return -1;
	if (sent != (ssize_t)frame_len)
		return -1;

	uint64_t now_ms = ez_ws_client_now_ms();
	ws->last_tx_time_ms = now_ms;
	ws->last_activity_time_ms = now_ms;

	return 0;
}

static void ez_ws_schedule_reconnect(struct ez_ws_client_handle *ws)
{
	if (!ws)
		return;
	if (ws->reconnect_timerfd >= 0 && !ws->interrupted) {
		ws->retry_count++;
		if (ws->config.reconnect_backoff_enable &&
		    ws->retry_count > ws->config.reconnect_backoff_min_retries &&
		    ws->retry_count <= ws->config.reconnect_backoff_high_threshold) {
			ws->current_retry_delay *= 2;
		}
		update_timerfd(ws->reconnect_timerfd, ws->current_retry_delay);
	}
}

/* 清理发送缓冲区 */
static void ez_ws_clear_send_buffer(struct ez_ws_client_handle *ws) {
	SAFE_FREE(ws->pending_send.frame_data);
	ws->pending_send.frame_len = 0;
	ws->pending_send.sent_len = 0;
	ws->pending_send.is_active = 0;
}

static void ez_ws_client_handle_reset(struct ez_ws_client_handle *ws, unsigned int reset_flags) {
	if (!ws)
		return;
	
	if (ws->sockfd >= 0) {
		epoll_ctl(ws->epollfd, EPOLL_CTL_DEL, ws->sockfd, NULL);
		close(ws->sockfd);
		ws->sockfd = -1;
	}
	
	ws->state = EZ_WS_STATE_DISCONNECTED;
	ez_ws_clear_send_buffer(ws);
	ws->awaiting_pong = 0;
	ws->last_rx_time_ms = 0;
	ws->last_tx_time_ms = 0;
	ws->last_activity_time_ms = 0;
	ws->last_ping_time_ms = 0;
	
	if (reset_flags & EZ_WS_RESET_HTTP) {
		ws->handshake_response_len = 0;
		ws->http_handshake_complete = 0;
		
		http_parser_init(&ws->http_parser, HTTP_RESPONSE);
		ws->http_parser.data = ws;
		
		SAFE_FREE(ws->http_header_field);
		SAFE_FREE(ws->http_header_value);
		SAFE_FREE(ws->http_sec_websocket_accept);
	}
	
	if (reset_flags & EZ_WS_RESET_WS) {
		ws->recv_buffer_len = 0;
		websocket_parser_init(&ws->ws_parser);
		ws->ws_parser.data = ws;
		
		SAFE_FREE(ws->current_frame.buffer);
		ws->current_frame.buffer_size = 0;
		ws->current_frame.buffer_cap = 0;
	}
	
	if ((reset_flags & EZ_WS_RESET_HEARTBEAT) && ws->heartbeat_timerfd >= 0) {
		struct itimerspec timer_spec = {0};
		timerfd_settime(ws->heartbeat_timerfd, 0, &timer_spec, NULL);
	}
	
	if (ws->callbacks.on_disconnected)
		ws->callbacks.on_disconnected(ws->callbacks.user_data);
}

/* 发送队列中的消息
 * 返回值：0 表示成功或队列为空，-1 表示连接错误（需要重连）
 */
static int ez_ws_process_send_queue(struct ez_ws_client_handle *ws) {
	pthread_mutex_lock(&ws->send_queue_lock);
	
	while (ws->send_queue_head) {
		struct send_queue_node *node = ws->send_queue_head;
		
		int ret = ez_ws_send_internal(ws, node->data, node->len, node->opcode);
		if (ret == EZ_WS_ERR_QUEUE_FULL) {
			/* 发送缓冲区满，保留在队列中，等待 EPOLLOUT */
			pthread_mutex_unlock(&ws->send_queue_lock);
			return 0;
		} else if (ret != EZ_WS_OK) {
			/* 连接错误（如 EPIPE, ECONNRESET），需要重连 */
			pthread_mutex_unlock(&ws->send_queue_lock);
			return -1;
		}
		
		/* 移除节点 */
		ws->send_queue_head = node->next;
		if (!ws->send_queue_head)
			ws->send_queue_tail = NULL;
		
		SAFE_FREE(node->data);
		SAFE_FREE(node);
		ws->send_queue_size--;
	}
	
	pthread_mutex_unlock(&ws->send_queue_lock);
	return 0;
}

/* WebSocket服务执行函数 - 执行一次事件循环迭代
 * 返回值：0 表示继续运行，-1 表示应该停止
 */
int ez_ws_service_exec(struct ez_ws_client_handle *ws, int timeout_ms) {
	if (!ws)
		return -1;
	
	/* 检查是否应该停止 */
	if (!ws->ws_running || ws->interrupted) {
		return -1;
	}
	
	/* 如果处于断开状态且未开始连接，尝试首次连接
	 * 注意：只在首次连接时尝试（retry_count == 0），重连由定时器处理
	 */
	if (ws->state == EZ_WS_STATE_DISCONNECTED && ws->sockfd < 0 && ws->retry_count == 0) {
		if (ez_ws_connect(ws) < 0) {
			/* 连接失败，启动重连定时器 */
			if (ws->reconnect_timerfd >= 0 && !ws->interrupted) {
				update_timerfd(ws->reconnect_timerfd, ws->current_retry_delay);
			}
		}
		/* 无论连接成功与否，继续执行事件循环 */
	}
	
	struct epoll_event events[10];
	
	/* 如果已中断，使用更短的超时时间以快速退出 */
	int timeout = (timeout_ms > 0) ? timeout_ms : (ws->interrupted ? 10 : 100);
	int nfds = epoll_wait(ws->epollfd, events, 10, timeout);
	if (nfds < 0) {
		if (errno == EINTR)
			return 0; /* 继续运行 */
		return -1; /* 错误，停止 */
	}
	
	for (int i = 0; i < nfds; i++) {
		if (events[i].data.fd == ws->sockfd) {
			if (events[i].events & EPOLLIN) {
				if (ws->state == EZ_WS_STATE_HANDSHAKING) {
					/* 握手阶段，接收并处理响应 */
					int handshake_result = ez_ws_handle_handshake(ws);
					if (handshake_result < 0) {
						/* 握手失败，关闭连接并启动重连 */
						ez_ws_client_handle_reset(ws, EZ_WS_RESET_HTTP);
						
						/* 启动重连定时器 */
						if (ws->reconnect_timerfd >= 0 && !ws->interrupted) {
							ws->retry_count++;
							if (ws->config.reconnect_backoff_enable &&
							    ws->retry_count > ws->config.reconnect_backoff_min_retries &&
							    ws->retry_count <= ws->config.reconnect_backoff_high_threshold) {
								ws->current_retry_delay *= 2;
							}
							update_timerfd(ws->reconnect_timerfd, ws->current_retry_delay);
						}
					} else if (handshake_result > 0) {
						/* 握手成功，不需要额外处理 */
					}
					/* handshake_result == 0 表示继续等待数据 */
				} else if (ws->state == EZ_WS_STATE_CONNECTED) {
					if (ez_ws_handle_data(ws) < 0) {
						/* 连接断开 */
						ez_ws_client_handle_reset(ws, EZ_WS_RESET_WS | EZ_WS_RESET_HEARTBEAT);
						
						/* 启动重连定时器 */
						if (ws->reconnect_timerfd >= 0 && !ws->interrupted) {
							ws->retry_count++;
							if (ws->config.reconnect_backoff_enable &&
							    ws->retry_count > ws->config.reconnect_backoff_min_retries &&
							    ws->retry_count <= ws->config.reconnect_backoff_high_threshold) {
								ws->current_retry_delay *= 2;
							}
							update_timerfd(ws->reconnect_timerfd, ws->current_retry_delay);
						}
					}
				}
			}
			
			if (events[i].events & EPOLLOUT) {
				if (ws->state == EZ_WS_STATE_CONNECTED) {
					/* 先处理部分发送的缓冲区 */
					if (ws->pending_send.is_active) {
						ssize_t sent = send(ws->sockfd,
						                    ws->pending_send.frame_data + ws->pending_send.sent_len,
						                    ws->pending_send.frame_len - ws->pending_send.sent_len, 0);
						if (sent > 0) {
							ws->pending_send.sent_len += sent;
							if (ws->pending_send.sent_len >= ws->pending_send.frame_len) {
								/* 发送完成，清理缓冲区 */
								free(ws->pending_send.frame_data);
								ws->pending_send.frame_data = NULL;
								ws->pending_send.is_active = 0;
								ws->pending_send.sent_len = 0;
								ws->pending_send.frame_len = 0;
							}
						} else if (sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
							/* 发送错误（如 EPIPE, ECONNRESET），连接已断开，触发重连 */
							LOG_ERROR("Send error: %s, closing connection\n", strerror(errno));
							ez_ws_client_handle_reset(ws, EZ_WS_RESET_WS | EZ_WS_RESET_HEARTBEAT);
							
							/* 启动重连定时器 */
							if (ws->reconnect_timerfd >= 0 && !ws->interrupted) {
								ws->retry_count++;
								if (ws->config.reconnect_backoff_enable &&
								    ws->retry_count > ws->config.reconnect_backoff_min_retries &&
								    ws->retry_count <= ws->config.reconnect_backoff_high_threshold) {
									ws->current_retry_delay *= 2;
								}
								update_timerfd(ws->reconnect_timerfd, ws->current_retry_delay);
							}
							continue; /* 跳过后续处理 */
						}
					}
					
					/* 处理发送队列（只有在没有待发送数据时才处理） */
					if (!ws->pending_send.is_active) {
						if (ez_ws_process_send_queue(ws) < 0) {
							/* 发送队列处理时发现连接错误，触发重连 */
							LOG_ERROR("Send queue processing error, closing connection\n");
							ez_ws_client_handle_reset(ws, EZ_WS_RESET_WS | EZ_WS_RESET_HEARTBEAT);
						
							/* 启动重连定时器 */
							if (ws->reconnect_timerfd >= 0 && !ws->interrupted) {
								ws->retry_count++;
								if (ws->config.reconnect_backoff_enable &&
								    ws->retry_count > ws->config.reconnect_backoff_min_retries &&
								    ws->retry_count <= ws->config.reconnect_backoff_high_threshold) {
									ws->current_retry_delay *= 2;
								}
								update_timerfd(ws->reconnect_timerfd, ws->current_retry_delay);
							}
						}
					}
				}
				/* 握手阶段不需要处理 EPOLLOUT */
			}
			
			if (events[i].events & (EPOLLERR | EPOLLHUP)) {
				/* 连接错误 */
				ez_ws_client_handle_reset(ws, EZ_WS_RESET_HTTP | EZ_WS_RESET_WS | EZ_WS_RESET_HEARTBEAT);
				
				/* 启动重连定时器 */
				if (ws->reconnect_timerfd >= 0 && !ws->interrupted) {
					ws->retry_count++;
					if (ws->config.reconnect_backoff_enable &&
					    ws->retry_count > ws->config.reconnect_backoff_min_retries &&
					    ws->retry_count <= ws->config.reconnect_backoff_high_threshold) {
						ws->current_retry_delay *= 2;
					}
					update_timerfd(ws->reconnect_timerfd, ws->current_retry_delay);
				}
			}
		} else if (events[i].data.fd == ws->reconnect_timerfd) {
			/* 重连定时器触发 */
			uint64_t expirations;
			read(ws->reconnect_timerfd, &expirations, sizeof(expirations));
			
			/* 如果已中断，立即停止重连 */
			if (ws->interrupted || !ws->ws_running) {
				struct itimerspec timer_spec = {0};
				timerfd_settime(ws->reconnect_timerfd, 0, &timer_spec, NULL);
				continue;
			}
			
			if (ws->state == EZ_WS_STATE_DISCONNECTED) {
				if (ws->config.reconnect_max_retries == 0 || ws->retry_count < ws->config.reconnect_max_retries) {
					if (ez_ws_connect(ws) == 0) {
						/* 连接成功，停止重连定时器 */
						struct itimerspec timer_spec = {0};
						timerfd_settime(ws->reconnect_timerfd, 0, &timer_spec, NULL);
					} else {
						/* 连接失败，调度重连 */
						ws->retry_count++;
						if (ws->config.reconnect_backoff_enable &&
						    ws->retry_count > ws->config.reconnect_backoff_min_retries &&
						    ws->retry_count <= ws->config.reconnect_backoff_high_threshold) {
							ws->current_retry_delay *= 2;
						}
						update_timerfd(ws->reconnect_timerfd, ws->current_retry_delay);
					}
				} else {
					LOG_WARN("Max retry count reached, stop reconnecting\n");
					ws->interrupted = 1;
				}
			}
		} else if (events[i].data.fd == ws->heartbeat_timerfd) {
			/* 心跳定时器触发 */
			uint64_t expirations;
			read(ws->heartbeat_timerfd, &expirations, sizeof(expirations));
			
			if (ws->state != EZ_WS_STATE_CONNECTED)
				continue;

			uint64_t now_ms = ez_ws_client_now_ms();

			if (ws->awaiting_pong) {
				if (now_ms - ws->last_ping_time_ms >= EZ_WS_CLIENT_PONG_TIMEOUT_MS) {
					LOG_WARN("Pong timeout, closing connection\n");
					ez_ws_client_handle_reset(ws, EZ_WS_RESET_WS | EZ_WS_RESET_HEARTBEAT);
					ez_ws_schedule_reconnect(ws);
					continue;
				}
			} else if (now_ms - ws->last_activity_time_ms >= EZ_WS_CLIENT_PING_INTERVAL_MS) {
				if (ez_ws_send_control_frame(ws, WS_OPCODE_PING, NULL, 0) == 0) {
					ws->awaiting_pong = 1;
					ws->last_ping_time_ms = now_ms;
					LOG_INFO("Ping sent to server\n");
				} else {
					LOG_WARN("Failed to send ping, closing connection\n");
					ez_ws_client_handle_reset(ws, EZ_WS_RESET_WS | EZ_WS_RESET_HEARTBEAT);
					ez_ws_schedule_reconnect(ws);
					continue;
				}
			}

			if (now_ms - ws->last_activity_time_ms >= EZ_WS_CLIENT_IDLE_TIMEOUT_MS) {
				LOG_WARN("Connection idle for %llu ms, closing\n",
				         (unsigned long long)(now_ms - ws->last_activity_time_ms));
				ez_ws_client_handle_reset(ws, EZ_WS_RESET_WS | EZ_WS_RESET_HEARTBEAT);
				ez_ws_schedule_reconnect(ws);
			}
		}
	}
	
	return 0; /* 继续运行 */
}

/* 公共API：发送文本 */
int ez_ws_send_text(struct ez_ws_client_handle *ws, const char *data, size_t len) {
	if (!ws || !data)
		return EZ_WS_ERR_INVALID_PARAM;
	
	if (!len)
		len = strlen(data);
	
	if (ws->state != EZ_WS_STATE_CONNECTED) {
		/* 如果队列未满，加入队列 */
		if (ws->send_queue_size < ws->send_queue_max) {
			struct send_queue_node *node = malloc(sizeof(struct send_queue_node));
			if (!node)
				return EZ_WS_ERR_NO_MEMORY;
			
			node->data = malloc(len + 1);
			if (!node->data) {
				free(node);
				return EZ_WS_ERR_NO_MEMORY;
			}
			
			memcpy(node->data, data, len);
			node->data[len] = '\0';
			node->len = len;
			node->opcode = WS_OPCODE_TEXT;
			node->next = NULL;
			
			pthread_mutex_lock(&ws->send_queue_lock);
			if (ws->send_queue_tail) {
				ws->send_queue_tail->next = node;
			} else {
				ws->send_queue_head = node;
			}
			ws->send_queue_tail = node;
			ws->send_queue_size++;
			pthread_mutex_unlock(&ws->send_queue_lock);
			
			return EZ_WS_OK;
		}
		return EZ_WS_ERR_NOT_CONNECTED;
	}
	
	return ez_ws_send_internal(ws, data, len, WS_OPCODE_TEXT);
}

/* 公共API：发送二进制 */
int ez_ws_send_binary(struct ez_ws_client_handle *ws, const void *data, size_t len) {
	if (!ws || !data || !len)
		return EZ_WS_ERR_INVALID_PARAM;
	
	if (ws->state != EZ_WS_STATE_CONNECTED)
		return EZ_WS_ERR_NOT_CONNECTED;
	
	return ez_ws_send_internal(ws, data, len, WS_OPCODE_BINARY);
}

/* 公共API：检查连接状态 */
int ez_ws_is_connected(struct ez_ws_client_handle *ws) {
	return (ws && ws->state == EZ_WS_STATE_CONNECTED) ? 1 : 0;
}

/* 公共API：获取连接状态 */
enum ez_ws_state ez_ws_get_state(struct ez_ws_client_handle *ws) {
	if (!ws)
		return EZ_WS_STATE_DISCONNECTED;
	return ws->state;
}

/* 初始化WebSocket客户端 */
struct ez_ws_client_handle *ez_ws_client_handle_create(struct ez_ws_client_config *config,
                                        struct ez_ws_callbacks *callbacks) {
	struct ez_ws_client_handle *ws = calloc(1, sizeof(struct ez_ws_client_handle));
	if (!ws)
		return NULL;
	
	/* 复制配置（如果提供） */
	if (config) {
		ws->config = *config;
	} else {
		/* 使用默认配置 */
		ws->config.server_addr = "localhost";
		ws->config.port = 54321;
		ws->config.url_path = "/";
		ws->config.protocol = "fws_a-burning";
		ws->config.reconnect_interval_ms = RECONNECT_INTERVAL_MS;
		ws->config.reconnect_max_retries = RECONNECT_MAX_RETRIES;
		ws->config.reconnect_backoff_enable = RECONNECT_BACKOFF_ENABLE;
		ws->config.reconnect_backoff_min_retries = RECONNECT_BACKOFF_MIN_RETRIES;
		ws->config.reconnect_backoff_high_threshold = RECONNECT_BACKOFF_HIGH_THRESHOLD;
	}
	
	ws->server_addr = strdup(ws->config.server_addr);
	ws->port = ws->config.port;
	ws->url_path = strdup(ws->config.url_path ? ws->config.url_path : "/");
	ws->protocol = ws->config.protocol ? strdup(ws->config.protocol) : NULL;
	ws->sockfd = -1;
	ws->reconnect_timerfd = -1;
	ws->heartbeat_timerfd = -1;
	ws->state = EZ_WS_STATE_DISCONNECTED;
	ws->current_retry_delay = ws->config.reconnect_interval_ms;
	ws->send_queue_max = 1024;
	ws->handshake_response_len = 0;
	ws->http_handshake_complete = 0;
	ws->http_header_field = NULL;
	ws->http_header_value = NULL;
	ws->http_sec_websocket_accept = NULL;
	ws->current_frame.buffer = NULL;
	ws->current_frame.buffer_size = 0;
	
	/* 初始化发送缓冲区 */
	ws->pending_send.frame_data = NULL;
	ws->pending_send.frame_len = 0;
	ws->pending_send.sent_len = 0;
	ws->pending_send.is_active = 0;
	
	if (callbacks)
		ws->callbacks = *callbacks;
	
	pthread_mutex_init(&ws->send_queue_lock, NULL);
	
	/* 初始化 HTTP 解析器 */
	http_parser_init(&ws->http_parser, HTTP_RESPONSE);
	ws->http_parser.data = ws;
	http_parser_settings_init(&ws->http_parser_settings);
	ws->http_parser_settings.on_status = on_http_status;
	ws->http_parser_settings.on_header_field = on_http_header_field;
	ws->http_parser_settings.on_header_value = on_http_header_value;
	ws->http_parser_settings.on_headers_complete = on_http_headers_complete;
	
	/* 初始化 WebSocket 解析器 */
	websocket_parser_init(&ws->ws_parser);
	ws->ws_parser.data = ws;
	websocket_parser_settings_init(&ws->ws_parser_settings);
	ws->ws_parser_settings.on_frame_begin = on_ws_frame_begin;
	ws->ws_parser_settings.on_frame_payload = on_ws_frame_payload;
	ws->ws_parser_settings.on_frame_complete = on_ws_frame_complete;
	
	/* 创建epoll */
	ws->epollfd = epoll_create1(EPOLL_CLOEXEC);
	if (ws->epollfd < 0) {
		free(ws);
		return NULL;
	}
	
	/* 创建重连定时器 */
	ws->reconnect_timerfd = create_timerfd(ws->config.reconnect_interval_ms);
	if (ws->reconnect_timerfd >= 0) {
		struct epoll_event ev;
		ev.events = EPOLLIN;
		ev.data.fd = ws->reconnect_timerfd;
		epoll_ctl(ws->epollfd, EPOLL_CTL_ADD, ws->reconnect_timerfd, &ev);
	}
	
	/* 创建心跳定时器 */
	ws->heartbeat_timerfd = create_timerfd(EZ_WS_CLIENT_HEARTBEAT_TICK_MS);
	if (ws->heartbeat_timerfd >= 0) {
		struct epoll_event ev;
		ev.events = EPOLLIN;
		ev.data.fd = ws->heartbeat_timerfd;
		epoll_ctl(ws->epollfd, EPOLL_CTL_ADD, ws->heartbeat_timerfd, &ev);
		
		/* 初始时停止心跳定时器 */
		struct itimerspec timer_spec = {0};
		timerfd_settime(ws->heartbeat_timerfd, 0, &timer_spec, NULL);
	}
	
	/* 重置 HTTP 解析器（准备新的握手） */
	http_parser_init(&ws->http_parser, HTTP_RESPONSE);
	ws->http_parser.data = ws;
	ws->http_handshake_complete = 0;
	SAFE_FREE(ws->http_header_field);
	SAFE_FREE(ws->http_header_value);
	SAFE_FREE(ws->http_sec_websocket_accept);
	ws->handshake_response_len = 0;
	
	/* 设置运行标志 */
	ws->ws_running = 1;
	ws->interrupted = 0;
	
	/* 注意：连接操作将在 ez_ws_service_exec 中执行，避免创建句柄时阻塞 */
	
	return ws;
}

/* 清理WebSocket客户端 */
void ez_ws_client_cleanup(struct ez_ws_client_handle *ws) {
	if (!ws)
		return;
	
	/* 立即停止所有定时器，避免在关闭时触发重连 */
	if (ws->reconnect_timerfd >= 0) {
		struct itimerspec timer_spec = {0};
		timerfd_settime(ws->reconnect_timerfd, 0, &timer_spec, NULL);
	}
	if (ws->heartbeat_timerfd >= 0) {
		struct itimerspec timer_spec = {0};
		timerfd_settime(ws->heartbeat_timerfd, 0, &timer_spec, NULL);
	}
	
	/* 设置中断标志 */
	ws->interrupted = 1;
	ws->ws_running = 0;
	
	/* 关闭 socket 以立即唤醒 epoll_wait */
	if (ws->sockfd >= 0) {
		epoll_ctl(ws->epollfd, EPOLL_CTL_DEL, ws->sockfd, NULL);
		shutdown(ws->sockfd, SHUT_RDWR);
		close(ws->sockfd);
		ws->sockfd = -1;
	}
	
	if (ws->sockfd >= 0)
		close(ws->sockfd);
	if (ws->epollfd >= 0)
		close(ws->epollfd);
	if (ws->reconnect_timerfd >= 0)
		close(ws->reconnect_timerfd);
	if (ws->heartbeat_timerfd >= 0)
		close(ws->heartbeat_timerfd);
	
	SAFE_FREE(ws->server_addr);
	SAFE_FREE(ws->url_path);
	SAFE_FREE(ws->protocol);
	SAFE_FREE(ws->handshake_key);
	
	/* 清理 HTTP 解析器相关 */
	SAFE_FREE(ws->http_header_field);
	SAFE_FREE(ws->http_header_value);
	SAFE_FREE(ws->http_sec_websocket_accept);
	
	/* 清理 WebSocket 解析器相关 */
	SAFE_FREE(ws->current_frame.buffer);
	
	/* 清理发送队列 */
	pthread_mutex_lock(&ws->send_queue_lock);
	while (ws->send_queue_head) {
		struct send_queue_node *node = ws->send_queue_head;
		ws->send_queue_head = node->next;
		free(node->data);
		free(node);
	}
	pthread_mutex_unlock(&ws->send_queue_lock);
	pthread_mutex_destroy(&ws->send_queue_lock);
	
	/* 清理发送缓冲区 */
	ez_ws_clear_send_buffer(ws);
	
	free(ws);
}
