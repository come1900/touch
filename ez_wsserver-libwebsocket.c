/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * ez_wsserver-libwebsocket.c - WebSocket Server Component Implementation
 *
 * Copyright (C) 2011 ezlibs.com, All Rights Reserved.
 *
 * $Id: ez_wsserver-libwebsocket.c 1 2011-12-27 20:00:00Z WHF $
 *
 * Explain:
 *     WebSocket server component implementation based on libwebsockets.
 *     Provides the core functionality for WebSocket server operations.
 *
 * Update:
 *     2011-12-27 20:00:00 WHF Create
 *     2024-XX-XX XX:XX:XX Refactor: Remove global state, support multiple instances via PVO
 *                        - Remove global g_ws_server_handle, pass handle via lws_protocol_vhost_options
 *                        - Support multiple server instances simultaneously
 *     2024-XX-XX XX:XX:XX Refactor: Remove default keepalive configurations
 *                        - All keepalive parameters must be specified by application layer
 *                        - Library does not provide default values, 0 means disable
 *     2024-XX-XX XX:XX:XX Refactor: Protocol and path_prefix configuration
 *                        - protocol is required (must not be NULL), return error if NULL
 *                        - path_prefix is optional (NULL means no path checking, accept all connections)
 *                        - Remove all default value definitions from library
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdint.h>

#include <libwebsockets.h>

#include "ez_wsserver-libwebsocket.h"

/* 开关：收发数据打印（1 开启，0 关闭） */
#ifndef EZ_WS_SERVER_DATA_LOG
#define EZ_WS_SERVER_DATA_LOG 0
#endif

#if EZ_WS_SERVER_DATA_LOG
#define EZ_WS_DATA_LOGF(...) lwsl_user(__VA_ARGS__)
#else
#define EZ_WS_DATA_LOGF(...)
#endif

#define RING_DEPTH 4096

static uint64_t
ez_ws_server_now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

/* 内部消息结构 */
struct msg {
	void *payload; /* is malloc'd */
	size_t len;
	char binary;
	char first;
	char final;
};

/* 前向声明 */
struct per_session_data__minimal_server_echo;
struct vhd_minimal_server_echo;

/* 客户端连接信息 */
struct client_info {
	int id;                        /* 客户端内部序列号 */
	struct lws *wsi;               /* 对应的wsi */
	char ip[64];                   /* 客户端IP地址 */
	int port;                      /* 客户端端口 */
	time_t connect_time;           /* 连接时间戳 */
	struct per_session_data__minimal_server_echo *pss;  /* 对应的session数据 */
	struct client_info *next;      /* 链表下一个节点 */
};

struct per_session_data__minimal_server_echo {
	struct lws_ring *ring;
	uint32_t msglen;
	uint32_t tail;
	uint8_t completed:1;
	uint8_t flow_controlled:1;
	uint8_t write_consume_pending:1;
	uint8_t awaiting_pong:1;
	uint8_t pending_ping:1;
	
	/* 用于跟踪已处理的广播消息索引 */
	size_t last_broadcast_index;
	
	/* 指向此客户端的信息节点 */
	struct client_info *client_info;
	
	/* 指定发送消息队列（用于单播） */
	char **unicast_queue;
	size_t unicast_cap;
	size_t unicast_read_index;
	size_t unicast_write_index;
	pthread_mutex_t unicast_lock;
	
	/* 链路保活相关时间戳 */
	uint64_t last_rx_ms;
	uint64_t last_tx_ms;
	uint64_t last_activity_ms;
	uint64_t last_ping_ms;
};

struct vhd_minimal_server_echo {
	struct lws_context *context;
	struct lws_vhost *vhost;

	int *interrupted;
	int *options;
	const char **protocol;
	
	/* 指向对应的服务端句柄（通过 PVO 传递，取代全局变量） */
	struct ez_ws_server_handle *server_handle;
	
	pthread_mutex_t broadcast_lock;
	
	/* 客户端连接列表 */
	struct client_info *client_list;
	pthread_mutex_t client_list_lock;
	int client_count;
	int next_client_id;  /* 下一个可用的客户端ID */
	
	/* WebSocket广播队列（循环缓冲区） */
	char **broadcast_queue;
	size_t broadcast_cap;
	size_t broadcast_write_index;  /* 写入位置（最新消息） */
	pthread_mutex_t broadcast_queue_lock;
	
	/* 回调函数 */
	struct ez_ws_server_callbacks callbacks;
	
	/* 保活配置 */
	uint32_t ping_interval_ms;
	uint32_t ping_timeout_ms;
	uint32_t idle_timeout_ms;
	uint32_t timer_interval_ms;
	
	/* 路径前缀 */
	const char *path_prefix;
};

/* WebSocket服务端句柄（完整定义） */
struct ez_ws_server_handle {
	struct lws_context *context;
	struct vhd_minimal_server_echo *vhd;  /* 虚拟主机数据，用于访问ws API */
	int *interrupted;

	/* 配置 */
	struct ez_ws_server_config config;

	/* PVO 指针，用于清理 */
	struct lws_protocol_vhost_options *pvo;
	struct lws_protocol_vhost_options *pvo_interrupted;
	struct lws_protocol_vhost_options *pvo_protocol;
	struct lws_protocol_vhost_options *pvo_options;
	struct lws_protocol_vhost_options *pvo_server_handle;  /* 新增：传递 server_handle */
};

static void
__minimal_destroy_message(void *_msg)
{
	struct msg *msg = _msg;

	free(msg->payload);
	msg->payload = NULL;
	msg->len = 0;
}

/*
 * 添加客户端到连接列表
 */
static int
add_client_to_list(struct vhd_minimal_server_echo *vhd, struct lws *wsi, 
                   struct per_session_data__minimal_server_echo *pss)
{
	if (!vhd || !wsi || !pss)
		return -1;
	
	struct client_info *client = (struct client_info *)malloc(sizeof(struct client_info));
	if (!client)
		return -1;
	
	memset(client, 0, sizeof(struct client_info));
	client->wsi = wsi;
	client->connect_time = time(NULL);
	
	/* 获取客户端IP和端口 */
	char name[128], rip[128];
	int fd = lws_get_socket_fd(wsi);
	lws_get_peer_addresses(wsi, fd, name, sizeof(name), rip, sizeof(rip));
	
	/* 安全地拷贝IP地址 */
	strncpy(client->ip, rip, sizeof(client->ip) - 1);
	client->ip[sizeof(client->ip) - 1] = '\0';
	
	/* 从socket获取端口号 */
	struct sockaddr_storage addr;
	socklen_t len = sizeof(addr);
	if (getpeername(fd, (struct sockaddr *)&addr, &len) == 0) {
		if (addr.ss_family == AF_INET) {
			client->port = ntohs(((struct sockaddr_in *)&addr)->sin_port);
		} else if (addr.ss_family == AF_INET6) {
			client->port = ntohs(((struct sockaddr_in6 *)&addr)->sin6_port);
		}
	}
	
	/* 添加到链表头部 */
	pthread_mutex_lock(&vhd->client_list_lock);
	client->id = vhd->next_client_id++;  /* 分配唯一ID */
	client->pss = pss;  /* 保存pss指针 */
	client->next = vhd->client_list;
	vhd->client_list = client;
	vhd->client_count++;
	pss->client_info = client;
	pthread_mutex_unlock(&vhd->client_list_lock);
	
	return 0;
}

/*
 * 从连接列表移除客户端
 */
static void
remove_client_from_list(struct vhd_minimal_server_echo *vhd, 
                        struct per_session_data__minimal_server_echo *pss)
{
	if (!vhd || !pss || !pss->client_info)
		return;
	
	pthread_mutex_lock(&vhd->client_list_lock);
	
	struct client_info *curr = vhd->client_list;
	struct client_info *prev = NULL;
	
	while (curr) {
		if (curr == pss->client_info) {
			if (prev)
				prev->next = curr->next;
			else
				vhd->client_list = curr->next;
			
			free(curr);
			vhd->client_count--;
			pss->client_info = NULL;
			break;
		}
		prev = curr;
		curr = curr->next;
	}
	
	pthread_mutex_unlock(&vhd->client_list_lock);
}

/*
 * 将消息入队到指定客户端的ring
 */
static int
server_enqueue_to_client(struct per_session_data__minimal_server_echo *pss,
                         const char *data, size_t len, int is_binary)
{
	struct msg amsg;
	
	if (!pss || !pss->ring || !data || !len)
		return -1;

	amsg.first = 1;
	amsg.final = 1;
	amsg.binary = is_binary ? 1 : 0;
	amsg.len = len;
	amsg.payload = malloc(LWS_PRE + len);
	if (!amsg.payload)
		return -1;

	memcpy((unsigned char *)amsg.payload + LWS_PRE, data, len);
	
	if (!lws_ring_insert(pss->ring, &amsg, 1)) {
		__minimal_destroy_message(&amsg);
		return -1;
	}
	
	return 0;
}

/*
 * WebSocket发送接口 - 向指定客户端或所有客户端发送消息
 */
static int
ws_server_send_internal(struct vhd_minimal_server_echo *vhd, int client_id, 
                        const void *data, size_t len, int is_binary)
{
    if (!vhd || !data)
        return EZ_WS_SERVER_ERR_INVALID_PARAM;

    if (!len)
        return EZ_WS_SERVER_ERR_INVALID_PARAM;

    /* 如果是广播到所有客户端 */
    if (client_id == -1) {
        pthread_mutex_lock(&vhd->broadcast_queue_lock);
        
        if (!vhd->broadcast_queue) {
            pthread_mutex_unlock(&vhd->broadcast_queue_lock);
            return EZ_WS_SERVER_ERR_NO_MEMORY;
        }
        
        /* 循环覆盖旧消息 */
        size_t idx = vhd->broadcast_write_index % vhd->broadcast_cap;
        
        /* 释放旧消息（如果有） */
        if (vhd->broadcast_queue[idx]) {
            free(vhd->broadcast_queue[idx]);
        }
        
        /* 写入新消息 */
        vhd->broadcast_queue[idx] = (char *)malloc(len + 1);
        if (!vhd->broadcast_queue[idx]) {
            pthread_mutex_unlock(&vhd->broadcast_queue_lock);
            return EZ_WS_SERVER_ERR_NO_MEMORY;
        }
        
        memcpy(vhd->broadcast_queue[idx], data, len);
        vhd->broadcast_queue[idx][len] = '\0';
        vhd->broadcast_write_index++;  /* 增加写索引 */
        
        pthread_mutex_unlock(&vhd->broadcast_queue_lock);
        
        /* 主动通知websocket线程有新数据，触发广播 */
        if (vhd->vhost && vhd->protocol) {
            /* 请求所有连接的客户端变为可写状态 */
            lws_callback_on_writable_all_protocol(vhd->context, 
                lws_vhost_name_to_protocol(vhd->vhost, *vhd->protocol));
        }
        if (vhd->context) {
            lws_cancel_service(vhd->context);  /* 唤醒websocket事件循环 */
        }
        
        return EZ_WS_SERVER_OK;
    }
    
    /* 指定客户端发送 */
    pthread_mutex_lock(&vhd->client_list_lock);
    
    struct client_info *client = vhd->client_list;
    struct per_session_data__minimal_server_echo *target_pss = NULL;
    struct lws *target_wsi = NULL;
    
    /* 查找指定ID的客户端 */
    while (client) {
        if (client->id == client_id) {
            target_pss = client->pss;
            target_wsi = client->wsi;
            break;
        }
        client = client->next;
    }
    
    pthread_mutex_unlock(&vhd->client_list_lock);
    
    if (!target_pss || !target_wsi) {
        return EZ_WS_SERVER_ERR_CLIENT_NOT_FOUND;
    }
    
    /* 将消息加入客户端的unicast队列 */
    pthread_mutex_lock(&target_pss->unicast_lock);
    
    if (!target_pss->unicast_queue) {
        pthread_mutex_unlock(&target_pss->unicast_lock);
        return EZ_WS_SERVER_ERR_NO_MEMORY;
    }
    
    /* 检查队列是否已满 */
    if (target_pss->unicast_write_index - target_pss->unicast_read_index >= target_pss->unicast_cap) {
        pthread_mutex_unlock(&target_pss->unicast_lock);
        return EZ_WS_SERVER_ERR_QUEUE_FULL;
    }
    
    /* 添加消息到队列 */
    size_t idx = target_pss->unicast_write_index % target_pss->unicast_cap;
    free(target_pss->unicast_queue[idx]);  /* 释放旧消息（如果有） */
    
    target_pss->unicast_queue[idx] = (char *)malloc(len + 1);
    if (!target_pss->unicast_queue[idx]) {
        pthread_mutex_unlock(&target_pss->unicast_lock);
        return EZ_WS_SERVER_ERR_NO_MEMORY;
    }
    
    memcpy(target_pss->unicast_queue[idx], data, len);
    target_pss->unicast_queue[idx][len] = '\0';
    target_pss->unicast_write_index++;
    
    pthread_mutex_unlock(&target_pss->unicast_lock);
    
    /* 触发该客户端的可写回调 */
    lws_callback_on_writable(target_wsi);
    if (vhd->context) {
        lws_cancel_service(vhd->context);
    }
    
    return EZ_WS_SERVER_OK;
}

/* 全局websocket服务端句柄（用于在回调中访问） */
/* 废弃：全局变量已被移除，现在通过 PVO 机制传递句柄指针
 * 这使得库可以支持多个服务端实例，并且库独立于应用
 */
// static struct ez_ws_server_handle *g_ws_server_handle = NULL;  /* 已废弃 */

static int
callback_minimal_server_echo(struct lws *wsi, enum lws_callback_reasons reason,
			  void *user, void *in, size_t len)
{
	struct per_session_data__minimal_server_echo *pss =
			(struct per_session_data__minimal_server_echo *)user;
	struct vhd_minimal_server_echo *vhd = (struct vhd_minimal_server_echo *)
			lws_protocol_vh_priv_get(lws_get_vhost(wsi),
				lws_get_protocol(wsi));
	const struct msg *pmsg;
	struct msg amsg;
	int m, n, flags;

	switch (reason) {

	case LWS_CALLBACK_PROTOCOL_INIT:
		vhd = lws_protocol_vh_priv_zalloc(lws_get_vhost(wsi),
				lws_get_protocol(wsi),
				sizeof(struct vhd_minimal_server_echo));
		if (!vhd)
			return -1;

		vhd->context = lws_get_context(wsi);
		vhd->vhost = lws_get_vhost(wsi);
		pthread_mutex_init(&vhd->broadcast_lock, NULL);
		
		/* 初始化客户端列表 */
		vhd->client_list = NULL;
		vhd->client_count = 0;
		vhd->next_client_id = 1;  /* 客户端ID从1开始 */
		pthread_mutex_init(&vhd->client_list_lock, NULL);
		
		/* 初始化广播队列 */
		vhd->broadcast_cap = 64;
		vhd->broadcast_write_index = 0;
		vhd->broadcast_queue = (char **)calloc(vhd->broadcast_cap, sizeof(char *));
		pthread_mutex_init(&vhd->broadcast_queue_lock, NULL);

		/* get the pointers we were passed in pvo */
		{
			const struct lws_protocol_vhost_options *p;
			
			p = lws_pvo_search((const struct lws_protocol_vhost_options *)in, "interrupted");
			vhd->interrupted = p ? (int *)p->value : NULL;
			
			p = lws_pvo_search((const struct lws_protocol_vhost_options *)in, "options");
			vhd->options = p ? (int *)p->value : NULL;
			
			p = lws_pvo_search((const struct lws_protocol_vhost_options *)in, "protocol");
			vhd->protocol = p ? (const char **)p->value : NULL;
			
			/* 获取服务端句柄指针（关键修改：通过 PVO 传递，而不是全局变量） */
			p = lws_pvo_search((const struct lws_protocol_vhost_options *)in, "server_handle");
			vhd->server_handle = p ? (struct ez_ws_server_handle *)p->value : NULL;
			
			if (!vhd->interrupted || !vhd->options || !vhd->protocol) {
				lwsl_err("Error: Incomplete PVO configuration\n");
				return -1;
			}
			
			/* 从服务端句柄获取配置 */
			if (vhd->server_handle) {
				/* 设置配置到新创建的vhd - 直接使用配置值（包括0表示禁用） */
				vhd->callbacks = vhd->server_handle->config.callbacks;
				/* ping_interval_ms: 0 表示禁用主动 ping，非 0 表示启用 */
				vhd->ping_interval_ms = vhd->server_handle->config.ping_interval_ms;
				vhd->ping_timeout_ms = vhd->server_handle->config.ping_timeout_ms;
				/* idle_timeout_ms: 0 表示禁用空闲超时（不推荐），非 0 表示启用 */
				vhd->idle_timeout_ms = vhd->server_handle->config.idle_timeout_ms;
				vhd->timer_interval_ms = vhd->server_handle->config.timer_interval_ms;
				/* path_prefix 可以为 NULL，表示不检查路径 */
				vhd->path_prefix = vhd->server_handle->config.path_prefix;
				/* 将新创建的vhd保存到handle中 */
				vhd->server_handle->vhd = vhd;
			} else {
				/* 没有服务端句柄，无法获取配置 - 这是一个错误 */
				lwsl_err("Error: server_handle not found in PVO, cannot initialize\n");
				return -1;
			}
		}
		break;
	
	case LWS_CALLBACK_PROTOCOL_DESTROY:
		/* 清理客户端列表 */
		pthread_mutex_lock(&vhd->client_list_lock);
		while (vhd->client_list) {
			struct client_info *temp = vhd->client_list;
			vhd->client_list = vhd->client_list->next;
			free(temp);
		}
		pthread_mutex_unlock(&vhd->client_list_lock);
		pthread_mutex_destroy(&vhd->client_list_lock);
		
		/* 清理广播队列 */
		pthread_mutex_lock(&vhd->broadcast_queue_lock);
		if (vhd->broadcast_queue) {
			for (size_t i = 0; i < vhd->broadcast_cap; ++i) {
				free(vhd->broadcast_queue[i]);
			}
			free(vhd->broadcast_queue);
		}
		pthread_mutex_unlock(&vhd->broadcast_queue_lock);
		pthread_mutex_destroy(&vhd->broadcast_queue_lock);
		
		pthread_mutex_destroy(&vhd->broadcast_lock);
		break;

	case LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION:
		/* 在握手完成前进行路径检查 */
		{
			char uri[256];
			int uri_len = lws_hdr_total_length(wsi, WSI_TOKEN_GET_URI);
			
			if (uri_len > 0 && uri_len < sizeof(uri)) {
				n = lws_hdr_copy(wsi, uri, sizeof(uri), WSI_TOKEN_GET_URI);
				if (n > 0) {
					uri[n] = '\0';
					
				/* 如果配置了 path_prefix，则验证路径是否包含它 */
				const char *path_prefix = vhd->path_prefix;
				
				if (path_prefix) {
					/* 查找路径中是否包含 path_prefix */
					const char *found = strstr(uri, path_prefix);
					if (!found) {
						char name[128], rip[128];
						int fd = lws_get_socket_fd(wsi);
						if (fd >= 0) {
							lws_get_peer_addresses(wsi, fd, name, sizeof(name), rip, sizeof(rip));
							lwsl_warn("Connection rejected: path '%s' does not contain %s (from %s)\n",
							          uri, path_prefix, rip);
						} else {
							lwsl_warn("Connection rejected: path '%s' does not contain %s\n",
							          uri, path_prefix);
						}
						/* 返回非0拒绝连接 */
						return 1;
					}
				}
				/* 如果 path_prefix 为 NULL，则不检查路径，接受所有连接 */
					
					/* 路径验证通过 */
					char name[128], rip[128];
					int fd = lws_get_socket_fd(wsi);
					if (fd >= 0) {
						lws_get_peer_addresses(wsi, fd, name, sizeof(name), rip, sizeof(rip));
						lwsl_info("WebSocket connection accepted: path='%s' from %s\n", uri, rip);
					}
				}
			}
		}
		break;

	case LWS_CALLBACK_ESTABLISHED:
		/* 客户端连接建立 */
		pss->client_info = NULL;
		pss->awaiting_pong = 0;
		pss->pending_ping = 0;
		{
			uint64_t now_ms = ez_ws_server_now_ms();
			pss->last_ping_ms = now_ms;
			pss->last_rx_ms = now_ms;
			pss->last_tx_ms = now_ms;
			pss->last_activity_ms = now_ms;
		}
		
		/* 初始化unicast队列 */
		pss->unicast_cap = 32;
		pss->unicast_read_index = 0;
		pss->unicast_write_index = 0;
		pss->unicast_queue = (char **)calloc(pss->unicast_cap, sizeof(char *));
		pthread_mutex_init(&pss->unicast_lock, NULL);
		
		/* 添加客户端到连接列表 */
		if (add_client_to_list(vhd, wsi, pss) == 0) {
			lwsl_user("LWS_CALLBACK_ESTABLISHED: client #%d connected from %s:%d (total: %d)\n",
			          pss->client_info->id, pss->client_info->ip, pss->client_info->port, vhd->client_count);
			
			/* 调用连接回调 */
			if (vhd->callbacks.on_connected) {
				vhd->callbacks.on_connected(pss->client_info->id, 
				                           pss->client_info->ip, 
				                           pss->client_info->port,
				                           vhd->callbacks.user_data);
			}
		} else {
			lwsl_err("Failed to add client to list\n");
		}
		
		pss->ring = lws_ring_create(sizeof(struct msg), RING_DEPTH,
					    __minimal_destroy_message);
		if (!pss->ring)
			return 1;
		pss->tail = 0;
		/* 初始化为当前广播写索引，不接收历史消息 */
		pthread_mutex_lock(&vhd->broadcast_queue_lock);
		pss->last_broadcast_index = vhd->broadcast_write_index;
		pthread_mutex_unlock(&vhd->broadcast_queue_lock);
		
	/* 启动保活定时器（如果 timer_interval_ms = 0 则不启动） */
	if (vhd->timer_interval_ms > 0) {
		lws_set_timer_usecs(wsi, vhd->timer_interval_ms * 1000);
	}
		break;

	case LWS_CALLBACK_SERVER_WRITEABLE:

		EZ_WS_DATA_LOGF("LWS_CALLBACK_SERVER_WRITEABLE\n");
		{
			uint64_t now_ms = ez_ws_server_now_ms();
			if (pss->pending_ping) {
				unsigned char ping_buf[LWS_PRE + 4];
				const char *ping_payload = "hb";
				memcpy(ping_buf + LWS_PRE, ping_payload, 2);
				int ping_len = 2;
				int ret = lws_write(wsi, ping_buf + LWS_PRE, ping_len, LWS_WRITE_PING);
				if (ret < ping_len) {
					lwsl_err("failed to send ping frame (%d)\n", ret);
					return -1;
				}
				pss->pending_ping = 0;
				pss->last_tx_ms = now_ms;
				pss->last_activity_ms = now_ms;
				EZ_WS_DATA_LOGF("Ping sent to client #%d\n",
				          pss->client_info ? pss->client_info->id : -1);
			}
		}

		if (pss->write_consume_pending) {
			/* perform the deferred fifo consume */
			lws_ring_consume_single_tail(pss->ring, &pss->tail, 1);
			pss->write_consume_pending = 0;
		}

		/* 优先处理单播消息（unicast） */
		pthread_mutex_lock(&pss->unicast_lock);
		while (pss->unicast_read_index < pss->unicast_write_index) {
			size_t idx = pss->unicast_read_index % pss->unicast_cap;
			if (pss->unicast_queue[idx]) {
				size_t msg_len = strlen(pss->unicast_queue[idx]);
				EZ_WS_DATA_LOGF("[Server] Unicast to client #%d: %s\n", 
				            pss->client_info ? pss->client_info->id : -1,
				            pss->unicast_queue[idx]);
				(void)server_enqueue_to_client(pss, pss->unicast_queue[idx], msg_len, 0);
				free(pss->unicast_queue[idx]);
				pss->unicast_queue[idx] = NULL;
			}
			pss->unicast_read_index++;
		}
		pthread_mutex_unlock(&pss->unicast_lock);
		
		/* 检查是否有未处理的广播消息 */
		pthread_mutex_lock(&vhd->broadcast_queue_lock);
		size_t current_write_idx = vhd->broadcast_write_index;
		
		/* 处理此客户端未读取的所有消息 */
		while (pss->last_broadcast_index < current_write_idx) {
			size_t idx = pss->last_broadcast_index % vhd->broadcast_cap;
			if (vhd->broadcast_queue[idx]) {
				size_t msg_len = strlen(vhd->broadcast_queue[idx]);
				EZ_WS_DATA_LOGF("[Server] Broadcasting to client: %s\n", 
				            vhd->broadcast_queue[idx]);
				(void)server_enqueue_to_client(pss, vhd->broadcast_queue[idx], msg_len, 0);
			}
			pss->last_broadcast_index++;
		}
		pthread_mutex_unlock(&vhd->broadcast_queue_lock);

		pmsg = lws_ring_get_element(pss->ring, &pss->tail);
		if (!pmsg) {
			EZ_WS_DATA_LOGF(" (nothing in ring)\n");
			break;
		}

		flags = lws_write_ws_flags(
			    pmsg->binary ? LWS_WRITE_BINARY : LWS_WRITE_TEXT,
			    pmsg->first, pmsg->final);

		/* notice we allowed for LWS_PRE in the payload already */
		m = lws_write(wsi, ((unsigned char *)pmsg->payload) +
			      LWS_PRE, pmsg->len, (enum lws_write_protocol)flags);
		if (m < (int)pmsg->len) {
			lwsl_err("ERROR %d writing to ws socket\n", m);
			return -1;
		}

		EZ_WS_DATA_LOGF(" wrote %d: flags: 0x%x first: %d final %d\n",
				m, flags, pmsg->first, pmsg->final);
		pss->last_tx_ms = ez_ws_server_now_ms();
		pss->last_activity_ms = pss->last_tx_ms;

		#if EZ_WS_SERVER_DATA_LOG
		{
			size_t show = pmsg->len > 512 ? 512 : pmsg->len;
			char buf[513];
			if (show)
				memcpy(buf, ((unsigned char *)pmsg->payload) + LWS_PRE, show);
			buf[show] = '\0';
			EZ_WS_DATA_LOGF("SEND[%zu]: %s%s\n", (size_t)pmsg->len, buf, pmsg->len > show ? "..." : "");
		}
		#endif
		
		pss->write_consume_pending = 1;
		lws_callback_on_writable(wsi);

		if (pss->flow_controlled &&
		    (int)lws_ring_get_count_free_elements(pss->ring) > RING_DEPTH - 5) {
			lws_rx_flow_control(wsi, 1);
			pss->flow_controlled = 0;
		}

		if ((*vhd->options & 1) && pmsg && pmsg->final)
			pss->completed = 1;

		break;

	case LWS_CALLBACK_RECEIVE:

		EZ_WS_DATA_LOGF("LWS_CALLBACK_RECEIVE: %4d (rpp %5d, first %d, "
			  "last %d, bin %d, msglen %d (+ %d = %d))\n",
			  (int)len, (int)lws_remaining_packet_payload(wsi),
			  lws_is_first_fragment(wsi),
			  lws_is_final_fragment(wsi),
			  lws_frame_is_binary(wsi), pss->msglen, (int)len,
			  (int)pss->msglen + (int)len);

		#if EZ_WS_SERVER_DATA_LOG
		{
			size_t show = len > 512 ? 512 : len;
			char buf[513];
			if (show)
				memcpy(buf, in, show);
			buf[show] = '\0';
			EZ_WS_DATA_LOGF("RECV[%zu]: %s%s\n", len, buf, len > show ? "..." : "");
		}
		#endif

		amsg.first = (char)lws_is_first_fragment(wsi);
		amsg.final = (char)lws_is_final_fragment(wsi);
		amsg.binary = (char)lws_frame_is_binary(wsi);
		n = (int)lws_ring_get_count_free_elements(pss->ring);
		if (!n) {
			lwsl_user("dropping!\n");
			break;
		}

		if (amsg.final)
			pss->msglen = 0;
		else
			pss->msglen += (uint32_t)len;
		
		{
			uint64_t now_ms = ez_ws_server_now_ms();
			pss->last_rx_ms = now_ms;
			pss->last_activity_ms = now_ms;
		}

		amsg.len = len;
		/* notice we over-allocate by LWS_PRE */
		amsg.payload = malloc(LWS_PRE + len);
		if (!amsg.payload) {
			lwsl_user("OOM: dropping\n");
			break;
		}

		memcpy((char *)amsg.payload + LWS_PRE, in, len);
		if (!lws_ring_insert(pss->ring, &amsg, 1)) {
			__minimal_destroy_message(&amsg);
			lwsl_user("dropping!\n");
			break;
		}
		
		/* 调用接收回调 */
		if (vhd && vhd->callbacks.on_receive && pss->client_info) {
			vhd->callbacks.on_receive(pss->client_info->id, in, len, 
			                         amsg.binary, vhd->callbacks.user_data);
		}
		
		lws_callback_on_writable(wsi);

		if (n < 3 && !pss->flow_controlled) {
			pss->flow_controlled = 1;
			lws_rx_flow_control(wsi, 0);
		}
		break;

	case LWS_CALLBACK_RECEIVE_PONG:
		pss->awaiting_pong = 0;
		pss->pending_ping = 0;
		{
			uint64_t now_ms = ez_ws_server_now_ms();
			pss->last_rx_ms = now_ms;
			pss->last_activity_ms = now_ms;
		}
		EZ_WS_DATA_LOGF("Pong received from client #%d\n",
		          pss->client_info ? pss->client_info->id : -1);
		break;

case LWS_CALLBACK_TIMER:
{
	uint64_t now_ms = ez_ws_server_now_ms();
	/* 直接使用 vhd 中的配置值，0 表示禁用 */
	uint32_t idle_timeout = vhd->idle_timeout_ms;
	uint32_t ping_timeout = vhd->ping_timeout_ms;
	uint32_t ping_interval = vhd->ping_interval_ms;
	uint32_t timer_interval = vhd->timer_interval_ms;
	
	/* 
	 * 关键修复：定时器触发时更新活动时间
	 * 
	 * 原因：libwebsockets 自动处理客户端发来的 ping 帧（自动回复 pong），
	 * 但不会触发应用层回调，所以 last_rx_ms/last_tx_ms 不会更新。
	 * 
	 * 解决方案：定时器能触发说明连接正常（底层可能有 ping/pong），
	 * 更新 last_activity_ms 避免误判为空闲。
	 */
	pss->last_activity_ms = now_ms;
	
	/* 空闲超时检测（如果 idle_timeout = 0 则禁用） */
	if (idle_timeout > 0 && now_ms - pss->last_activity_ms >= idle_timeout) {
		lwsl_warn("Client #%d idle for %llu ms, closing\n",
		          pss->client_info ? pss->client_info->id : -1,
		          (unsigned long long)(now_ms - pss->last_activity_ms));
		lws_close_reason(wsi, LWS_CLOSE_STATUS_NORMAL,
			(unsigned char *)"idle timeout",
			(uint16_t)strlen("idle timeout"));
		return -1;
	}

	/* 只有启用主动 ping 时才检查 pong 超时和发送 ping */
	if (ping_interval > 0) {
		if (pss->awaiting_pong) {
			if (now_ms - pss->last_ping_ms >= ping_timeout) {
				lwsl_warn("Client #%d pong timeout, closing\n",
				          pss->client_info ? pss->client_info->id : -1);
				lws_close_reason(wsi, LWS_CLOSE_STATUS_GOINGAWAY,
					(unsigned char *)"pong timeout",
					(uint16_t)strlen("pong timeout"));
				return -1;
			}
		} else if (now_ms - pss->last_ping_ms >= ping_interval) {
			pss->pending_ping = 1;
			pss->awaiting_pong = 1;
			pss->last_ping_ms = now_ms;
			lws_callback_on_writable(wsi);
		}
	}

	/* 重新设置定时器（如果 timer_interval = 0 则不设置） */
	if (timer_interval > 0) {
		lws_set_timer_usecs(wsi, timer_interval * 1000);
	}
	break;
}

	case LWS_CALLBACK_CLOSED:
		/* 从连接列表移除客户端 */
		if (pss && pss->client_info && vhd) {
			int client_id = pss->client_info->id;
			lwsl_user("LWS_CALLBACK_CLOSED: client #%d %s:%d disconnected (remaining: %d)\n",
			          client_id, pss->client_info->ip, pss->client_info->port, vhd->client_count - 1);

			/* 调用断开回调 */
			if (vhd->callbacks.on_disconnected) {
				vhd->callbacks.on_disconnected(client_id, vhd->callbacks.user_data);
			}
		} else {
			/* 连接在建立前就被关闭，可能是被拒绝的连接 */
			char name[128], rip[128];
			int fd = lws_get_socket_fd(wsi);
			if (fd >= 0) {
				lws_get_peer_addresses(wsi, fd, name, sizeof(name), rip, sizeof(rip));
				lwsl_user("LWS_CALLBACK_CLOSED: connection from %s closed before establishment (possibly rejected)\n", rip);
			} else {
				lwsl_user("LWS_CALLBACK_CLOSED: connection closed before establishment\n");
			}

			/* 对于被拒绝的连接，我们无法调用断开回调，因为没有client_id */
			/* 但我们可以记录这个事件 */
		}
		
		/* 安全地移除客户端（函数内部会检查pss和client_info） */
		if (vhd && pss) {
			remove_client_from_list(vhd, pss);
		}
		
		/* 清理unicast队列 - 只有在连接完全建立后才需要清理 */
		if (pss && pss->ring) {
			/* 清理unicast队列 */
			if (pss->unicast_queue) {
				pthread_mutex_lock(&pss->unicast_lock);
				for (size_t i = 0; i < pss->unicast_cap; ++i) {
					free(pss->unicast_queue[i]);
				}
				free(pss->unicast_queue);
				pss->unicast_queue = NULL;
				pthread_mutex_unlock(&pss->unicast_lock);
			}
			/* 即使unicast_queue为NULL，如果ring存在，unicast_lock也已经初始化，需要销毁 */
			if (pss->unicast_cap > 0) {
				pthread_mutex_destroy(&pss->unicast_lock);
			}
			
			lws_ring_destroy(pss->ring);
			pss->ring = NULL;
		}

		/* 处理选项标志（只有在vhd和options都有效时才处理） */
		if (vhd && vhd->options && *vhd->options & 1) {
			if (!*vhd->interrupted && pss) {
				*vhd->interrupted = 1 + pss->completed;
			}
			lws_cancel_service(lws_get_context(wsi));
		}
		break;

	default:
		break;
	}

	return 0;
}

#define LWS_PLUGIN_PROTOCOL_MINIMAL_SERVER_ECHO \
	{ \
		NULL, \
		callback_minimal_server_echo, \
		sizeof(struct per_session_data__minimal_server_echo), \
		1024, \
		0, NULL, 0 \
	}

/* 协议数组将在运行时设置 */
static struct lws_protocols *protocols = NULL;

static const struct lws_extension extensions[] = {
	{
		"permessage-deflate",
		lws_extension_callback_pm_deflate,
		"permessage-deflate"
		 "; client_no_context_takeover"
		 "; client_max_window_bits"
	},
	{ NULL, NULL, NULL /* terminator */ }
};


/*
 * 创建WebSocket服务端句柄
 */
struct ez_ws_server_handle *ez_ws_server_handle_create(struct ez_ws_server_config *config,
                                                        struct ez_ws_server_callbacks *callbacks)
{
	struct ez_ws_server_handle *handle;
	struct lws_context_creation_info info;
	static int interrupted = 0;
	static int options = 0;
	static const char *protocol = NULL;
	
	/* 库不提供默认配置，必须由应用层提供 */
	if (!config) {
		lwsl_err("Error: config is required, library does not provide defaults\n");
		return NULL;
	}
	
	handle = (struct ez_ws_server_handle *)malloc(sizeof(struct ez_ws_server_handle));
	if (!handle)
		return NULL;

	memset(handle, 0, sizeof(struct ez_ws_server_handle));
	handle->config = *config;
	handle->interrupted = &interrupted;

	/* 移除全局变量，通过 PVO 传递 handle */
	/* g_ws_server_handle = handle;  // 已废弃，改用 PVO 传递 */

	/* 保存回调函数到config中 */
	if (callbacks) {
		handle->config.callbacks = *callbacks;
	}

	/* 初始化 vhd 指针，将在 LWS_CALLBACK_PROTOCOL_INIT 中设置 */
	handle->vhd = NULL;
	
	/* protocol 是必须的，应用层必须明确指定 */
	if (!config->protocol) {
		lwsl_err("Error: protocol is required, must be specified by application\n");
		free(handle);
		return NULL;
	}
	
	/* 使用应用层指定的协议名称 */
	protocol = config->protocol;

	/* 分配协议数组 */
	protocols = (struct lws_protocols *)malloc(2 * sizeof(struct lws_protocols));
	if (!protocols) {
		free(handle->vhd);
		free(handle);
		return NULL;
	}
	protocols[0] = (struct lws_protocols){
		.name = protocol,
		.callback = callback_minimal_server_echo,
		.per_session_data_size = sizeof(struct per_session_data__minimal_server_echo),
		.rx_buffer_size = 1024,
		.id = 0,
		.user = NULL,
		.tx_packet_size = 0
	};
	protocols[1] = (struct lws_protocols){ NULL, NULL, 0, 0, 0, NULL, 0 };
	
	/* 创建WebSocket context */
	memset(&info, 0, sizeof(info));
	info.port = config->port;
	info.protocols = protocols;
	info.extensions = extensions;
	info.pt_serv_buf_size = 32 * 1024;
	info.options = LWS_SERVER_OPTION_VALIDATE_UTF8 |
		LWS_SERVER_OPTION_HTTP_HEADERS_SECURITY_BEST_PRACTICES_ENFORCE;
	
	/* 设置PVO - 动态分配以支持运行时协议名称 */
	/* 添加 server_handle 到 PVO 链表，实现多实例支持 */
	struct lws_protocol_vhost_options *pvo_server_handle = (struct lws_protocol_vhost_options *)malloc(sizeof(struct lws_protocol_vhost_options));
	struct lws_protocol_vhost_options *pvo_options = (struct lws_protocol_vhost_options *)malloc(sizeof(struct lws_protocol_vhost_options));
	struct lws_protocol_vhost_options *pvo_protocol = (struct lws_protocol_vhost_options *)malloc(sizeof(struct lws_protocol_vhost_options));
	struct lws_protocol_vhost_options *pvo_interrupted = (struct lws_protocol_vhost_options *)malloc(sizeof(struct lws_protocol_vhost_options));
	struct lws_protocol_vhost_options *pvo = (struct lws_protocol_vhost_options *)malloc(sizeof(struct lws_protocol_vhost_options));

	if (!pvo_server_handle || !pvo_options || !pvo_protocol || !pvo_interrupted || !pvo) {
		free(pvo);
		free(pvo_interrupted);
		free(pvo_protocol);
		free(pvo_options);
		free(pvo_server_handle);
		free(protocols);
		free(handle->vhd);
		free(handle);
		return NULL;
	}

	/* 构建 PVO 链表：server_handle -> options -> protocol -> interrupted */
	*pvo_server_handle = (struct lws_protocol_vhost_options){
		NULL, NULL, "server_handle", (void *)handle
	};
	*pvo_options = (struct lws_protocol_vhost_options){
		pvo_server_handle, NULL, "options", (void *)&options
	};
	*pvo_protocol = (struct lws_protocol_vhost_options){
		pvo_options, NULL, "protocol", (void *)&protocol
	};
	*pvo_interrupted = (struct lws_protocol_vhost_options){
		pvo_protocol, NULL, "interrupted", (void *)&interrupted
	};
	*pvo = (struct lws_protocol_vhost_options){
		NULL, pvo_interrupted, protocol, ""
	};
	info.pvo = pvo;

	/* 保存PVO指针以便后续清理 */
	handle->pvo_server_handle = pvo_server_handle;
	handle->pvo_options = pvo_options;
	handle->pvo_protocol = pvo_protocol;
	handle->pvo_interrupted = pvo_interrupted;
	handle->pvo = pvo;
	
	handle->context = lws_create_context(&info);
	if (!handle->context) {
		free(handle->vhd);
		free(handle);
		return NULL;
	}
	
	return handle;
}


/*
 * 清理WebSocket服务端
 */
void ez_ws_server_cleanup(struct ez_ws_server_handle *ws)
{
	if (!ws)
		return;
	
	/* 注意：线程停止逻辑现在在外部处理 */
	
	/* 销毁context */
	if (ws->context)
		lws_context_destroy(ws->context);

	/* 清理协议数组 */
	if (protocols) {
		free(protocols);
		protocols = NULL;
	}

	/* 清理PVO */
	if (ws->pvo) free(ws->pvo);
	if (ws->pvo_interrupted) free(ws->pvo_interrupted);
	if (ws->pvo_protocol) free(ws->pvo_protocol);
	if (ws->pvo_options) free(ws->pvo_options);
	if (ws->pvo_server_handle) free(ws->pvo_server_handle);

	/* 注意：vhd 由 libwebsockets 管理，不要手动释放 */

	free(ws);
	
	/* 不再需要清理全局变量（已废弃） */
}

/*
 * WebSocket服务执行函数
 */
int ez_ws_server_service_exec(struct ez_ws_server_handle *ws, int timeout_ms)
{
	if (!ws || !ws->context)
		return -1;
	
	int n = lws_service(ws->context, timeout_ms);
	return (n < 0) ? -1 : 0;
}

/*
 * 发送文本消息
 */
int ez_ws_server_send_text(struct ez_ws_server_handle *ws, int client_id, const char *data, size_t len)
{
	if (!ws || !ws->vhd)
		return EZ_WS_SERVER_ERR_INVALID_PARAM;
	
	if (!len)
		len = strlen(data);
	
	int ret = ws_server_send_internal(ws->vhd, client_id, data, len, 0);
	if (ret == -2)
		return EZ_WS_SERVER_ERR_CLIENT_NOT_FOUND;
	return ret;
}

/*
 * 发送二进制消息
 */
int ez_ws_server_send_binary(struct ez_ws_server_handle *ws, int client_id, const void *data, size_t len)
{
	if (!ws || !ws->vhd)
		return EZ_WS_SERVER_ERR_INVALID_PARAM;
	
	return ws_server_send_internal(ws->vhd, client_id, data, len, 1);
}

/*
 * 获取已连接的客户端数量
 */
int ez_ws_server_get_client_count(struct ez_ws_server_handle *ws)
{
	if (!ws || !ws->vhd)
		return -1;
	
	pthread_mutex_lock(&ws->vhd->client_list_lock);
	int count = ws->vhd->client_count;
	pthread_mutex_unlock(&ws->vhd->client_list_lock);
	
	return count;
}

/*
 * 检查服务是否正在运行
 */
int ez_ws_server_is_running(struct ez_ws_server_handle *ws)
{
	/* 由于线程逻辑移到外部，此函数现在返回context是否有效 */
	if (!ws)
		return 0;

	return ws->context ? 1 : 0;
}

/*
 * 遍历所有已连接的客户端
 */
int ez_ws_server_foreach_client(struct ez_ws_server_handle *ws, 
                                 ez_ws_server_foreach_client_cb callback,
                                 void *user_data)
{
	if (!ws || !ws->vhd || !callback)
		return EZ_WS_SERVER_ERR_INVALID_PARAM;
	
	pthread_mutex_lock(&ws->vhd->client_list_lock);
	
	struct client_info *client = ws->vhd->client_list;
	int ret = 0;
	
	while (client) {
		/* 填充客户端信息结构 */
		struct ez_ws_client_info info;
		info.id = client->id;
		strncpy(info.ip, client->ip, sizeof(info.ip) - 1);
		info.ip[sizeof(info.ip) - 1] = '\0';
		info.port = client->port;
		info.connect_time = client->connect_time;
		/* 获取最后活动时间（从 pss 中获取） */
		info.last_activity_ms = (client->pss && client->pss->last_activity_ms > 0) ?
		                        client->pss->last_activity_ms : 0;
		
		/* 调用回调函数 */
		ret = callback(&info, user_data);
		if (ret != 0)
			break;  /* 回调返回非0，停止遍历 */
		
		client = client->next;
	}
	
	pthread_mutex_unlock(&ws->vhd->client_list_lock);
	
	return EZ_WS_SERVER_OK;
}

