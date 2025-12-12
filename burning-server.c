
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdint.h>

#include <libwebsockets.h>
#include <ezutil/ez_system_api.h>
/* 开关：收发数据打印（1 开启，0 关闭）*/
#ifndef LWS_MIN_SERVER_ECHO_DATA_LOG
#define LWS_MIN_SERVER_ECHO_DATA_LOG 1
#endif

/* 控制台提示符 */
#ifndef LWS_MIN_SERVER_PROMPT
#define LWS_MIN_SERVER_PROMPT "srv> "
#endif

#if LWS_MIN_SERVER_ECHO_DATA_LOG
#define S_DATA_LOGF(...) lwsl_user(__VA_ARGS__)
#else
#define S_DATA_LOGF(...)
#endif

#define RING_DEPTH 4096

/* ================== WebSocket协议和路径配置 ================== */
#ifndef WS_SERVER_PROTOCOL
#define WS_SERVER_PROTOCOL "come.0"
#endif

#ifndef WS_SERVER_PATH_PREFIX
#define WS_SERVER_PATH_PREFIX "/come"
#endif

/* ================== 链路保活与超时策略 ================== */
/* 
 * 参数设计原则：
 * 1. TIMER_INTERVAL <= min(PING_TIMEOUT, PING_INTERVAL) / 2，确保及时检测超时
 * 2. PING_TIMEOUT < PING_INTERVAL，给客户端足够的响应时间（通常为PING_INTERVAL的1/3到1/2）
 * 3. PING_INTERVAL 通常设置为30-60秒，平衡网络开销和保活效果
 * 4. IDLE_TIMEOUT 应该远大于 PING_INTERVAL（通常为3-5倍），避免正常连接被误判为空闲
 * 
 * 当前配置说明：
 * - PING_INTERVAL: 30秒，每30秒发送一次心跳
 * - PING_TIMEOUT: 10秒，等待pong的最大时长（约为PING_INTERVAL的1/3）
 * - TIMER_INTERVAL: 3秒，每3秒检查一次（确保能及时检测10秒超时）
 * - IDLE_TIMEOUT: 180秒，无任何活动后断开（约为PING_INTERVAL的6倍）
 */
#define WS_SERVER_PING_INTERVAL_MS       30*1000   /* 心跳间隔：30秒（推荐30-60秒） */
#define WS_SERVER_PING_TIMEOUT_MS        10*1000   /* 等待pong最大时长：10秒（应小于PING_INTERVAL，推荐为1/3到1/2） */
#define WS_SERVER_IDLE_TIMEOUT_MS        180*1000  /* 无任何业务流量则断开：180秒（应远大于PING_INTERVAL，推荐3-5倍） */
#define WS_SERVER_TIMER_INTERVAL_MS      3*1000     /* 定时器检测周期：3秒（应 <= min(PING_TIMEOUT, PING_INTERVAL)/2，推荐2-5秒） */

static uint64_t
ws_server_now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

/* one of these created for each message */

struct msg {
	void *payload; /* is malloc'd */
	size_t len;
	char binary;
	char first;
	char final;
};

/* 前向声明 */
struct per_session_data__minimal_server_echo;

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

/* 前向声明 */
struct vhd_minimal_server_echo;

/* WebSocket服务端句柄（定义在console_data之前，以便console引用） */
struct ws_server_handle {
	struct lws_context *context;
	struct vhd_minimal_server_echo *vhd;  /* 虚拟主机数据，用于访问ws API */
	int *interrupted;
	
	/* 线程相关 */
	pthread_t ws_thread;     /* websocket运行线程 */
	int ws_running;          /* websocket线程运行标志 */
};

/* Console句柄 - 作为websocket的外部使用方 */
struct console_handle {
	pthread_t console_thread;
	int console_running;
	int *interrupted;  /* 指向全局中断标志 */
	
	/* 持有websocket句柄，用于调用websocket API */
	struct ws_server_handle *ws_handle;
};

struct vhd_minimal_server_echo {
	struct lws_context *context;
	struct lws_vhost *vhost;

	int *interrupted;
	int *options;
	const char **protocol;
	
	pthread_mutex_t broadcast_lock;
	
	/* 客户端连接列表 */
	struct client_info *client_list;
	pthread_mutex_t client_list_lock;
	int client_count;
	int next_client_id;  /* 下一个可用的客户端ID */
	
	/* WebSocket广播队列（循环缓冲区） - 独立于console */
	char **broadcast_queue;
	size_t broadcast_cap;
	size_t broadcast_write_index;  /* 写入位置（最新消息） */
	pthread_mutex_t broadcast_queue_lock;
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
 * 显示所有已连接的客户端信息
 */
static void
show_client_list(struct vhd_minimal_server_echo *vhd)
{
	if (!vhd)
		return;
	
	pthread_mutex_lock(&vhd->client_list_lock);
	
	lwsl_user("\n=== Connected Clients (%d) ===\n", vhd->client_count);
	if (vhd->client_count == 0) {
		lwsl_user("  No clients connected\n");
	} else {
		struct client_info *client = vhd->client_list;
		time_t now = time(NULL);
		
		lwsl_user("  %-4s %-6s %-20s %-8s %-15s\n", "No.", "ID", "IP", "Port", "Duration(s)");
		lwsl_user("  %s\n", "------------------------------------------------------------");
		
		int idx = 1;
		while (client) {
			time_t duration = now - client->connect_time;
			lwsl_user("  %-4d %-6d %-20s %-8d %-15ld\n", 
			          idx++, client->id, client->ip, client->port, (long)duration);
			client = client->next;
		}
	}
	lwsl_user("\n");
	
	pthread_mutex_unlock(&vhd->client_list_lock);
}

/*
 * WebSocket发送接口 - 向指定客户端或所有客户端发送消息
 * @vhd: websocket虚拟主机数据
 * @client_id: 客户端ID，-1表示广播到所有客户端
 * @data: 要发送的数据
 * @len: 数据长度
 * 返回0成功，-1失败，-2客户端未找到
 */
static int
ws_server_send_text(struct vhd_minimal_server_echo *vhd, int client_id, const char *data, size_t len)
{
    if (!vhd || !data)
        return -1;

    if (!len)
        len = strlen(data);

    /* 如果是广播到所有客户端 */
    if (client_id == -1) {
        pthread_mutex_lock(&vhd->broadcast_queue_lock);
        
        if (!vhd->broadcast_queue) {
            pthread_mutex_unlock(&vhd->broadcast_queue_lock);
            return -1;
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
            return -1;
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
        
        return 0;
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
        return -2;  /* 客户端未找到 */
    }
    
    /* 将消息加入客户端的unicast队列 */
    pthread_mutex_lock(&target_pss->unicast_lock);
    
    if (!target_pss->unicast_queue) {
        pthread_mutex_unlock(&target_pss->unicast_lock);
        return -1;
    }
    
    /* 检查队列是否已满 */
    if (target_pss->unicast_write_index - target_pss->unicast_read_index >= target_pss->unicast_cap) {
        pthread_mutex_unlock(&target_pss->unicast_lock);
        return -1;  /* 队列已满 */
    }
    
    /* 添加消息到队列 */
    size_t idx = target_pss->unicast_write_index % target_pss->unicast_cap;
    free(target_pss->unicast_queue[idx]);  /* 释放旧消息（如果有） */
    
    target_pss->unicast_queue[idx] = (char *)malloc(len + 1);
    if (!target_pss->unicast_queue[idx]) {
        pthread_mutex_unlock(&target_pss->unicast_lock);
        return -1;
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
    
    return 0;
}

/*
 * 将console消息入队到指定客户端的ring
 * 由WRITEABLE回调调用
 */
static int
server_enqueue_to_client(struct per_session_data__minimal_server_echo *pss,
                         const char *data, size_t len)
{
	struct msg amsg;
	
	if (!pss || !pss->ring || !data || !len)
		return -1;

	amsg.first = 1;
	amsg.final = 1;
	amsg.binary = 0;
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
            if (console->ws_handle && console->ws_handle->vhd) {
                show_client_list(console->ws_handle->vhd);
            } else {
                lwsl_user("WebSocket not initialized\n");
            }
            continue;
        }

        /* 处理status命令 */
        if (!strcmp(line, "status")) {
            if (console->ws_handle && console->ws_handle->vhd) {
                lwsl_user("\nServer Status:\n");
                lwsl_user("  Port: listening\n");
                lwsl_user("  Connected clients: %d\n", console->ws_handle->vhd->client_count);
                lwsl_user("  Broadcast queue: %zu messages in buffer\n", 
                          console->ws_handle->vhd->broadcast_write_index);
                lwsl_user("  Active: running\n");
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
            if (!console->ws_handle || !console->ws_handle->vhd) {
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
            size_t msg_len = strlen(cmd_ptr);
            int ret = ws_server_send_text(console->ws_handle->vhd, (int)client_id, cmd_ptr, msg_len);
            
            if (ret == 0) {
                lwsl_user("[server console] Message sent to client #%d\n", (int)client_id);
            } else if (ret == -2) {
                lwsl_user("[server console] Client #%d not found\n", (int)client_id);
            } else {
                lwsl_user("[server console] Send failed, error code: %d\n", ret);
            }
            continue;
        }

        /* 发送消息 - 使用websocket发送接口（广播） */
        if (console->ws_handle && console->ws_handle->vhd) {
            /* 默认广播到所有客户端 */
            int ret = ws_server_send_text(console->ws_handle->vhd, -1, line, len);
            if (ret == -1) {
                lwsl_user("[server console] Broadcast failed, queue full\n");
            } else if (ret == -2) {
                lwsl_user("[server console] Client not found\n");
            }
        } else {
            lwsl_user("[server console] WebSocket not initialized\n");
        }
    }

    console->console_running = 0;
    return NULL;
}

/* 全局websocket服务端句柄（用于在回调中访问） */
static struct ws_server_handle *g_ws_server_handle = NULL;

/* console句柄实例将在main中初始化，这里只声明静态变量 */
static struct console_handle console_handle_instance;

#include <assert.h>
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
			
			if (!vhd->interrupted || !vhd->options || !vhd->protocol) {
				lwsl_err("Error: Incomplete PVO configuration\n");
				return -1;
			}
			
			/* 将vhd保存到全局handle中，供console使用 */
			if (g_ws_server_handle) {
				g_ws_server_handle->vhd = vhd;
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
		/* 在握手完成前进行路径检查（更早拒绝不符合要求的连接） */
		{
			char uri[256];
			int uri_len = lws_hdr_total_length(wsi, WSI_TOKEN_GET_URI);
			
			if (uri_len > 0 && uri_len < sizeof(uri)) {
				n = lws_hdr_copy(wsi, uri, sizeof(uri), WSI_TOKEN_GET_URI);
				if (n > 0) {
					uri[n] = '\0';
					
					/* 验证路径是否以 WS_SERVER_PATH_PREFIX 开头 */
					if (strncmp(uri, WS_SERVER_PATH_PREFIX, strlen(WS_SERVER_PATH_PREFIX)) != 0) {
						char name[128], rip[128];
						int fd = lws_get_socket_fd(wsi);
						if (fd >= 0) {
							lws_get_peer_addresses(wsi, fd, name, sizeof(name), rip, sizeof(rip));
							lwsl_warn("Connection rejected: path '%s' does not start with %s (from %s)\n", 
							          uri, WS_SERVER_PATH_PREFIX, rip);
						} else {
							lwsl_warn("Connection rejected: path '%s' does not start with %s\n", 
							          uri, WS_SERVER_PATH_PREFIX);
						}
						/* 返回非0拒绝连接 */
						return 1;
					}
					
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
			uint64_t now_ms = ws_server_now_ms();
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
		
		/* 启动保活定时器 */
		lws_set_timer_usecs(wsi, WS_SERVER_TIMER_INTERVAL_MS * 1000);
		break;

	case LWS_CALLBACK_SERVER_WRITEABLE:

		lwsl_user("LWS_CALLBACK_SERVER_WRITEABLE\n");
		{
			uint64_t now_ms = ws_server_now_ms();
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
				lwsl_user("Ping sent to client #%d\n",
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
				S_DATA_LOGF("[Server] Unicast to client #%d: %s\n", 
				            pss->client_info ? pss->client_info->id : -1,
				            pss->unicast_queue[idx]);
				(void)server_enqueue_to_client(pss, pss->unicast_queue[idx], msg_len);
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
				S_DATA_LOGF("[Server] Broadcasting to client: %s\n", 
				            vhd->broadcast_queue[idx]);
				(void)server_enqueue_to_client(pss, vhd->broadcast_queue[idx], msg_len);
			}
			pss->last_broadcast_index++;
		}
		pthread_mutex_unlock(&vhd->broadcast_queue_lock);

		pmsg = lws_ring_get_element(pss->ring, &pss->tail);
		if (!pmsg) {
			lwsl_user(" (nothing in ring)\n");
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

		lwsl_user(" wrote %d: flags: 0x%x first: %d final %d\n",
				m, flags, pmsg->first, pmsg->final);
		pss->last_tx_ms = ws_server_now_ms();
		pss->last_activity_ms = pss->last_tx_ms;

		#if LWS_MIN_SERVER_ECHO_DATA_LOG
		{
			size_t show = pmsg->len > 512 ? 512 : pmsg->len;
			char buf[513];
			if (show)
				memcpy(buf, ((unsigned char *)pmsg->payload) + LWS_PRE, show);
			buf[show] = '\0';
			S_DATA_LOGF("SEND[%zu]: %s%s\n", (size_t)pmsg->len, buf, pmsg->len > show ? "..." : "");
		}
		#endif
		/*
		 * Workaround deferred deflate in pmd extension by only
		 * consuming the fifo entry when we are certain it has been
		 * fully deflated at the next WRITABLE callback.  You only need
		 * this if you're using pmd.
		 */
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

		lwsl_user("LWS_CALLBACK_RECEIVE: %4d (rpp %5d, first %d, "
			  "last %d, bin %d, msglen %d (+ %d = %d))\n",
			  (int)len, (int)lws_remaining_packet_payload(wsi),
			  lws_is_first_fragment(wsi),
			  lws_is_final_fragment(wsi),
			  lws_frame_is_binary(wsi), pss->msglen, (int)len,
			  (int)pss->msglen + (int)len);


		#if LWS_MIN_SERVER_ECHO_DATA_LOG
		{
			size_t show = len > 512 ? 512 : len;
			char buf[513];
			if (show)
				memcpy(buf, in, show);
			buf[show] = '\0';
			S_DATA_LOGF("RECV[%zu]: %s%s\n", len, buf, len > show ? "..." : "");
		}
		#endif
		/* 可选：保留十六进制转储用于底层排查 */
		// lwsl_hexdump_notice(in, len);

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
			uint64_t now_ms = ws_server_now_ms();
			pss->last_rx_ms = now_ms;
			pss->last_activity_ms = now_ms;
		}

		{
			char io_pto[1024] = {0};
			int m = snprintf(io_pto, sizeof(io_pto), "msglen: %d\n", pss->msglen);
			(void)&m;
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
			uint64_t now_ms = ws_server_now_ms();
			pss->last_rx_ms = now_ms;
			pss->last_activity_ms = now_ms;
		}
		lwsl_user("Pong received from client #%d\n",
		          pss->client_info ? pss->client_info->id : -1);
		break;

	case LWS_CALLBACK_TIMER:
	{
		uint64_t now_ms = ws_server_now_ms();
		if (now_ms - pss->last_activity_ms >= WS_SERVER_IDLE_TIMEOUT_MS) {
			lwsl_warn("Client #%d idle for %llu ms, closing\n",
			          pss->client_info ? pss->client_info->id : -1,
			          (unsigned long long)(now_ms - pss->last_activity_ms));
			lws_close_reason(wsi, LWS_CLOSE_STATUS_NORMAL,
				(unsigned char *)"idle timeout",
				(uint16_t)strlen("idle timeout"));
			return -1;
		}

		if (pss->awaiting_pong) {
			if (now_ms - pss->last_ping_ms >= WS_SERVER_PING_TIMEOUT_MS) {
				lwsl_warn("Client #%d pong timeout, closing\n",
				          pss->client_info ? pss->client_info->id : -1);
				lws_close_reason(wsi, LWS_CLOSE_STATUS_GOINGAWAY,
					(unsigned char *)"pong timeout",
					(uint16_t)strlen("pong timeout"));
				return -1;
			}
		} else if (now_ms - pss->last_ping_ms >= WS_SERVER_PING_INTERVAL_MS) {
			pss->pending_ping = 1;
			pss->awaiting_pong = 1;
			pss->last_ping_ms = now_ms;
			lws_callback_on_writable(wsi);
		}

		lws_set_timer_usecs(wsi, WS_SERVER_TIMER_INTERVAL_MS * 1000);
		break;
	}

	case LWS_CALLBACK_CLOSED:
		/* 从连接列表移除客户端 */
		/* 注意：如果连接在握手阶段被拒绝（如路径不匹配），client_info可能为NULL */
		if (pss && pss->client_info && vhd) {
			lwsl_user("LWS_CALLBACK_CLOSED: client #%d %s:%d disconnected (remaining: %d)\n",
			          pss->client_info->id, pss->client_info->ip, pss->client_info->port, vhd->client_count - 1);
		} else {
			lwsl_user("LWS_CALLBACK_CLOSED: connection closed before establishment\n");
		}
		
		/* 安全地移除客户端（函数内部会检查pss和client_info） */
		if (vhd && pss) {
			remove_client_from_list(vhd, pss);
		}
		
		/* 清理unicast队列 - 只有在连接完全建立后才需要清理 */
		/* 检查ring是否存在，如果不存在说明连接在ESTABLISHED之前就被关闭了 */
		if (pss && pss->ring) {
			/* 清理unicast队列 */
			/* 如果ring存在，说明LWS_CALLBACK_ESTABLISHED已经执行，unicast_lock已经初始化 */
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
			/* 检查unicast_cap是否非零（在ESTABLISHED中会被设置为32）来判断锁是否已初始化 */
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
		WS_SERVER_PROTOCOL, \
		callback_minimal_server_echo, \
		sizeof(struct per_session_data__minimal_server_echo), \
		1024, \
		0, NULL, 0 \
	}

///////////////////////////////////////////////////////////////////////////////

static struct lws_protocols protocols[] = {
	LWS_PLUGIN_PROTOCOL_MINIMAL_SERVER_ECHO,
	LWS_PROTOCOL_LIST_TERM
};

static int interrupted, port = 54321, options;
static const char *protocol = WS_SERVER_PROTOCOL;

/* pass pointers to shared vars to the protocol */

static const struct lws_protocol_vhost_options pvo_options = {
	NULL,
	NULL,
	"options",		/* pvo name */
	(void *)&options	/* pvo value */
};

static const struct lws_protocol_vhost_options pvo_protocol = {
	&pvo_options,
	NULL,
	"protocol",		/* pvo name */
	(void *)&protocol	/* pvo value */
};

static const struct lws_protocol_vhost_options pvo_interrupted = {
	&pvo_protocol,
	NULL,
	"interrupted",		/* pvo name */
	(void *)&interrupted	/* pvo value */
};

static const struct lws_protocol_vhost_options pvo = {
	NULL,				/* "next" pvo linked-list */
	&pvo_interrupted,	/* "child" pvo linked-list */
	WS_SERVER_PROTOCOL,	/* protocol name we belong to on this vhost */
	""				/* ignored */
};
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

void sigint_handler(int sig)
{
	(void)sig;
	interrupted = 1;
	if (g_ws_server_handle && g_ws_server_handle->context)
		lws_cancel_service(g_ws_server_handle->context);
}

/* 
 * ========================================================================
 * WebSocket服务端封装接口
 * ========================================================================
 */

/* WebSocket服务端配置结构 */
struct ws_server_config {
	int port;                      /* 监听端口 */
	const char *protocol;          /* 协议名称 */
	int options;                   /* 选项标志 */
};

/*
 * 初始化WebSocket服务端
 */
static struct ws_server_handle*
ws_server_init(struct ws_server_config *config, int *interrupted_flag)
{
	struct lws_context_creation_info info;
	struct ws_server_handle *handle;
	
	if (!config || !interrupted_flag)
		return NULL;
	
	handle = (struct ws_server_handle *)malloc(sizeof(struct ws_server_handle));
	if (!handle)
		return NULL;
	
	memset(handle, 0, sizeof(struct ws_server_handle));
	handle->interrupted = interrupted_flag;
	
	/* 设置全局handle（用于在回调中访问） */
	g_ws_server_handle = handle;
	
	/* 设置全局变量（兼容现有协议实现） */
	port = config->port;
	protocol = config->protocol;
	options = config->options;
	interrupted = 0;
	
	/* 创建WebSocket context */
	memset(&info, 0, sizeof(info));
	info.port = config->port;
	info.protocols = protocols;
	info.pvo = &pvo;
	info.extensions = extensions;
	info.pt_serv_buf_size = 32 * 1024;
	info.options = LWS_SERVER_OPTION_VALIDATE_UTF8 |
		LWS_SERVER_OPTION_HTTP_HEADERS_SECURITY_BEST_PRACTICES_ENFORCE;
	
	handle->context = lws_create_context(&info);
	if (!handle->context) {
		free(handle);
		return NULL;
	}
	
	return handle;
}

/*
 * WebSocket线程函数
 */
static void*
ws_server_thread_func(void *arg)
{
	struct ws_server_handle *handle = (struct ws_server_handle *)arg;
	int n = 0;
	
	if (!handle || !handle->context)
		return NULL;
	
	lwsl_user("[ws_thread] WebSocket thread started\n");
	handle->ws_running = 1;
	
	while (n >= 0 && handle->ws_running && !(*handle->interrupted)) {
		n = lws_service(handle->context, 0);
	}
	
	lwsl_user("[ws_thread] WebSocket thread exiting\n");
	handle->ws_running = 0;
	return NULL;
}

/*
 * 启动WebSocket服务端（在独立线程中运行）
 */
static int
ws_server_start(struct ws_server_handle *handle)
{
	if (!handle || !handle->context)
		return -1;
	
	handle->ws_running = 0;
	if (pthread_create(&handle->ws_thread, NULL, ws_server_thread_func, handle) != 0) {
		lwsl_err("Failed to create WebSocket thread\n");
		return -1;
	}
	
	return 0;
}

/*
 * 停止WebSocket服务端
 */
static void
ws_server_stop(struct ws_server_handle *handle)
{
	if (!handle)
		return;
	
	/* 设置停止标志 */
	handle->ws_running = 0;
	if (handle->context)
		lws_cancel_service(handle->context);
	
	/* 等待线程结束 */
	if (handle->ws_thread) {
		pthread_join(handle->ws_thread, NULL);
	}
}

/*
 * 清理WebSocket服务端
 */
static void
ws_server_cleanup(struct ws_server_handle *handle)
{
	if (!handle)
		return;
	
	/* 销毁context */
	if (handle->context)
		lws_context_destroy(handle->context);
	
	free(handle);
}

/*
 * 初始化Console（作为websocket的外部使用方）
 */
static struct console_handle*
console_init(struct ws_server_handle *ws_handle, int *interrupted_flag)
{
	struct console_handle *console;
	
	if (!ws_handle || !interrupted_flag)
		return NULL;
	
	console = &console_handle_instance;
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
}

int main(int argc, const char **argv)
{
	struct ws_server_handle *server_handle = NULL;
	struct console_handle *console = NULL;
	struct ws_server_config config;
	const char *p;
	int logs = LLL_USER | LLL_ERR | LLL_WARN | LLL_NOTICE;

	/* 1. 解析命令行参数 */
	if ((p = lws_cmdline_option(argc, argv, "-d")))
		logs = atoi(p);

	lws_set_log_level(logs, NULL);
	lwsl_user("LWS minimal ws server echo + permessage-deflate\n");
	lwsl_user("   lws-minimal-ws-server-echo [-p port] [--protocol name] [-o (once)]\n");

	/* 2. 配置WebSocket服务端参数 */
	config.port = 54321;  /* 默认端口 */
	config.protocol = WS_SERVER_PROTOCOL;  /* 默认协议名称 */
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

	/* 4. 初始化WebSocket服务端 */
	server_handle = ws_server_init(&config, &interrupted);
	if (!server_handle) {
		lwsl_err("WebSocket server init failed\n");
		return 1;
	}

	/* 5. 初始化Console（作为websocket的外部使用方） */
	console = console_init(server_handle, &interrupted);
	if (!console) {
		lwsl_err("Console init failed\n");
		ws_server_cleanup(server_handle);
		return 1;
	}

	/* 6. 启动WebSocket线程 */
	if (ws_server_start(server_handle) != 0) {
		lwsl_err("Failed to start WebSocket thread\n");
		console_cleanup(console);
		ws_server_cleanup(server_handle);
		return 1;
	}

	/* 7. 启动Console线程 */
	if (console_start(console) != 0) {
		lwsl_err("Failed to start console thread\n");
		ws_server_stop(server_handle);
		console_cleanup(console);
		ws_server_cleanup(server_handle);
		return 1;
	}

	lwsl_user("All threads started, entering main loop...\n");

	/* 8. 主循环：等待中断信号 */
	while (!interrupted) {
		sleep(1);  /* 每秒检查一次 */
	}

	/* 9. 清理资源 */
	lwsl_user("Shutting down server...\n");
	if (server_handle && server_handle->context)
		lws_cancel_service(server_handle->context);
	
	/* 停止线程 */
	console_stop(console);
	ws_server_stop(server_handle);
	
	/* 清理资源 */
	console_cleanup(console);
	ws_server_cleanup(server_handle);

	lwsl_user("Completed: %s\n", interrupted == 2 ? "OK" : "failed");

	return interrupted != 2;
}
