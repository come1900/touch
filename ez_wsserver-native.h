/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * ez_wsserver-native.h - Native Linux WebSocket Server Header
 *
 * Copyright (C) 2011 ezlibs.com, All Rights Reserved.
 *
 * $Id: ez_wsserver-native.h 1 2011-12-27 20:00:00Z WHF $
 *
 * Explain:
 *     Native Linux WebSocket server component header file.
 *     Uses epoll, timerfd, socket and other Linux basic functions.
 *     Does not depend on 3rd library.
 *
 * Update:
 *     2011-12-27 20:00:00 WHF Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#ifndef EZ_WSSERVER_NATIVE_H
#define EZ_WSSERVER_NATIVE_H

#include <stdint.h>
#include <stddef.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* WebSocket错误码定义 */
#define EZ_WS_SERVER_OK                0   /* 成功 */
#define EZ_WS_SERVER_ERR_INVALID_PARAM -1   /* 无效参数 */
#define EZ_WS_SERVER_ERR_NOT_RUNNING   -2   /* 服务未运行 */
#define EZ_WS_SERVER_ERR_NO_MEMORY     -3   /* 内存分配失败 */
#define EZ_WS_SERVER_ERR_QUEUE_FULL    -4   /* 发送队列已满 */
#define EZ_WS_SERVER_ERR_CLIENT_NOT_FOUND -5 /* 客户端未找到 */

/* WebSocket回调函数类型定义 */
typedef void (*ez_ws_server_on_receive_cb)(int client_id, const void *data, size_t len, int is_binary, void *user_data);
typedef void (*ez_ws_server_on_connected_cb)(int client_id, const char *ip, int port, void *user_data);
typedef void (*ez_ws_server_on_disconnected_cb)(int client_id, void *user_data);

/* WebSocket回调接口结构 */
struct ez_ws_server_callbacks {
	ez_ws_server_on_receive_cb on_receive;        /* 接收数据回调（可选） */
	ez_ws_server_on_connected_cb on_connected;    /* 连接建立回调（可选） */
	ez_ws_server_on_disconnected_cb on_disconnected; /* 连接断开回调（可选） */
	void *user_data;                              /* 用户自定义数据 */
};

/* WebSocket服务端配置 */
struct ez_ws_server_config {
	/* 连接配置 */
	int port;                      /* 监听端口 */
	const char *protocol;          /* WebSocket 子协议名称（必须指定，不能为 NULL） */
	const char *path_prefix;       /* URL 路径前缀（如 "/come"），NULL 表示不检查路径，接受所有连接 */

	/* 保活配置（所有配置由应用层控制，库不提供默认值） */
	uint32_t ping_interval_ms;     /* 心跳间隔（毫秒），0 表示禁用服务端主动 ping */
	uint32_t ping_timeout_ms;      /* 等待 pong 最大时长（毫秒），0 表示禁用 pong 超时检测 */
	uint32_t idle_timeout_ms;      /* 无业务流量断开时间（毫秒），0 表示禁用空闲超时（不推荐） */
	uint32_t timer_interval_ms;    /* 定时器检测周期（毫秒），0 表示禁用定时器 */
	uint32_t ping_jitter_percent;  /* ping 间隔抖动百分比（0-50），0 表示禁用抖动，推荐 10% */

	/* 选项 */
	int options;                   /* 选项标志（保留，当前未使用） */
};

/* WebSocket服务端句柄（不透明结构） */
struct ez_ws_server_handle;

/* 公共 API */

/**
 * 创建WebSocket服务端句柄
 * @param config 服务端配置（可以为NULL，使用默认配置）
 * @param callbacks 回调函数结构（可以为NULL）
 * @return 成功返回句柄指针，失败返回NULL
 */
struct ez_ws_server_handle *ez_ws_server_handle_create(struct ez_ws_server_config *config,
                                                        struct ez_ws_server_callbacks *callbacks);

/**
 * 清理WebSocket服务端
 * @param ws 服务端句柄
 * @note 清理前会自动停止服务，无需额外调用停止函数
 */
void ez_ws_server_cleanup(struct ez_ws_server_handle *ws);

/**
 * WebSocket服务执行函数 - 执行一次事件循环迭代
 * 该函数应该在外部线程中循环调用
 * @param ws 服务端句柄
 * @param timeout_ms 超时时间（毫秒），此参数被忽略（内部使用智能调度）
 * @return 0表示继续运行，-1表示应该停止
 */
int ez_ws_server_service_exec(struct ez_ws_server_handle *ws, int timeout_ms);

/**
 * 发送文本消息到指定客户端
 * @param ws 服务端句柄
 * @param client_id 客户端ID，-1表示广播到所有客户端
 * @param data 文本数据
 * @param len 数据长度，0表示自动计算（以\0结尾的字符串）
 * @return EZ_WS_SERVER_OK表示成功，其他值表示错误
 */
int ez_ws_server_send_text(struct ez_ws_server_handle *ws, int client_id, const char *data, size_t len);

/**
 * 发送二进制消息到指定客户端
 * @param ws 服务端句柄
 * @param client_id 客户端ID，-1表示广播到所有客户端
 * @param data 二进制数据
 * @param len 数据长度
 * @return EZ_WS_SERVER_OK表示成功，其他值表示错误
 */
int ez_ws_server_send_binary(struct ez_ws_server_handle *ws, int client_id, const void *data, size_t len);

/**
 * 获取已连接的客户端数量
 * @param ws 服务端句柄
 * @return 客户端数量，-1表示错误
 */
int ez_ws_server_get_client_count(struct ez_ws_server_handle *ws);

/**
 * 检查服务是否正在运行
 * @param ws 服务端句柄
 * @return 1表示已准备好，0表示未准备好
 */
int ez_ws_server_is_ready(struct ez_ws_server_handle *ws);

/**
 * 客户端信息结构（用于遍历客户端列表）
 */
struct ez_ws_client_info {
	int id;              /* 客户端ID */
	char ip[64];         /* 客户端IP地址 */
	int port;            /* 客户端端口 */
	time_t connect_time; /* 连接时间戳 */
	uint64_t last_activity_ms; /* 最后活动时间（毫秒），0 表示未初始化 */
};

/**
 * 客户端列表遍历回调函数类型
 * @param client_info 客户端信息
 * @param user_data 用户自定义数据
 * @return 返回0继续遍历，返回非0停止遍历
 */
typedef int (*ez_ws_server_foreach_client_cb)(const struct ez_ws_client_info *client_info, void *user_data);

/**
 * 遍历所有已连接的客户端
 * @param ws 服务端句柄
 * @param callback 遍历回调函数
 * @param user_data 用户自定义数据
 * @return EZ_WS_SERVER_OK表示成功，其他值表示错误
 */
int ez_ws_server_foreach_client(struct ez_ws_server_handle *ws, 
                                 ez_ws_server_foreach_client_cb callback,
                                 void *user_data);

#ifdef __cplusplus
}
#endif

#endif /* EZ_WSSERVER_NATIVE_H */

