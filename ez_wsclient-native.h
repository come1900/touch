/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * ez_wsclient-native.h - Native Linux WebSocket Client Header
 *
 * Copyright (C) 2011 ezlibs.com, All Rights Reserved.
 *
 * $Id: ez_wsclient-native.h 1 2011-12-27 20:00:00Z WHF $
 *
 * Explain:
 *     Native Linux WebSocket client component header file.
 *     Uses epoll, timerfd, socket and other Linux basic functions.
 *     Does not depend on 3rd library.
 *
 * Update:
 *     2011-12-27 20:00:00 WHF Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#ifndef EZ_WSCLIENT_NATIVE_H
#define EZ_WSCLIENT_NATIVE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* WebSocket错误码定义 */
#define EZ_WS_OK                    0   /* 成功 */
#define EZ_WS_ERR_INVALID_PARAM    -1   /* 无效参数 */
#define EZ_WS_ERR_NOT_CONNECTED    -2   /* WebSocket未连接 */
#define EZ_WS_ERR_NOT_READY        -3   /* 未就绪 */
#define EZ_WS_ERR_NO_MEMORY        -4   /* 内存分配失败 */
#define EZ_WS_ERR_QUEUE_FULL       -5   /* 发送队列已满 */
#define EZ_WS_ERR_CONNECTING       -6   /* 正在连接中 */

/* WebSocket回调函数类型定义 */
typedef void (*ez_ws_on_receive_cb)(const void *data, size_t len, int is_binary, void *user_data);
typedef void (*ez_ws_on_connected_cb)(void *user_data);
typedef void (*ez_ws_on_disconnected_cb)(void *user_data);
typedef void (*ez_ws_on_sent_cb)(const void *data, size_t len, void *user_data);

/* WebSocket回调接口结构 */
struct ez_ws_callbacks {
	ez_ws_on_receive_cb on_receive;        /* 接收数据回调（必须） */
	ez_ws_on_connected_cb on_connected;    /* 连接建立回调（可选） */
	ez_ws_on_disconnected_cb on_disconnected; /* 连接断开回调（可选） */
	ez_ws_on_sent_cb on_sent;              /* 发送完成回调（可选） */
	void *user_data;                    /* 用户自定义数据 */
};

/* WebSocket连接状态 */
enum ez_ws_state {
	EZ_WS_STATE_DISCONNECTED,
	EZ_WS_STATE_CONNECTING,
	EZ_WS_STATE_HANDSHAKING,
	EZ_WS_STATE_CONNECTED,
	EZ_WS_STATE_CLOSING,
	EZ_WS_STATE_CLOSED
};

/* WebSocket客户端配置 */
struct ez_ws_client_config {
	/* 连接配置 */
	const char *url_path;          /* WebSocket URL 路径 */
	const char *protocol;          /* WebSocket 子协议名称 */
	const char *server_addr;       /* 服务器地址 */
	unsigned short port;           /* 服务器端口 */
	
	/* 重连策略配置 */
	uint16_t reconnect_backoff_enable;        /* 是否启用退避策略 */
	uint16_t reconnect_max_retries;      /* 最大重试次数（0表示不限制） */
	uint16_t reconnect_backoff_min_retries;     /* 前 N 次重试不增加延迟 */
	uint16_t reconnect_backoff_high_threshold;  /* 达到或超过此次数后不再增加延迟 */
	uint16_t reconnect_interval_ms;      /* 重连间隔（毫秒） */
};

/* WebSocket客户端句柄（不透明结构） */
struct ez_ws_client_handle;

/* 公共 API */

/**
 * 创建WebSocket客户端句柄
 * @param config 客户端配置（可以为NULL，使用默认配置）
 * @param callbacks 回调函数结构（可以为NULL）
 * @return 成功返回句柄指针，失败返回NULL
 * @note 创建后不会立即连接，连接操作将在 ez_ws_service_exec 中异步执行，避免阻塞
 */
struct ez_ws_client_handle *ez_ws_client_handle_create(struct ez_ws_client_config *config,
                                                  struct ez_ws_callbacks *callbacks);

/**
 * 清理WebSocket客户端
 * @param ws 客户端句柄
 * @note 清理前会自动停止连接，无需额外调用停止函数
 */
void ez_ws_client_cleanup(struct ez_ws_client_handle *ws);

/**
 * WebSocket服务执行函数 - 执行一次事件循环迭代
 * 该函数应该在外部线程中循环调用
 * @param ws 客户端句柄
 * @param timeout_ms 超时时间（毫秒），0表示使用默认超时
 * @return 0表示继续运行，-1表示应该停止
 */
int ez_ws_service_exec(struct ez_ws_client_handle *ws, int timeout_ms);

/**
 * 发送文本消息
 * @param ws 客户端句柄
 * @param data 文本数据
 * @param len 数据长度，0表示自动计算（以\0结尾的字符串）
 * @return EZ_WS_OK表示成功，其他值表示错误
 */
int ez_ws_send_text(struct ez_ws_client_handle *ws, const char *data, size_t len);

/**
 * 发送二进制消息
 * @param ws 客户端句柄
 * @param data 二进制数据
 * @param len 数据长度
 * @return EZ_WS_OK表示成功，其他值表示错误
 */
int ez_ws_send_binary(struct ez_ws_client_handle *ws, const void *data, size_t len);

/**
 * 检查连接状态
 * @param ws 客户端句柄
 * @return 1表示已连接，0表示未连接
 */
int ez_ws_is_connected(struct ez_ws_client_handle *ws);

/**
 * 获取连接状态
 * @param ws 客户端句柄
 * @return 连接状态枚举值
 */
enum ez_ws_state ez_ws_get_state(struct ez_ws_client_handle *ws);

#ifdef __cplusplus
}
#endif

#endif /* EZ_WSCLIENT_NATIVE_H */

