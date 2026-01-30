/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * DevWsRegisterCli.cpp - WebSocket client communication layer implementation
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: DevWsRegisterCli.cpp $
 *
 *  Explain:
 *     Implementation of WebSocket client communication layer.
 *
 *  Update:
 *     2013-08-05 20:38:27 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#include "DevWsRegisterCli.h"
#include <stdio.h>
#include <string.h>

CDevWsRegisterCli* CDevWsRegisterCli::instance()
{
    // Meyers单例模式：使用局部静态变量，C++11及以后线程安全
    static CDevWsRegisterCli s_instance;
    return &s_instance;
}

CDevWsRegisterCli::CDevWsRegisterCli()
    : CEZThread("CDevWsRegisterCli", THREAD_PRIORITY_DEFAULT)
    , m_ws_handle(NULL)
    , m_SigNotify(2/*SIGNAL_NODE_NEW*/)
    , m_iUser(0)
{
}

CDevWsRegisterCli::~CDevWsRegisterCli()
{
    Stop();
}

bool CDevWsRegisterCli::Start(const char *server_addr, unsigned short port, 
                              const char *url_path, const char *protocol)
{
    // 参数检查
    if (!server_addr || server_addr[0] == '\0') {
        return false;  // server_addr 不能为空
    }
    
    if (port == 0 || port > 65535) {
        return false;  // port 必须在有效范围内 (1-65535)
    }
    
    if (m_bLoop) {
        return true;  // 已经启动
    }
    
    // 配置WebSocket客户端
    struct ez_ws_client_config config = {0};
    config.server_addr = server_addr;
    config.port = port;
    config.url_path = url_path ? url_path : "/come";
    config.protocol = protocol ? protocol : "come.1";
    config.connect_timeout_ms = 5000;
    config.reconnect_interval_ms = 3000;
    config.reconnect_max_retries = 0;  // 无限重试
    config.reconnect_backoff_enable = 1;
    config.reconnect_backoff_min_retries = 2;
    config.reconnect_backoff_high_threshold = 5;
    
    // 设置回调
    struct ez_ws_callbacks callbacks = {0};
    callbacks.on_receive = s_on_receive;
    callbacks.on_connected = s_on_connected;
    callbacks.on_disconnected = s_on_disconnected;
    callbacks.user_data = this;
    
    // 创建WebSocket客户端句柄
    m_ws_handle = ez_ws_client_handle_create(&config, &callbacks);
    if (!m_ws_handle) {
        return false;
    }
    
    // 启动ezThread自驱动线程
    if (CreateThread() != EZTHREAD_BOOL_TRUE) {
        ez_ws_client_cleanup(m_ws_handle);
        m_ws_handle = NULL;
        return false;
    }
    
    return true;
}

bool CDevWsRegisterCli::Stop()
{
    if (!m_bLoop) {
        return true;
    }
    
    m_bLoop = EZTHREAD_BOOL_FALSE;
    DestroyThread(EZTHREAD_BOOL_TRUE);
    
    // 清理WebSocket客户端
    if (m_ws_handle) {
        ez_ws_client_cleanup(m_ws_handle);
        m_ws_handle = NULL;
    }
    
    return true;
}

bool CDevWsRegisterCli::Start(CEZObject *pObj, DevWsRegisterCliSignalProc_t pProc)
{
    CEZLock lock(m_MutexSigBuffer);
    
    int ret = m_SigNotify.Attach(pObj, pProc);
    if (ret >= 0 && m_iUser == 0) {
        // 第一次注册信号，确保WebSocket已启动
        // 注意：这里不自动启动，需要先调用 Start(server_addr, port)
        m_iUser++;
    }
    
    return ret >= 0;
}

bool CDevWsRegisterCli::Stop(CEZObject *pObj, DevWsRegisterCliSignalProc_t pProc)
{
    CEZLock lock(m_MutexSigBuffer);
    
    if (m_iUser > 0) {
        m_iUser--;
    }
    
    int ret = m_SigNotify.Detach(pObj, pProc);
    return ret == 0;
}

int CDevWsRegisterCli::SendText(const char *data, size_t len)
{
    if (!m_ws_handle) {
        return -1;
    }
    return ez_ws_send_text(m_ws_handle, data, len);
}

int CDevWsRegisterCli::SendBinary(const void *data, size_t len)
{
    if (!m_ws_handle) {
        return -1;
    }
    return ez_ws_send_binary(m_ws_handle, data, len);
}

bool CDevWsRegisterCli::IsConnected() const
{
    if (!m_ws_handle) {
        return false;
    }
    return ez_ws_is_connected(m_ws_handle) != 0;
}

void CDevWsRegisterCli::ThreadProc()
{
    // ezThread 自驱动循环
    while (m_bLoop) {
        if (m_ws_handle) {
            int ret = ez_ws_service_exec(m_ws_handle, 100);
            if (ret < 0) {
                break;
            }
        } else {
            SystemSleep(100);  // 等待100ms
        }
    }
}

void CDevWsRegisterCli::s_on_receive(const void *data, size_t len, int is_binary, void *user_data)
{
    CDevWsRegisterCli *self = (CDevWsRegisterCli *)user_data;
    if (self) {
        CEZLock lock(self->m_MutexSigBuffer);
        // 触发统一信号：SIGNAL_RECEIVE, data, len, is_binary, status(0=正常)
        self->m_SigNotify(SIGNAL_RECEIVE, data, len, is_binary, 0);
    }
}

void CDevWsRegisterCli::s_on_connected(void *user_data)
{
    CDevWsRegisterCli *self = (CDevWsRegisterCli *)user_data;
    if (self) {
        CEZLock lock(self->m_MutexSigBuffer);
        // 触发统一信号：SIGNAL_CONNECTED, NULL, 0, status(0=成功), 0
        self->m_SigNotify(SIGNAL_CONNECTED, NULL, 0, 0, 0);
    }
}

void CDevWsRegisterCli::s_on_disconnected(void *user_data)
{
    CDevWsRegisterCli *self = (CDevWsRegisterCli *)user_data;
    if (self) {
        CEZLock lock(self->m_MutexSigBuffer);
        // 触发统一信号：SIGNAL_DISCONNECTED, NULL, 0, error_code(0=正常断开), 0
        self->m_SigNotify(SIGNAL_DISCONNECTED, NULL, 0, 0, 0);
    }
}

