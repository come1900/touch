/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * DevWsRegisterSvr.cpp - WebSocket server communication layer implementation
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: DevWsRegisterSvr.cpp $
 *
 *  Explain:
 *     Implementation of WebSocket server communication layer.
 *
 *  Update:
 *     2013-12-03 22:10:15 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#include "DevWsRegisterSvr.h"
#include <stdio.h>
#include <string.h>

CDevWsRegisterSvr* CDevWsRegisterSvr::instance()
{
    // Meyers单例模式：使用局部静态变量，C++11及以后线程安全
    static CDevWsRegisterSvr s_instance;
    return &s_instance;
}

CDevWsRegisterSvr::CDevWsRegisterSvr()
    : CEZThread("CDevWsRegisterSvr", THREAD_PRIORITY_DEFAULT)
    , m_ws_handle(NULL)
    , m_SigNotify(2/*SIGNAL_NODE_NEW*/)
    , m_iUser(0)
{
}

CDevWsRegisterSvr::~CDevWsRegisterSvr()
{
    Stop();
}

bool CDevWsRegisterSvr::Start(unsigned short port, const char *protocol, const char *path_prefix)
{
    // 参数检查：端口范围 1-65535
    if (port == 0 || port > 65535) {
        return false;  // port 必须在有效范围内 (1-65535)
    }
    
    if (m_bLoop) {
        return true;  // 已经启动
    }
    
    // 配置WebSocket服务端
    struct ez_ws_server_config config = {0};
    config.port = (int)port;  // 转换为底层库需要的 int 类型
    config.protocol = protocol ? protocol : "come.1";
    config.path_prefix = path_prefix ? path_prefix : "/come";
    config.ip = NULL;  // 监听所有接口
    config.options = 0;
    config.ping_interval_ms = 30000;
    config.ping_timeout_ms = 10000;
    config.idle_timeout_ms = 180000;
    config.timer_interval_ms = 3000;
    config.ping_jitter_percent = 10;
    
    // 设置回调
    struct ez_ws_server_callbacks callbacks = {0};
    callbacks.on_receive = s_on_receive;
    callbacks.on_connected = s_on_connected;
    callbacks.on_disconnected = s_on_disconnected;
    callbacks.user_data = this;
    
    // 创建WebSocket服务端句柄
    m_ws_handle = ez_ws_server_handle_create(&config, &callbacks);
    if (!m_ws_handle) {
        return false;
    }
    
    // 启动ezThread自驱动线程
    if (CreateThread() != EZTHREAD_BOOL_TRUE) {
        ez_ws_server_cleanup(m_ws_handle);
        m_ws_handle = NULL;
        return false;
    }
    
    return true;
}

bool CDevWsRegisterSvr::Stop()
{
    if (!m_bLoop) {
        return true;
    }
    
    m_bLoop = EZTHREAD_BOOL_FALSE;
    DestroyThread(EZTHREAD_BOOL_TRUE);
    
    // 清理WebSocket服务端
    if (m_ws_handle) {
        ez_ws_server_cleanup(m_ws_handle);
        m_ws_handle = NULL;
    }
    
    return true;
}

bool CDevWsRegisterSvr::Start(CEZObject *pObj, DevWsRegisterSvrSignalProc_t pProc)
{
    CEZLock lock(m_MutexSigBuffer);
    
    int ret = m_SigNotify.Attach(pObj, pProc);
    if (ret >= 0 && m_iUser == 0) {
        // 第一次注册信号，确保WebSocket已启动
        // 注意：这里不自动启动，需要先调用 Start(port)
        m_iUser++;
    }
    
    return ret >= 0;
}

bool CDevWsRegisterSvr::Stop(CEZObject *pObj, DevWsRegisterSvrSignalProc_t pProc)
{
    CEZLock lock(m_MutexSigBuffer);
    
    if (m_iUser > 0) {
        m_iUser--;
    }
    
    int ret = m_SigNotify.Detach(pObj, pProc);
    return ret == 0;
}

int CDevWsRegisterSvr::SendText(int client_id, const char *data, size_t len)
{
    if (!m_ws_handle) {
        return -1;
    }
    return ez_ws_server_send_text(m_ws_handle, client_id, data, len);
}

int CDevWsRegisterSvr::SendBinary(int client_id, const void *data, size_t len)
{
    if (!m_ws_handle) {
        return -1;
    }
    return ez_ws_server_send_binary(m_ws_handle, client_id, data, len);
}

bool CDevWsRegisterSvr::IsReady() const
{
    if (!m_ws_handle) {
        return false;
    }
    return ez_ws_server_is_ready(m_ws_handle) != 0;
}

int CDevWsRegisterSvr::GetClientCount() const
{
    if (!m_ws_handle) {
        return 0;
    }
    return ez_ws_server_get_client_count(m_ws_handle);
}

void CDevWsRegisterSvr::ThreadProc()
{
    // ezThread 自驱动循环
    while (m_bLoop) {
        if (m_ws_handle) {
            int ret = ez_ws_server_service_exec(m_ws_handle, 100);
            if (ret < 0) {
                break;
            }
        } else {
            SystemSleep(100);  // 等待100ms
        }
    }
}

void CDevWsRegisterSvr::s_on_receive(int client_id, const void *data, size_t len, int is_binary, void *user_data)
{
    CDevWsRegisterSvr *self = (CDevWsRegisterSvr *)user_data;
    if (self) {
        CEZLock lock(self->m_MutexSigBuffer);
        // 触发统一信号：SIGNAL_RECEIVE, client_id, data(转为char*), len, is_binary, status(0=正常)
        self->m_SigNotify(SIGNAL_RECEIVE, client_id, (const char*)data, (int)len, is_binary, 0);
    }
}

void CDevWsRegisterSvr::s_on_connected(int client_id, const char *ip, int port, void *user_data)
{
    CDevWsRegisterSvr *self = (CDevWsRegisterSvr *)user_data;
    if (self) {
        CEZLock lock(self->m_MutexSigBuffer);
        // 触发统一信号：SIGNAL_CONNECTED, client_id, ip, port, status(0=成功), 0
        self->m_SigNotify(SIGNAL_CONNECTED, client_id, ip ? ip : "", port, 0, 0);
    }
}

void CDevWsRegisterSvr::s_on_disconnected(int client_id, void *user_data)
{
    CDevWsRegisterSvr *self = (CDevWsRegisterSvr *)user_data;
    if (self) {
        CEZLock lock(self->m_MutexSigBuffer);
        // 触发统一信号：SIGNAL_DISCONNECTED, client_id, NULL, error_code(0=正常断开), 0, 0
        self->m_SigNotify(SIGNAL_DISCONNECTED, client_id, NULL, 0, 0, 0);
    }
}

