/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * FunRegisterCli.cpp - device registration function layer implementation (client)
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: FunRegisterCli.cpp $
 *
 *  Explain:
 *     Implementation of device registration function layer for client side.
 *
 *  Update:
 *     2013-08-22 20:15:30 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#include "FunRegisterCli.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>

CFunRegisterCli::CFunRegisterCli(const std::string& device_id, const std::string& device_key, 
                                 const std::string& device_type)
    : m_device_id(device_id)
    , m_device_key(device_key)
    , m_device_type(device_type)
    , m_started(false)
    , m_on_register_result(NULL)
    , m_user_data(NULL)
{
}

CFunRegisterCli::~CFunRegisterCli()
{
    Stop();
}

void CFunRegisterCli::Start(const char *server_addr, unsigned short port)
{
    if (m_started) {
        return;
    }
    
    // 启动WebSocket客户端
    if (!g_DevWsRegisterCli.Start(server_addr, port, "/come", "come.1")) {
        return;
    }
    
    // 注册WebSocket统一信号槽
    g_DevWsRegisterCli.Start(this, (CDevWsRegisterCli::DevWsRegisterCliSignalProc_t)&CFunRegisterCli::OnWebsocketNotify);
    
    m_started = true;
}

void CFunRegisterCli::Stop()
{
    if (!m_started) {
        return;
    }
    
    // 注销WebSocket统一信号槽
    g_DevWsRegisterCli.Stop(this, (CDevWsRegisterCli::DevWsRegisterCliSignalProc_t)&CFunRegisterCli::OnWebsocketNotify);
    
    g_DevWsRegisterCli.Stop();
    
    m_started = false;
}

void CFunRegisterCli::SetRegisterCallback(OnRegisterResultProc_t proc, void *user_data)
{
    m_on_register_result = proc;
    m_user_data = user_data;
}

bool CFunRegisterCli::RegisterDevice(bool is_power_on)
{
    if (!m_started || !g_DevWsRegisterCli.IsConnected()) {
        return false;
    }
    
    // 生成随机nonce
    char nonce_buf[33];
    srand(time(NULL));
    snprintf(nonce_buf, sizeof(nonce_buf), "%08x%08x%08x%08x", 
             rand(), rand(), rand(), rand());
    std::string nonce(nonce_buf);
    
    // 构造DeviceOnline消息
    DeviceOnline msg(m_device_id, m_device_key, m_device_type, nonce);
    
    // 编码为JSON
    std::string json_str = ComeJsonCodec::encode(msg);
    if (json_str.empty()) {
        return false;
    }
    
    // 发送注册消息
    int ret = g_DevWsRegisterCli.SendText(json_str.c_str(), json_str.length());
    if (ret != 0) {
        return false;
    }
    
    return true;
}

void CFunRegisterCli::OnWebsocketNotify(CDevWsRegisterCli::SignalType sig_type, const void *data, size_t len, int param1, int param2)
{
    switch (sig_type) {
        case CDevWsRegisterCli::SIGNAL_RECEIVE:
            // param1=is_binary, param2=status
            if (param1 == 0) {  // 只处理文本消息
                handle_receive(data, len);
            }
            break;
            
        case CDevWsRegisterCli::SIGNAL_CONNECTED:
            // param1=status, param2=0
            // 连接建立后自动触发上电注册
            RegisterDevice(true);
            break;
            
        case CDevWsRegisterCli::SIGNAL_DISCONNECTED:
            // param1=error_code, param2=0
            // 连接断开，可以在这里处理重连逻辑
            break;
            
        default:
            break;
    }
}

void CFunRegisterCli::handle_receive(const void *data, size_t len)
{
    std::string json_str((const char *)data, len);
    
    // 尝试解析为AckDeviceOnline
    AckDeviceOnline ack;
    if (ComeJsonCodec::decode(json_str, ack)) {
        handle_register_ack(json_str);
        return;
    }
}

void CFunRegisterCli::handle_register_ack(const std::string& json_str)
{
    AckDeviceOnline ack;
    if (!ComeJsonCodec::decode(json_str, ack)) {
        return;
    }
    
    // 调用用户回调
    if (m_on_register_result) {
        m_on_register_result(ack.success && ack.code == 0, ack.msg, m_user_data);
    }
}

