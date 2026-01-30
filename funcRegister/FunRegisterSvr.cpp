/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * FunRegisterSvr.cpp - device registration function layer implementation (server)
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: FunRegisterSvr.cpp $
 *
 *  Explain:
 *     Implementation of device registration function layer for server side.
 *
 *  Update:
 *     2013-10-14 21:30:55 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#include "FunRegisterSvr.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

CFunRegisterSvr::CFunRegisterSvr()
    : m_started(false)
    , m_on_device_register(NULL)
    , m_user_data(NULL)
{
}

CFunRegisterSvr::~CFunRegisterSvr()
{
    Stop();
}

void CFunRegisterSvr::Start(unsigned short port)
{
    if (m_started) {
        return;
    }
    
    // 启动WebSocket服务端
    if (!g_DevWsRegisterSvr.Start(port, "come.1", "/come")) {
        return;
    }
    
    // 注册WebSocket统一信号槽
    g_DevWsRegisterSvr.Start(this, (CDevWsRegisterSvr::DevWsRegisterSvrSignalProc_t)&CFunRegisterSvr::OnWebsocketNotify);
    
    m_started = true;
}

void CFunRegisterSvr::Stop()
{
    if (!m_started) {
        return;
    }
    
    // 注销WebSocket统一信号槽
    g_DevWsRegisterSvr.Stop(this, (CDevWsRegisterSvr::DevWsRegisterSvrSignalProc_t)&CFunRegisterSvr::OnWebsocketNotify);
    
    g_DevWsRegisterSvr.Stop();
    
    m_client_devices.clear();
    m_started = false;
}

void CFunRegisterSvr::SetRegisterCallback(OnDeviceRegisterProc_t proc, void *user_data)
{
    m_on_device_register = proc;
    m_user_data = user_data;
}

void CFunRegisterSvr::OnWebsocketNotify(CDevWsRegisterSvr::SignalType sig_type, int client_id, const char *str_param, int int_param1, int int_param2, int int_param3)
{
    switch (sig_type) {
        case CDevWsRegisterSvr::SIGNAL_RECEIVE:
            // str_param=data(转为char*), int_param1=len, int_param2=is_binary, int_param3=status
            if (int_param2 == 0) {  // 只处理文本消息
                handle_receive(client_id, str_param, (size_t)int_param1);
            }
            break;
            
        case CDevWsRegisterSvr::SIGNAL_CONNECTED:
            // str_param=ip, int_param1=port, int_param2=status, int_param3=0
            // 连接建立，可以在这里记录客户端信息
            break;
            
        case CDevWsRegisterSvr::SIGNAL_DISCONNECTED:
            // str_param=NULL, int_param1=error_code, int_param2=0, int_param3=0
            // 清理设备信息
            m_client_devices.erase(client_id);
            break;
            
        default:
            break;
    }
}

void CFunRegisterSvr::handle_receive(int client_id, const char *data, size_t len)
{
    std::string json_str((const char *)data, len);
    
    // 尝试解析为DeviceOnline
    DeviceOnline msg;
    if (ComeJsonCodec::decode(json_str, msg)) {
        handle_device_online(client_id, json_str);
        return;
    }
}

void CFunRegisterSvr::handle_device_online(int client_id, const std::string& json_str)
{
    DeviceOnline msg;
    if (!ComeJsonCodec::decode(json_str, msg)) {
        return;
    }
    
    // 验证设备
    bool verified = verify_device(msg.id, msg.key);
    
    // 构造应答消息
    AckDeviceOnline ack;
    if (verified) {
        // 生成access_token（简单实现：使用device_id+timestamp）
        char token_buf[65];
        time_t now = time(NULL);
        snprintf(token_buf, sizeof(token_buf), "%s_%ld", msg.id.c_str(), now);
        std::string access_token(token_buf);
        
        ack = AckDeviceOnline(0, true, "Register success", 
                             access_token, "Bearer", 3600);
        
        // 保存设备信息
        m_client_devices[client_id] = msg.id;
    } else {
        ack = AckDeviceOnline(-1, false, "Device verification failed");
    }
    
    // 编码并发送应答
    std::string ack_json = ComeJsonCodec::encode(ack);
    if (!ack_json.empty()) {
        g_DevWsRegisterSvr.SendText(client_id, ack_json.c_str(), ack_json.length());
    }
    
    // 调用用户回调
    if (m_on_device_register) {
        m_on_device_register(client_id, msg.id, verified, m_user_data);
    }
}

bool CFunRegisterSvr::verify_device(const std::string& device_id, const std::string& device_key)
{
    // 简单实现：检查key是否非空
    if (device_id.empty() || device_key.empty()) {
        return false;
    }
    
    return true;  // 简单实现：只要非空就通过
}

