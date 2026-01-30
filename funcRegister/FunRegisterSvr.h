/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * FunRegisterSvr.h - device registration function layer (server)
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: FunRegisterSvr.h $
 *
 *  Explain:
 *     Device registration function layer for server side. Based on devWsRegister
 *     layer, implements business logic, maintains state, and calls device function
 *     interfaces. Handles device registration requests and validates device credentials.
 *
 *  Update:
 *     2013-09-08 18:45:12 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#ifndef FUN_REGISTER_SVR_H
#define FUN_REGISTER_SVR_H

#include <string>
#include <map>
#include "EZObject.h"
#include "../devWsRegister/DevWsRegisterSvr.h"
#include "../../come.1/come_1.h"
#include "../../come.1/json/come.1.json.h"

class CFunRegisterSvr : public CEZObject
{
public:
    CFunRegisterSvr();
    virtual ~CFunRegisterSvr();
    
    // 启动/停止
    void Start(unsigned short port);
    void Stop();
    
    // 注册回调：设备注册成功/失败（保留兼容性）
    typedef void (*OnDeviceRegisterProc_t)(int client_id, const std::string& device_id, 
                                            bool success, void *user_data);
    void SetRegisterCallback(OnDeviceRegisterProc_t proc, void *user_data);
    
private:
    bool m_started;
    OnDeviceRegisterProc_t m_on_device_register;
    void *m_user_data;
    
    // 设备信息（client_id -> device_id）
    std::map<int, std::string> m_client_devices;
    
    // WebSocket统一信号槽处理函数
    void OnWebsocketNotify(CDevWsRegisterSvr::SignalType sig_type, int client_id, const char *str_param, int int_param1, int int_param2, int int_param3);
    
    void handle_receive(int client_id, const char *data, size_t len);
    void handle_device_online(int client_id, const std::string& json_str);
    
    // 验证设备（简单实现：检查key是否匹配）
    bool verify_device(const std::string& device_id, const std::string& device_key);
};

#endif // FUN_REGISTER_SVR_H

