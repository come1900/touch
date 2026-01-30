/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * FunRegisterCli.h - device registration function layer (client)
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: FunRegisterCli.h $
 *
 *  Explain:
 *     Device registration function layer for client side. Based on devWsRegister
 *     layer, implements business logic, maintains state, and calls device function
 *     interfaces. Supports automatic device registration on power-on, restart, and
 *     reconnection scenarios.
 *
 *  Update:
 *     2013-07-15 19:23:45 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#ifndef FUN_REGISTER_CLI_H
#define FUN_REGISTER_CLI_H

#include <string>
#include "EZObject.h"
#include "../devWsRegister/DevWsRegisterCli.h"
#include "../../come.1/come_1.h"
#include "../../come.1/json/come.1.json.h"

class CFunRegisterCli : public CEZObject
{
public:
    CFunRegisterCli(const std::string& device_id, const std::string& device_key, 
                    const std::string& device_type = "");
    virtual ~CFunRegisterCli();
    
    // 启动/停止
    void Start(const char *server_addr, unsigned short port);
    void Stop();
    
    // 注册回调：设备注册成功/失败（保留兼容性）
    typedef void (*OnRegisterResultProc_t)(bool success, const std::string& msg, void *user_data);
    void SetRegisterCallback(OnRegisterResultProc_t proc, void *user_data);
    
    // 手动触发注册（上电注册或重启注册）
    bool RegisterDevice(bool is_power_on = true);  // true=上电注册, false=重启注册
    
private:
    std::string m_device_id;
    std::string m_device_key;
    std::string m_device_type;
    
    bool m_started;
    OnRegisterResultProc_t m_on_register_result;
    void *m_user_data;
    
    // WebSocket统一信号槽处理函数
    void OnWebsocketNotify(CDevWsRegisterCli::SignalType sig_type, const void *data, size_t len, int param1, int param2);
    
    void handle_receive(const void *data, size_t len);
    void handle_register_ack(const std::string& json_str);
};

#endif // FUN_REGISTER_CLI_H

