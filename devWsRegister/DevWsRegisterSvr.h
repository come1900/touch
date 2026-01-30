/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * DevWsRegisterSvr.h - WebSocket server communication layer (ezThread self-driven)
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: DevWsRegisterSvr.h $
 *
 *  Explain:
 *     WebSocket server communication layer implementation. Provides reliable data
 *     communication link and notifies upper layer about data and connection status.
 *     Based on ezThread self-driven entity for automatic connection management.
 *
 *  Update:
 *     2013-11-12 18:20:40 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#ifndef DEV_WS_REGISTER_SVR_H
#define DEV_WS_REGISTER_SVR_H

#include <string>
#include "EZThread.h"
#include "EZSignals.h"
#include "ez_wsserver-native.h"

#define g_DevWsRegisterSvr (*CDevWsRegisterSvr::instance())

class CDevWsRegisterSvr : public CEZThread
{
public:
    // 单例模式
    static CDevWsRegisterSvr* instance();
    
    // 信号类型枚举
    enum SignalType {
        SIGNAL_RECEIVE = 0,      // 数据接收信号: sig_type, client_id, data, len, is_binary, status
        SIGNAL_CONNECTED = 1,    // 连接建立信号: sig_type, client_id, ip_str, port, status, 0
        SIGNAL_DISCONNECTED = 2  // 连接断开信号: sig_type, client_id, NULL, error_code, 0, 0
    };
    
    // 统一信号类型定义
    // 参数: SignalType sig_type, int client_id, const char *str_param, int int_param1, int int_param2, int int_param3
    // RECEIVE: sig_type=SIGNAL_RECEIVE, client_id=客户端ID, str_param=数据指针(转为char*), int_param1=len, int_param2=is_binary, int_param3=status
    // CONNECTED: sig_type=SIGNAL_CONNECTED, client_id=客户端ID, str_param=ip, int_param1=port, int_param2=status, int_param3=0
    // DISCONNECTED: sig_type=SIGNAL_DISCONNECTED, client_id=客户端ID, str_param=NULL, int_param1=error_code, int_param2=0, int_param3=0
    typedef TSignal6<SignalType, int, const char *, int, int, int> DevWsRegisterSvrSignal_t;
    typedef DevWsRegisterSvrSignal_t::SigProc DevWsRegisterSvrSignalProc_t;
    
    // 启动/停止
    bool Start(unsigned short port, const char *protocol = "come.1", const char *path_prefix = "/come");
    bool Stop();
    
    // 注册信号槽（统一接口）
    bool Start(CEZObject *pObj, DevWsRegisterSvrSignalProc_t pProc);
    bool Stop(CEZObject *pObj, DevWsRegisterSvrSignalProc_t pProc);
    
    // 发送消息
    int SendText(int client_id, const char *data, size_t len = 0);  // client_id=-1表示广播
    int SendBinary(int client_id, const void *data, size_t len);
    
    // 获取状态
    bool IsReady() const;
    int GetClientCount() const;
    
    // ezThread 自驱动循环
    void ThreadProc();

private:
    CDevWsRegisterSvr();
    virtual ~CDevWsRegisterSvr();
    
    // 禁止拷贝
    CDevWsRegisterSvr(const CDevWsRegisterSvr&);
    CDevWsRegisterSvr& operator=(const CDevWsRegisterSvr&);
    
    struct ez_ws_server_handle *m_ws_handle;
    
    // 统一信号槽
    DevWsRegisterSvrSignal_t m_SigNotify;
    CEZMutex m_MutexSigBuffer;
    
    // 计数
    int m_iUser;
    
    // 内部回调函数
    static void s_on_receive(int client_id, const void *data, size_t len, int is_binary, void *user_data);
    static void s_on_connected(int client_id, const char *ip, int port, void *user_data);
    static void s_on_disconnected(int client_id, void *user_data);
};

#endif // DEV_WS_REGISTER_SVR_H

