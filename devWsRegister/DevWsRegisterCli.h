/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * DevWsRegisterCli.h - WebSocket client communication layer (ezThread self-driven)
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: DevWsRegisterCli.h $
 *
 *  Explain:
 *     WebSocket client communication layer implementation. Provides reliable data
 *     communication link and notifies upper layer about data and connection status.
 *     Based on ezThread self-driven entity for automatic connection management.
 *
 *  Update:
 *     2013-07-28 19:52:18 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#ifndef DEV_WS_REGISTER_CLI_H
#define DEV_WS_REGISTER_CLI_H

#include <string>
#include "EZThread.h"
#include "EZSignals.h"
#include "ez_wsclient-native.h"

#define g_DevWsRegisterCli (*CDevWsRegisterCli::instance())

class CDevWsRegisterCli : public CEZThread
{
public:
    // 单例模式
    static CDevWsRegisterCli* instance();
    
    // 信号类型枚举
    enum SignalType {
        SIGNAL_RECEIVE = 0,      // 数据接收信号: sig_type, data, len, is_binary, status
        SIGNAL_CONNECTED = 1,    // 连接建立信号: sig_type, NULL, 0, status, 0
        SIGNAL_DISCONNECTED = 2  // 连接断开信号: sig_type, NULL, 0, error_code, 0
    };
    
    // 统一信号类型定义
    // 参数: SignalType sig_type, const void *data, size_t len, int param1, int param2
    // RECEIVE: sig_type=SIGNAL_RECEIVE, data=数据指针, len=数据长度, param1=is_binary, param2=status
    // CONNECTED: sig_type=SIGNAL_CONNECTED, data=NULL, len=0, param1=status, param2=0
    // DISCONNECTED: sig_type=SIGNAL_DISCONNECTED, data=NULL, len=0, param1=error_code, param2=0
    typedef TSignal5<SignalType, const void *, size_t, int, int> DevWsRegisterCliSignal_t;
    typedef DevWsRegisterCliSignal_t::SigProc DevWsRegisterCliSignalProc_t;
    
    // 启动/停止
    bool Start(const char *server_addr, unsigned short port, 
               const char *url_path = "/come", const char *protocol = "come.1");
    bool Stop();
    
    // 注册信号槽（统一接口）
    bool Start(CEZObject *pObj, DevWsRegisterCliSignalProc_t pProc);
    bool Stop(CEZObject *pObj, DevWsRegisterCliSignalProc_t pProc);
    
    // 发送消息
    int SendText(const char *data, size_t len = 0);
    int SendBinary(const void *data, size_t len);
    
    // 获取状态
    bool IsConnected() const;
    
    // ezThread 自驱动循环
    void ThreadProc();

private:
    CDevWsRegisterCli();
    virtual ~CDevWsRegisterCli();
    
    // 禁止拷贝
    CDevWsRegisterCli(const CDevWsRegisterCli&);
    CDevWsRegisterCli& operator=(const CDevWsRegisterCli&);
    
    struct ez_ws_client_handle *m_ws_handle;
    
    // 统一信号槽
    DevWsRegisterCliSignal_t m_SigNotify;
    CEZMutex m_MutexSigBuffer;
    
    // 计数
    int m_iUser;
    
    // 内部回调函数
    static void s_on_receive(const void *data, size_t len, int is_binary, void *user_data);
    static void s_on_connected(void *user_data);
    static void s_on_disconnected(void *user_data);
};

#endif // DEV_WS_REGISTER_CLI_H

