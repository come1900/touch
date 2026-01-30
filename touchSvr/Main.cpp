/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * Main.cpp - touch server test program
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: Main.cpp $
 *
 *  Explain:
 *     Touch server test program. Handles device registration requests.
 *
 *  Update:
 *     2013-10-30 20:55:22 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include "../funcRegister/FunRegisterSvr.h"
#include "EZThread.h"
#include "EZTimer.h"

static bool g_running = true;

void sigint_handler(int sig)
{
    (void)sig;
    g_running = false;
}

void on_device_register(int client_id, const std::string& device_id, bool success, void *user_data)
{
    (void)user_data;
    printf("Device register: client_id=%d, device_id=%s, success=%d\n", 
           client_id, device_id.c_str(), success);
}

int main(int argc, char *argv[])
{
    // 解析命令行参数
    unsigned short port = 54321;
    if (argc > 2 && strcmp(argv[1], "-p") == 0) {
        int port_val = atoi(argv[2]);
        if (port_val <= 0 || port_val > 65535) {
            fprintf(stderr, "Invalid port: %d (must be 1-65535)\n", port_val);
            return 1;
        }
        port = (unsigned short)port_val;
    }
    
    printf("touchSvr: Starting server on port %d\n", port);
    
    // 初始化ezThread
    g_TimerManager.Start();
    g_ThreadManager.RegisterMainThread(ThreadGetID());
    
    // 注册信号处理
    signal(SIGINT, sigint_handler);
    
    // 创建注册功能对象
    CFunRegisterSvr register_svr;
    register_svr.SetRegisterCallback(on_device_register, NULL);
    
    // 启动
    register_svr.Start(port);
    
    // 主循环：等待中断
    printf("touchSvr running, press Ctrl+C to exit...\n");
    while (g_running) {
        sleep(1);
    }
    
    // 停止
    printf("Shutting down...\n");
    register_svr.Stop();
    
    printf("touchSvr exited\n");
    return 0;
}
