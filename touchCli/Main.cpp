/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/
/*
 * Main.cpp - touch client test program
 *
 * Copyright (C) 2013 - ezlibs.com -, All Rights Reserved.
 *
 * $Id: Main.cpp $
 *
 *  Explain:
 *     Touch client test program. Automatically registers device on startup.
 *
 *  Update:
 *     2013-09-25 19:45:33 Create
 */
/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include "../funcRegister/FunRegisterCli.h"
#include "EZThread.h"
#include "EZTimer.h"

static bool g_running = true;

void sigint_handler(int sig)
{
    (void)sig;
    g_running = false;
}

void on_register_result(bool success, const std::string& msg, void *user_data)
{
    (void)user_data;
    printf("Register result: %s, msg: %s\n", 
           success ? "SUCCESS" : "FAILED", msg.c_str());
}

int main(int argc, char *argv[])
{
    // 解析命令行参数
    const char *server_addr = "localhost";
    unsigned short port = 54321;
    const char *device_id = "device001";
    const char *device_key = "key001";
    const char *device_type = "touch";
    
    if (argc > 1) {
        server_addr = argv[1];
    }
    if (argc > 2) {
        port = (unsigned short)atoi(argv[2]);
    }
    if (argc > 3) {
        device_id = argv[3];
    }
    if (argc > 4) {
        device_key = argv[4];
    }
    if (argc > 5) {
        device_type = argv[5];
    }
    
    printf("touchCli: Connecting to %s:%d\n", server_addr, port);
    printf("Device ID: %s, Type: %s\n", device_id, device_type);
    
    // 初始化ezThread
    g_TimerManager.Start();
    g_ThreadManager.RegisterMainThread(ThreadGetID());
    
    // 注册信号处理
    signal(SIGINT, sigint_handler);
    
    // 创建注册功能对象
    CFunRegisterCli register_cli(device_id, device_key, device_type);
    register_cli.SetRegisterCallback(on_register_result, NULL);
    
    // 启动（会自动连接并注册）
    register_cli.Start(server_addr, port);
    
    // 主循环：等待中断
    printf("touchCli running, press Ctrl+C to exit...\n");
    while (g_running) {
        sleep(1);
    }
    
    // 停止
    printf("Shutting down...\n");
    register_cli.Stop();
    
    printf("touchCli exited\n");
    return 0;
}
