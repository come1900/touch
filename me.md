# 程序说明
## 版本3
### 文件
minimal-ws-client-echo-reconn.c
### 修改
#### minimal-ws-client-echo-reconn.c
lws_extension extensions 将扩展特性明确

## 版本2
### 文件
minimal-ws-client-echo-console.c  
minimal-ws-server-echo-console.c

### 修改
#### minimal-ws-client-echo-console.c
    1、定时发送功能： sul_tx_cb
    2、console发送功能：console_thread_func
    3、客户端和服务端链路正常：i.protocol = "fws_a-burning";

## 基本版本
### 文件
minimal-ws-client-echo.c protocol_lws_minimal_client_echo.h
minimal-ws-server-echo.c protocol_lws_minimal_server_echo.h

来源
@ libwebsockets-v4.4.1
   cp ./minimal-examples-lowlevel/ws-server/minimal-ws-server-echo/*.c /home/10312200@zte.intra/svn/daily/src/library/3rd-libwebsockets/tutorials/wsEcho/
   cp ./minimal-examples-lowlevel/ws-client/minimal-ws-client-echo/*.c /home/10312200@zte.intra/svn/daily/src/library/3rd-libwebsockets/tutorials/wsEcho/
@ wsEcho
   mv protocol_lws_minimal_client_echo.c protocol_lws_minimal_client_echo.h
   mv protocol_lws_minimal_server_echo.c protocol_lws_minimal_server_echo.h


minimal-ws-client-echo-console.c  minimal-ws-server-echo-console.c
这两个文件是websocket通讯的客户端和服务端， 请把pthread_create(&vhd->console_thread, NULL, console_thread_func, vhd);移到外部， console终端不要依赖websocket过程

