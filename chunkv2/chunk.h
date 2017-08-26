//
// Created by 黄胜 on 08/08/2017.
//

#ifndef CHUNK_CHUNK_H
#define CHUNK_CHUNK_H

#include <muduo/net/TcpServer.h>
#include <jsoncpp/json/json.h>
#include <muduo/net/TcpClient.h>
#include <string>
#include <list>
#include "md5.h"
#include <sstream>

class Chunk
{
public:
    Chunk(muduo::net::EventLoop* loop,const muduo::net::InetAddress& listenAddr);
    void start();
    //void openfd();
    void checkmd5();
    void sendclient(const muduo::net::TcpConnectionPtr& conn);
    void senddata(const muduo::net::TcpConnectionPtr& conn,std::FILE* fp);
    //void getipport();
private:
    muduo::net::TcpServer server_;
    //每个md5对应一系列的文件分片的名字
    std::map<std::string,std::vector<std::string> > chunkinfo;
    //json就是用来通信协议的
    Json::Value data;
    std::FILE* fp_;
    muduo::net::TcpConnectionPtr conn_;
    std::string method;
    int size;
    std::string md;//chunkmd
    //std::map<std::string,std::list<std::string> > filetochunk;
    //muduo::string ipport;
    void onConnection(const muduo::net::TcpConnectionPtr& conn);
    void onMessage(const muduo::net::TcpConnectionPtr& conn,muduo::net::Buffer *buf);
    void receive(const muduo::net::TcpConnectionPtr& conn,muduo::net::Buffer* buf,int fd,int& size);
    void onWriteComplete(const muduo::net::TcpConnectionPtr& conn);
};



//chunkclient 很简单就是和master通信，master给chunkclient传md5，md5对应着分片的信息。
//chunkclient 只和master痛惜，发送自己的ipport，收到的是master给的分片信息和md5.
class ChunkClient
{

public:
    ChunkClient(muduo::net::EventLoop* loop, const muduo::net::InetAddress& sereverAddr);
    void connect();
    void disconnect();
    void sendipport();

private:
    muduo::net::TcpClient client_;
    muduo::net::TcpConnectionPtr connect_;
    Json::Value sendMaster;
    muduo::string ipport;
    void onConnection(const muduo::net::TcpConnectionPtr& conn);
    void onMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer* buf);
};

#endif //CHUNK_CHUNK_H
