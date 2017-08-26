//
// Created by 黄胜 on 07/08/2017.
//
//客户端要把程序的md5和大小发过来，master维持一个分片信息的结构和md5对应的map

#ifndef MASTERV1_MASTER_H
#define MASTERV1_MASTER_H

#include <muduo/net/TcpServer.h>
#include <jsoncpp/json/json.h>
#include <map>

//chunkinfo是一个大文件对应的一系列chunk的信息
struct fileinfo
{
    std::string md5;
    uint64_t size;
    int chunknum;
};
class Master
{
public:
    Master(muduo::net::EventLoop* loop,const muduo::net::InetAddress& listenAddr);
    void start();
    void clientdownload(std::string& filename);
    void clientupload(std::string& filename, std::string& md5, uint64_t& size);
    void chunkinfo(std::string& ipport);
private:
    muduo::net::TcpServer server_;
    muduo::net::TcpConnectionPtr conn_;
    //用来通信的数据
    Json::Value data;
    Json::Value sendclient;
    Json::Value sendchunk;
    //每个md5，对应一个分片信息
    std::map<std::string,std::vector<std::string> > info;
    //每个文件名对应一个md5
    //std::map<std::string, fileinfo> filetomd5;
    //master存储的chunk信息，我们会根据这个来分割文件
    std::vector<std::string> chunkaddr;
    //每个文件对应的信息
    fileinfo filest;
    void onConnection(const muduo::net::TcpConnectionPtr& conn);
    void onMessage(const muduo::net::TcpConnectionPtr& conn,muduo::net::Buffer *buf);
};

#endif //MASTERV1_MASTER_H
