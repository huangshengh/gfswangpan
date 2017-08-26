//
// Created by 黄胜 on 10/08/2017.
//

#ifndef CLIENT_CLIENT_H
#define CLIENT_CLIENT_H

#include <jsoncpp/json/json.h>
#include <muduo/base/Logging.h>
#include <muduo/net/Endian.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpClient.h>
#include <muduo/base/Mutex.h>
#include <muduo/net/EventLoopThread.h>
#include <boost/bind.hpp>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <utility>
#include <iostream>
#include <stdio.h>

using namespace muduo;
using namespace muduo::net;

class ClienttoChunk;

class Client : boost::noncopyable
{
public:
    Client(EventLoop* loop, const InetAddress& serverAddr,EventLoop* chunkloop);
    void connect();
    void disconnect();
    void parse(const TcpConnectionPtr& conn,Buffer* buffer);//onmessage里面解析json
    void upload(const TcpConnectionPtr& conn);
    void uploadprotocol(const TcpConnectionPtr& conn,std::string path);
    void downloadprotocol(const TcpConnectionPtr& conn);
    void parseaddr(std::vector<std::string> & addr);
    void downparse(std::vector<std::string> & addr,long long length);
    std::string filename;//download／upload的文件名字或者是path
    std::string method;//对于master而言，是download还是upload
    void sendtomaster();
    void timecall();
private:
    TcpClient client_;
    TcpConnectionPtr connection_;
    std::string md;
    std::string md5_;//下载的时候用的md5
    //Json::Value root;
    EventLoop* saveloop;
    uint64_t size;
    std::vector<ClienttoChunk* > clientvector;

    int flag;//表示协议和数据的标志
    //用来存放chunk服务器ipport
    std::vector<std::string> chunkaddr;
    std::vector<std::vector<std::string> > ipport;
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buffer);
    void onWriteComplete(const TcpConnectionPtr& conn);
};

class ClienttoChunk
{
public:
    ClienttoChunk(EventLoop* loop,const InetAddress& serverAddr,std::string md5,std::string totalmd5,std::string method,std::string filename,std::FILE* fp,uint64_t chunksize,long long offset);
    //ClienttoChunk(EventLoop* loop,const InetAddress& serverAddr,std::string md5, std::FILE* fp,std::string method);
    void connect();
    void disconnect();
    void chunkupload();
    void dataupload();
    void datadownload();
    void chunkdownload();
    std::string method_;
    std::string filename;
    int newflag;
    //int updo;
private:
    TcpClient chunckclient_;
    TcpConnectionPtr chunkconn_;
    std::string md_;
    std::string totalmd5_;
    Json::Value sendtochunk_;
    std::FILE* fp_;
    long long offset_;
    long long chunksize_;//这个size也有用，chunk收到这么多size的数据之后就可以发回信息了。
    int nread_;//用来表示什么时候发完。
    int nwrite_;


    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn,Buffer* buffer);
    void onWriteComplete(const TcpConnectionPtr& conn);

};
#endif //CLIENT_CLIENT_H
