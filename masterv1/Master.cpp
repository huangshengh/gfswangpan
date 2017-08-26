//
// Created by 黄胜 on 07/08/2017.
//

#include "Master.h"
#include <sstream>
#include <iostream>
#include <boost/bind.hpp>
#include <muduo/base/Logging.h>
#include <muduo/net/EventLoop.h>

static std::map<std::string, fileinfo> filetomd5;

Master::Master(muduo::net::EventLoop *loop, const muduo::net::InetAddress& listenAddr)
:server_(loop,listenAddr,"Master")
{
    server_.setConnectionCallback(boost::bind(&Master::onConnection,this,_1));
    server_.setMessageCallback(boost::bind(&Master::onMessage,this,_1,_2));
}

void Master::start()
{
    server_.start();
}

void Master::onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    LOG_INFO<< "Master connnection h";
    if(conn->connected())
        conn_ = conn;
    else
        conn_.reset();
}

void Master::clientdownload(std::string &filename)
{
    fileinfo filemd5;
    if(filetomd5.count(filename))
        filemd5 = filetomd5[filename];
    else
    {
        LOG_INFO << "没有这个文件 ";
        sendclient["who"] = "Master";
        sendclient["operate"] = "download";
        sendclient["size"] = 0;
        Json::FastWriter writer;
        conn_->send(writer.write(sendclient));
        return;
    }
    //找到md5之后，client得到chunk的信息，从chunk下载
    //得到的是一个chunk服务器的数组。
    std::vector<std::string> chunkinfo = info[filemd5.md5];

    //把数据发送到client上 ,我们只发送了chunk服务器的地址，每个chunk服务器里面都会有md5；
    sendclient["who"] = "Master";
    sendclient["operate"] = "download";
    sendclient["chunknum"] =(int)filemd5.chunknum;
    sendclient["md5"] = filemd5.md5;//client根据这个md5去chunk下载文件。
    sendclient["size"] = (Json::UInt64)filemd5.size;
    for(int i=0;i<filemd5.chunknum;++i)
    {
        std::string str;
        std::stringstream ss;
        ss << i;
        ss >> str;
        sendclient[str.c_str()] = chunkinfo[i].c_str();
    }
    LOG_INFO<<" found the file, start to download ";
    Json::FastWriter writer;
    conn_->send(writer.write(sendclient));
}

//处理收到的chunk的ipport
void Master::chunkinfo(std::string& ipport)
{
    //std::cout<<"ipport is: "<< ipport <<std::endl;
    chunkaddr.push_back(ipport);
}


//filetomd5 是文件名对应文件信息（md5，size，chunknum）
//info是md5对应的一系列chunk服务器

//在upload里面添加信息，在download里面查找信息。完成
void Master::clientupload(std::string &filename, std::string &md5, uint64_t &size)
{
    //std::cout<<" client upload"<< std::endl;
    //fileinfo filest;
    std::string str;

    //有相同的文件名，看看有没有相同的md5，我们现在假设同名的文件就是同一个文件。
    //不会有文件名重复的情况
    if(filetomd5.count(filename))
    {
        filest = filetomd5[filename];
        //有相同的md5，所以文件相同。那就秒传了
        if(filest.md5 == md5)
        {
            sendclient["who"] = "Master";
            sendclient["operate"] = "upload";
            sendclient["size"] = (Json::UInt64)size;
            sendclient["status"] = "done";
        }
        //不考虑文件名相同但是md5不同的情况
    }
    else//文件不存在
    {
        //std::cout<<"文件不存在"<<std::endl;
        filest.md5 = md5;
        filest.size = size;
        int num = size/67108864;
        if(num* 67108864 < size)
            num += 1;
        filest.chunknum = num;//要分这么多片
        std::cout << " 分片数量 num is "<< num << std::endl;
        //先不加，client发过来done之后再加
        filetomd5[filename] = filest;//把这个文件的信息加入到master的filetomd5
        sendclient["who"] = "Master";
        sendclient["operate"] = "upload";
        sendclient["size"] = (Json::UInt64)size;
        sendclient["status"] = "sendchunkinfo";
        sendclient["chunknum"] = num;
        //从chunkaddr选择chunk，保存到info里面，发送给client
        for(int i=0,j=0;i<num;++i)
        {
            if (j > chunkaddr.size() - 1)//size 一般是2，只有两个chunk服务器
                j = 0;
            info[md5].push_back(chunkaddr[j]);
            std::string str;
            std::stringstream ss;
            ss << i;
            ss >> str;
            sendclient[str.c_str()] = chunkaddr[j].c_str();
            ++j;
        }
    }
    //把json消息发给client。
    Json::FastWriter writer;
    conn_->send(writer.write(sendclient));
}

//处理到来的消息。client-master， chunk-master
void Master::onMessage(const muduo::net::TcpConnectionPtr& conn,muduo::net::Buffer* buf)
{
    LOG_INFO<<"   Master收到数据。 ";
    const char* msg(buf->peek());
    Json::Reader reader;
    std::string user;
    std::string operate;
    std::string filename;
    std::string md5;
    uint64_t size;
    if(reader.parse(msg,data)) {
        user = data["who"].asString();
        operate = data["operate"].asString();
        LOG_INFO << " 操作 "<<operate;
        md5 = data["md5"].asString();
        filename = data["filename"].asString();
        LOG_INFO<< "文件名" <<filename;
        size = data["size"].asUInt64();
    }
    if(user=="Client"&&operate=="download")
        clientdownload(filename);
    else if(user=="Client"&&operate=="upload")
        clientupload(filename,md5,size);
    else if(user == "Client"&&operate =="updone")
        {
            //filetomd5[filename] = filest;
            LOG_INFO<<"添加文件";
            return;
        }
    else if(user =="ChunkClient")
        chunkinfo(operate);
    else
        std::cout<<" not now "<<std::endl;
}

int main()
{
    muduo::net::EventLoop loop;
    muduo::net::InetAddress listenAddr(2018);
    Master server(&loop,listenAddr);
    server.start();
    loop.loop();
}



