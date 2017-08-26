//
// Created by 黄胜 on 08/08/2017.
//

#include "chunk.h"
#include <unistd.h>
#include <fcntl.h>
#include <boost/bind.hpp>
#include <muduo/base/Logging.h>
#include <muduo/net/EventLoop.h>

const int kBufSize = 1024*1024;
typedef boost::shared_ptr<FILE> FilePtr;
static std::map<std::string,std::list<std::string> > filetochunk;

struct perconn
{
    int recv;
    int fd;
    int chunksize;
    int usesize;
    std::string filename;
    std::string md5;
};

typedef boost::shared_ptr<perconn> perptr;

Chunk::Chunk(muduo::net::EventLoop *loop, const muduo::net::InetAddress& listenAddr)
        :server_(loop,listenAddr,"Chunk Server")
{
    server_.setConnectionCallback(boost::bind(&Chunk::onConnection,this,_1));
    server_.setMessageCallback(boost::bind(&Chunk::onMessage,this,_1,_2));
    server_.setWriteCompleteCallback(boost::bind(&Chunk::onWriteComplete,this,_1));
}

void Chunk::start()
{
    server_.start();
}
/*
void Chunk::checkmd5()
{
    md5.reset();
    std::ifstream in;
    in.open("");
    md5.update(in);
    md = md5.toString();
    std::cout<< " file2 md5 is: "<< md <<std::endl;
}
*/
void Chunk::sendclient(const muduo::net::TcpConnectionPtr& conn)
{
    data["who"] = "Chunk";
    data["operate"] = "uploaddone";
    Json::FastWriter writter;
    conn->send(writter.write(data));
}

void Chunk::receive(const muduo::net::TcpConnectionPtr& conn,muduo::net::Buffer *buf,int fd,int& usesize)
{
    //LOG_INFO<<" receive data";
    const char* buffer(buf->peek());
    size_t putin = write(fd,buffer,buf->readableBytes());
    buf->retrieveAll();
    usesize -= putin;//这一块写完了
    //LOG_INFO<<" size left: "<< usesize;
    if(usesize<=0)
    {
        LOG_INFO<<" chunk receive done ";
        //sendclient(conn);
        close(fd);
    }
    return;
}

void Chunk::senddata(const muduo::net::TcpConnectionPtr& conn,std::FILE* fp)
{
    char buf[kBufSize];//每次读1M，不会出现越界的情况
    size_t littlesize = sizeof(buf);
    //LOG_INFO<< " 那就是这里 ";
    size_t readonce = fread(buf, 1, littlesize, fp);
    size += readonce;
    FilePtr ctx(fp, ::fclose);
    conn->setContext(ctx);
    conn->send(buf, static_cast<int>(readonce));
}

void Chunk::onWriteComplete(const muduo::net::TcpConnectionPtr& conn)
{
    //LOG_INFO<<" 进到这里了？ ";
    const FilePtr& fp = boost::any_cast<const FilePtr&>(conn->getContext());
    char buf[kBufSize];//每次读1M，不会出现越界的情况
    size_t littlesize = sizeof(buf);
    size_t readonce = fread(buf, 1, littlesize,get_pointer(fp));
    size += readonce;
    //LOG_INFO<<"发送了 "<< size;
     if(readonce==0)
     {
        LOG_INFO <<" chunk 发完了";

        //disconnect();
        return;
     }
    conn->send(buf, static_cast<int>(readonce));
}

void Chunk::onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if(conn->connected())
    {
        conn_ = conn;
        perconn* contextptr = new perconn;
        contextptr->recv = 0;
        contextptr->fd = 0;
        contextptr->chunksize =0; 
        contextptr->usesize =0;
        perptr ctx(contextptr);
        conn->setContext(ctx);
    }
    else
    {
        if(method == "upload")
        {
        const perptr& context = boost::any_cast<const perptr&>(conn->getContext());
        std::vector<std::string> chunkmd = chunkmd5(context->filename,context->chunksize,1);
        context->md5 = chunkmd[0];
        //filetochunk[md].push_back(chunkmd[0]);//把同一个文件chunk放在一起
        LOG_INFO<<"file "<< context->filename <<" md5 is "<<context->md5;
        }
        if(method == "download")
            conn_.reset();
    }

}

//这个还要考虑client发送给chunkserver的分片信心，在upload的时候
void Chunk::onMessage(const muduo::net::TcpConnectionPtr& conn,muduo::net::Buffer* buf)
{
    const perptr& context = boost::any_cast<const perptr&>(conn->getContext());
   
    if(context->recv == 1)
    {
        receive(conn,buf,context->fd,context->usesize);
        return;
    }
    LOG_INFO<<"  收到消息。";
    const char* msg(buf->peek());
    Json::Reader reader;
    std::string client;
    //std::string method;
    std::string totalmd5;
    int fd;

    if(reader.parse(msg,data))
    {
        client = data["who"].asString();
        method = data["operate"].asString();
        totalmd5 = data["totalmd5"].asString();
        md = data["md5"].asString();

        size = data["size"].asInt();
        //LOG_INFO<<"chunk md: "<< md;
    }
    if(client =="Client" && method == "upload")
    {
        fd = open(md.data(),O_CREAT|O_APPEND|O_RDWR,0666);
        context->recv =1;
        context->fd = fd;
        context->filename = md;
        context->chunksize = size;//是分块的size
        context->usesize = size;
        //LOG_INFO<<" totalmd5 是 "<< totalmd5;
        filetochunk[totalmd5].push_back(md);//记录了这个文件发过来的块
    }
    if(client == "Client" && method == "download")
    {
        //LOG_INFO<<" totalmd5 是 "<<totalmd5;
        std::string chunkfile = filetochunk[totalmd5].front();
        LOG_INFO << " 块文件名 "<< chunkfile;
        filetochunk[totalmd5].pop_front();
        std::FILE* fp = ::fopen(chunkfile.data(), "rb");
        if(fp == NULL)
                LOG_ERROR<<" open file failed, md5";
        //LOG_INFO<<" 死在这里 ";
        senddata(conn,fp);
    }
    buf->retrieve(buf->readableBytes());//删除掉json数据
}

ChunkClient::ChunkClient(muduo::net::EventLoop* eventloop, const muduo::net::InetAddress& serverAddr)
:client_(eventloop,serverAddr,"Chunk client")
{
    client_.setConnectionCallback(boost::bind(&ChunkClient::onConnection,this,_1));
    //master给chunk发送分片md5信息。
    client_.setMessageCallback(boost::bind(&ChunkClient::onMessage,this,_1,_2));
    //client_.enableRetry();这个函数先不用。
}

void ChunkClient::connect()
{
    client_.connect();
}

void ChunkClient::disconnect()
{
    client_.disconnect();
}

//给Master传递chunk的ipport
void ChunkClient::sendipport()
{

    muduo::net::InetAddress localaddr;
    localaddr = connect_->localAddress();
    muduo::string ip = localaddr.toIp();
    ipport = ip;
    ipport+= ":2016";
    //std::cout<<ipport <<std::endl;
    sendMaster["who"] = "ChunkClient";
    sendMaster["operate"] = ipport.c_str();
    sendMaster["md5"] = "None";
    sendMaster["filename"] = "None";
    sendMaster["size"] = 0;
    Json::FastWriter writer;
    connect_->send(writer.write(sendMaster));
}


//连接到master，我们会发送who-chunkclient，operator-ip+port
void ChunkClient::onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    LOG_INFO<< "ChunkClient connnected to Master h";
    if(conn->connected())
    {
        connect_ = conn;
        sendipport();
    }
    else
    {
        LOG_INFO<< " ChunkClient connnect down";
    }
}

//master不用把信息发送给chunkclient，是client收到分片信息的时候，发送给chunkserver
void ChunkClient::onMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer *buf)
{

}


int main()
{
    muduo::net::EventLoop loop;
    muduo::net::InetAddress listenAddr(2016);
    muduo::net::InetAddress serverAddr(2018);
    Chunk server(&loop,listenAddr);
    ChunkClient client(&loop,serverAddr);
    client.connect();
    server.start();
    loop.loop();
}