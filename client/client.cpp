//
// Created by 黄胜 on 07/08/2017.
//

//写上传函数
#include <errno.h>
#include "client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "md5.h"

const int kBufSize = 1024*1024;
typedef boost::shared_ptr<FILE> FilePtr;

ClienttoChunk::ClienttoChunk(EventLoop *loop, const InetAddress &serverAddr, std::string md5,std::string totalmd5,std::string method,std::string filename,std::FILE* fp,uint64_t chunksize,long long offset)
        :chunckclient_(loop,serverAddr,"ClienttoChunk"), md_(md5),method_(method),filename(filename),fp_(fp),chunksize_(chunksize),totalmd5_(totalmd5),offset_(offset)
{
    chunckclient_.setConnectionCallback(boost::bind(&ClienttoChunk::onConnection,this,_1));
    chunckclient_.setMessageCallback(boost::bind(&ClienttoChunk::onMessage,this,_1,_2));
    if(method_=="upload")
        chunckclient_.setWriteCompleteCallback(boost::bind(&ClienttoChunk::onWriteComplete,this,_1));
    nread_=0;
    newflag = 0;
    nwrite_=0;
    chunckclient_.enableRetry();
}

void ClienttoChunk::connect()
{
    chunckclient_.connect();
}

void ClienttoChunk::disconnect()
{
    chunckclient_.disconnect();
}

//client 和chunk之间的上传通信
void ClienttoChunk::chunkupload()
{
    //flag =0;
    sendtochunk_["who"] = "Client";
    sendtochunk_["operate"] = "upload";
    sendtochunk_["totalmd5"] = totalmd5_;
    sendtochunk_["md5"] = md_;
    sendtochunk_["size"] = (Json::UInt64)chunksize_;
    //LOG_INFO<<" 发送块的大小 "<< chunksize_;
    Json::FastWriter writter;
    //int size = writter.write(sendtochunk_).size();
    //chunkconn_->send()
    chunkconn_->send(writter.write(sendtochunk_));
}

void ClienttoChunk::chunkdownload()
{
    /*
    FilePtr ctx(fp_, ::fclose);
    chunkconn_->setContext(ctx);
     */
    sendtochunk_["who"] = "Client";
    sendtochunk_["operate"] = "download";
    sendtochunk_["totalmd5"] = totalmd5_;
    sendtochunk_["md5"] = " ";
    sendtochunk_["size"]=0;
    Json::FastWriter writter;
    chunkconn_->send(writter.write(sendtochunk_));
}
/*
void ClienttoChunk::dataupload()
{
    if (fp_)
    {
        FilePtr ctx(fp_, ::fclose);
        //LOG_INFO<< " file LOG_INFO<<" " ctx";
        chunkconn_->setContext(ctx);

    }
    else
    {
        chunkconn_->shutdown();
        LOG_INFO << "FileServer - no such file";
    }
}
 */

//第一次是通讯协议传完
void ClienttoChunk::onWriteComplete(const TcpConnectionPtr& conn)
{

    //LOG_INFO << "1";
    //const FilePtr& fp = boost::any_cast<const FilePtr&>(conn->getContext());
    char buf[kBufSize];//每次读1M，不会出现越界的情况
    size_t littlesize = sizeof(buf);

    if ((chunksize_-nread_) >0)
    {
        fseek(fp_,offset_,SEEK_SET);

        size_t readonce = ::fread(buf, 1, kBufSize, fp_);
        offset_ += readonce;
        chunkconn_->send(buf, static_cast<int>(readonce));

        nread_ += readonce;
        //LOG_INFO<<" chunksize_ is" << chunksize_ <<"  nread_ is"<< nread_;
        //LOG_INFO<<" 不断的传数据中--";
    }
    else
    {
        //LOG_INFO<<" 关闭时还剩 "<< chunksize_-nread_;
        //LOG_INFO<< " 偏移 "<<offset_<< " "<< chunksize_<< " " << nread_;
        //这里不断掉连接
        fclose(fp_);
        //fclose(tempfp);
        disconnect();
        LOG_INFO << "client  send data to chunk done";
        //newflag =1;
    }
    //LOG_INFO<<"2";
}

void ClienttoChunk::datadownload()
{

}

void ClienttoChunk::onConnection(const TcpConnectionPtr &conn)
{
    if(conn->connected())
    {
        chunkconn_ = conn;
        conn->setTcpNoDelay(1);
        if(method_ == "upload")
        {
            //先向chunk服务器发送jsn消息
            //chunkupload();
            //dataupload();
            chunkupload();
        }
        else if(method_ == "download")
        {
            chunkdownload();
            //LOG_INFO<<" 下载大小 "<<chunksize_;
            //onMessage
        }
        else
        {
            LOG_ERROR << " operate wrong: neither upload or download ";
        }
    }
    else {
        if(method_ == "upload")
        {
           // LOG_INFO << " 上传完成 ";
        }
        //chunkconn_.reset();
        if(method_ == "download")
        {
            LOG_INFO<<" 下载完成  ";
        }
    }
}


void ClienttoChunk::onMessage(const TcpConnectionPtr &conn, Buffer *buffer)
{
    /*
    Json::Reader reader;
    Json::Value data;
    const char* msg(buffer->peek());
    std::string name;
    std::string operate;
    if(reader.parse(msg,data))
    {
        name = data["who"].asString();
        operate = data["operate"].asString();
    }
    if(name == "Chunk" && operate == "uploaddone")
    {
        newflag=2;
        //LOG_INFO<<" kan "guanbi << newflag;
        //LOG_INFO<< conn->localAddress().toIpPort();
    }
     */
    //要写的文件位置
    /*
    if(chunksize_<=0)
    {
        return;
        //disconnect();
    }
     */
    //const FilePtr& fp = boost::any_cast<const FilePtr&>(conn->getContext());
    const char* buff(buffer->peek());
    fseek(fp_,offset_,SEEK_SET);
    size_t realsize = buffer->readableBytes();
    int guanbi;
    /*
    while(realsize)
    {
        guanbi = fwrite(buff,1,kBufSize,fp_);
        realsize -= guanbi;
    }
    */
    if(realsize>kBufSize)
        LOG_ERROR<<"这个错误";
    guanbi = fwrite(buff,1,realsize,fp_);
    fflush(fp_);
    //LOG_INFO<<" 开始的偏移量 "<< offset_;
    offset_+= guanbi;
    nwrite_ +=guanbi;
    
    chunksize_ = chunksize_-guanbi;
    //LOG_INFO<<" 写入文件 "<<nwrite_;

    if(chunksize_<=0)
    {
        LOG_INFO<<" 偏移量" << offset_;
        LOG_INFO<< "chunksize 还剩： "<< chunksize_;
        disconnect();
    }

    buffer->retrieveAll();
}

void Client::sendtomaster()
{
    Json::Value temp;
    temp["who"] = "Client";
    temp["operate"] = "updoned";
    temp["md5"] = " ";
    temp["filename"] = " ";
    temp["size "] = 0;
    Json::FastWriter writer;
    //connection_->send(writer.write(temp));
    LOG_INFO<< connection_->peerAddress().toIpPort();
    LOG_INFO<<"     文件上传完毕     ";
}
void Client::timecall()
{
   // LOG_INFO<<" fuck ";
    bool complete = false;
    for(auto it = clientvector.begin(); it != this->clientvector.end(); it++)
    {
        /*
        if((*it)->newflag == 1)
        {
            delete *it;
            //LOG_INFO<<" delete class ";
            clientvector.erase(it);
            --it;
        }
         */
       // LOG_INFO<<"newflag: "<<(*it)->newflag;
        if((*it)->newflag ==2)
            complete = true;
        else {
            complete = false;
            break;
        }
    }
    if(complete)
    {
       // LOG_INFO<<" true ";
        for(auto it = clientvector.begin(); it != this->clientvector.end(); it++) {
            (*it)->newflag =0;
            (*it)->disconnect();
            //客户端断掉连接，大客户端发消息给
        }
        sendtomaster();
    }
}

Client:: Client(EventLoop* loop, const InetAddress& serverAddr,EventLoop* chunkloop)
            : client_(loop, serverAddr, "Client"),saveloop(chunkloop)
    {
        client_.setConnectionCallback(
                boost::bind(&Client::onConnection, this, _1));
        client_.setMessageCallback(
                boost::bind(&Client::onMessage, this, _1, _2));
        /*client_.setWriteCompleteCallback(boost::bind(&Client::onWriteComplete,this,_1));
        */
        client_.enableRetry();
        boost::function<void()> fp = boost::bind(&Client::timecall,this);
        //loop->runEvery(5.0, fp);
        flag=0;
    }

void Client::connect()
{
    client_.connect();
}

void Client::disconnect()
{ client_.disconnect(); }

void Client::uploadprotocol(const TcpConnectionPtr& conn,std::string path)
{
    flag = 1;
    int fd = open(path.data(),O_RDWR);

    struct stat buf;
    fstat(fd,&buf);

    size = buf.st_size;
    //std::cout<<" md5 begin caculate "<< std::endl;
    md = md5file(path.c_str());
    
    LOG_INFO<<"  md5 is: "<< md ;
    std::cout <<  "md5大小是 " <<md.size()<<std::endl;
    Json::Value root;
    root["who"] = "Client";
    root["operate"] = "upload";
    root["md5"] = md;
    int i = path.rfind('/');
    std::string file = path;
    if( i != -1)
    {
        file = path.substr(i+1,path.size()-i);
    }
    root["filename"] = file;
    root["size"] = (Json::UInt64)size;
    Json::FastWriter writer;
    conn->send(writer.write(root));
    //LOG_INFO<< connection_->peerAddress().toIpPort();
    //std::cout<<"send upload info"<<std::endl;
}

void Client::downparse(std::vector <std::string> &addr,long long length) {
    int num = addr.size();//有几个chunkip就有几个块，在这里算md5
    // std::cout<< " addr size: " << size << std::endl;
    std::vector <std::vector<std::string>> parse(num, std::vector<std::string>(0));
    std::vector <std::string> chunk(num, " ");
    //filename,size都是class member。
    //chunk = chunkmd5(filename, size,num);
    //std::string::iterator it;
    std::size_t found;
    for (int i = 0, j = 0; i < num; ++i) {
        found = addr[i].find(":");
        int length = addr[i].size();
        if (found == std::string::npos)
            continue;
        else {
            std::string str1;
            std::string str2;
            str1 = addr[i].substr(0, found);
            str2 = addr[i].substr(found + 1, length - 1);
            // std::cout<< str1 << " " << str2 << std::endl;
            parse[j].push_back(str1);
            parse[j].push_back(str2);
            ++j;
        }
    }
    ipport = parse;
    long long chunksize = 67108864;
    long long total = length;
    long long temp=0;

    std::FILE *file;
    file = std::fopen(filename.data(), "w");
    if (file == NULL)
        LOG_ERROR << " open file failed, md5";
    for (int i = 0; i < ipport.size(); ++i) {
        chunksize = std::min(chunksize,total);
        total  = total-chunksize;
        uint16_t port = static_cast<uint16_t>(atoi(ipport[i][1].data()));
        InetAddress serveraddr(ipport[i][0].data(), port);
        //LOG_INFO<<" 初始偏移量："<< total;
        ClienttoChunk *it = new ClienttoChunk(saveloop, serveraddr, " ", md5_, method, filename, file, chunksize,temp);
        temp += chunksize;
        clientvector.push_back(it);
        it->connect();
    }

}
//解析完addr之后，我们开始创建chunk对象来传消息和数据
void Client::parseaddr(std::vector <std::string> &addr)
{
    int num = addr.size();//有几个chunkip就有几个块，在这里算md5
   // std::cout<< " addr size: " << size << std::endl;
    std::vector<std::vector<std::string> >parse(num,std::vector<std::string>(0));
    std::vector<std::string> chunk(num," ");
    //filename,size都是class member。
    chunk = chunkmd5(filename, size,num);
    //std::string::iterator it;
    std::size_t found;
    for(int i=0,j=0;i<num;++i)
    {
        found = addr[i].find(":");
        int length = addr[i].size();
        if(found == std::string::npos)
            continue;
        else
        {
            std::string str1;
            std::string str2;
            str1=addr[i].substr(0,found);
            str2 = addr[i].substr(found+1,length-1);
           // std::cout<< str1 << " " << str2 << std::endl;
            parse[j].push_back(str1);
            parse[j].push_back(str2);
            ++j;
        }
    }
    ipport = parse;
    long long chunksize = 67108864;
    long long total = (long long)size;
    long long temp = 0;
    /*
    std::FILE* file;
    file = std::fopen(filename.data(), "rb");
    if(file == NULL)
        LOG_ERROR<<" open file failed, md5";
        */
    //构造和chunk沟通的client，传入了每一块的md5，这是在客户端上传的时候，下载的时候md5就为0吧，或者是再创建一个类。再说
    for(int i=0;i<ipport.size();++i)
    {
        chunksize = std::min(chunksize,total);
        total  = total-chunksize;

        uint16_t port = static_cast<uint16_t>(atoi(ipport[i][1].data()));
        InetAddress serveraddr(ipport[i][0].data(),port);
        //LOG_INFO<<"transfer ipport";
        LOG_INFO<<"chunk md5 is "<<chunk[i];
        

        std::FILE* file;
        if(method =="upload")
        {
            file = std::fopen(filename.data(), "rb");
            if(file == NULL)
                LOG_ERROR<<" open file failed, md5";
            //LOG_INFO<<" 主类 chunk size is "<< chunksize;
            //fseek(file,chunksize*i,SEEK_SET);
        }

        //ClienttoChunk chunkclient (saveloop,serveraddr,chunk[i],method,filename,file,chunksize);
        ClienttoChunk * it = new ClienttoChunk(saveloop,serveraddr,chunk[i],md,method,filename,file,chunksize,temp);
        temp += chunksize;
        clientvector.push_back(it);
        //chunkclient.connect();
        it->connect();
        //LOG_INFO<<" client to chunk ";
    }
}

void Client::downloadprotocol(const TcpConnectionPtr& conn)
{
    Json::Value down;
    down["who"] = "Client";
    down["operate"] = "download";
    down["filename"] = filename;
    Json::FastWriter writer;
    conn->send(writer.write(down));
}
void Client:: onConnection(const TcpConnectionPtr& conn)
{
        if(conn->connected()) {
            LOG_INFO<<"client connect to master";
            connection_ = conn;
            if(method ==  "upload")//这都是发送给master的消息了。
                uploadprotocol(conn,filename);
            if(method == "download")
                downloadprotocol(conn);
        }
        else
            connection_.reset();
}

//在这里添加download解析
void Client::parse(const TcpConnectionPtr& conn, Buffer* buffer)
{
    const char* msg(buffer->peek());
    Json::Reader reader;
    Json::Value xieyi;
    std::string user;
    //std::string md5;
    std::string operate;
    uint64_t size;
    std::string status;
    int num;
    if(reader.parse(msg,xieyi))
    {
        user = xieyi["who"].asString();
        operate = xieyi["operate"].asString();
        size = xieyi["size"].asUInt64();
        md5_ = xieyi["md5"].asString();
        status = xieyi["status"].asString();
        if(status == "sendchunkinfo"&&operate=="upload")
        {
            num = xieyi["chunknum"].asInt();
            for(int i=0;i<num;++i)
            {
                std::string str = std::to_string(i);
                std::string ipport = xieyi[str.data()].asString();
                chunkaddr.push_back(ipport);
                //std::cout<<" ipport is: "<<ipport <<std::endl;
            }
            //解析chunk地址，为upload做准
            parseaddr(chunkaddr);
            return;
        }
        if(status=="done")
        {
            std::cout << "file already exists" << std::endl;
            disconnect();
        }
        //hu
        if(operate == "download")
        {
            num = xieyi["chunknum"].asInt();
            for(int i=0;i<num;++i)
            {
                std::string str = std::to_string(i);
                std::string ipport = xieyi[str.data()].asString();
                chunkaddr.push_back(ipport);
                //std::cout<<" ipport is: "<<ipport <<std::endl;
            }
            downparse(chunkaddr,(long long)size);
        }
    }

}

//收到master的信息upload的信息是：文件md5对应的一串chunk的ipport
void Client::onMessage(const TcpConnectionPtr& conn, Buffer* buffer)
{
    //解析消息；
    LOG_INFO<<" into parse";
    parse(conn,buffer);
}

int main(int argc, char* argv[]) {
    if(argc>2)
    {
        EventLoopThread masterloopthread,chunkloopthread;
        uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
        const InetAddress serverAddr(argv[1],port);
        Client client(masterloopthread.startLoop(),serverAddr,chunkloopthread.startLoop());
        //从终端读字符串
        std::cout<<" download or upload:"<<std::endl;
        std::string str;
        getline(std::cin,str);
        if(str == "download")
            client.method = str;
        else if(str == "upload")
            client.method = str;
        else
        {
            std::cout<<" invaliad input";
            exit(0);
        }
        std::cout<< " input filename or path: "<<std::endl;
        getline(std::cin,str);
        client.filename = str;
        client.connect();
        while(1)
        {

        }
        //client.disconnect();
    }
    else
        printf("usage: %s host_ip port",argv[0]);
}