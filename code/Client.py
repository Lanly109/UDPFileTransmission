#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from socket import AF_INET, SOCK_DGRAM, socket
from sys import argv
from threading import *
import threading
from config.config import *
from config.Logger import *
from config.Receiver import *
from config.Sender import *
from config.util import *
from random import randint
import numpy as np
import matplotlib.pyplot as plt

# 服务端ip:port
Serveraddr = ("127.0.0.1", 22222)


class Client(object):
    """客户端类

    用于发送请求，握手，交换文件信息

    握手完毕后创建Sender类或者Receiver类发送或接受文件。
    """

    def __init__(self, index, identify, file):
        """初始化函数
        - index 客户端进程编号，用于并发时区分不同进程
        - identify 标识自身身份是Sender还是Receiver
        - file 该客户端处理的相对路径文件名
        """

        self.log = Logger(f"Client {index} {identify}")
        self.log.info(f"Creating Client {index} to {identify} {file}")
        # 随机在一个端口上监听
        self.udpsocket = socket(AF_INET, SOCK_DGRAM)
        cnt = 0
        try:
            self.hostport = randint(20000, 60000)
            self.udpsocket.bind(("", self.hostport))
        except Exception as e:
            # 如果端口已经被其他进程监听，实际发生概率很小
            cnt += 1
            if cnt == 5:
                self.log.err(
                    f"Failed to listen to some port for {cnt} time{'' if cnt <= 1 else 's'}"
                )
                raise (e)
            self.hostport = randint(20000, 60000)
            self.udpsocket.bind(("", self.hostport))
        self.destaddr = Serveraddr
        self.udpsocket.settimeout(5)
        self.identify = identify
        self.file = file
        self.rwnd = default_rwnd
        self.num = startnum
        self.MSS = MSS
        self.package = Struct(f"!HHI{MSS}s")
        self.MSS_size = self.package.size

    def Shakehand(self):
        """握手函数
        - 首先发送服务端请求（send or receive)
        - 若自己为接收方，则请求中包括发送接收文件（如果有）的大小及md5码给客户端
        - 接收服务方的应答
        - 若自己为发送方，将收到的服务端方的文件（如果有）的大小及md5，与自身文件，再询问用户是否断点续传（如果已有数据一致）或重传，然后发送请求
        - 收到回复后，即开始发送/接收文件
        - 若期间连续超时5次未收到包，则退出进程
        - 一次超时时间为5秒
        """

        # 记录连续超时次数
        cnt = 0
        # 记录当前握手状态
        status = 0
        info = get_fileinfo(self.file)
        time.sleep(0.5)
        if self.identify == "Send":
            # 若自身文件不存在，退出
            if not os.path.exists(self.file):
                self.log.err(f"File not found: {self.file}, aborted.")
                return False
            pkg = self.package.pack(
                self.sign,
                self.rwnd,
                self.num,
                spliter.join([send_command, self.file, *info]).encode(),
            )
            self.filesize = int(info[0])
            while True:

                try:
                    self.log.info(
                        f"sending the {'first' if status == 0 else 'second'} handshake package {self.num}"
                    )
                    self.udpsocket.sendto(pkg, self.destaddr)
                except Exception as e:
                    self.log.err(
                        f"Error occured while handling sending in Shakehand: {e}, aborted."
                    )
                    return False

                ans = ""
                try:
                    raw, destaddr = self.udpsocket.recvfrom(self.MSS_size)
                    self.destaddr = destaddr
                    try:
                        sign, self.rwnd, num, data = self.package.unpack(raw)
                    except Exception as e:
                        self.log.warning(
                            f"Unable to unpack received package due to the error : {e}, droped."
                        )
                        continue
                    data = data.decode().strip(b"\x00".decode())
                    # 状态为1,表示已经发送重传或续传报文，等待确认服务端是否收到，收到后才能发送文件
                    if status == 1:
                        if sign != self.sign or num != (self.num) or data != ans:
                            self.log.warning(
                                f"got an uncorrect message, Expected sign num ans: {self.sign} {self.num} {ans}, but got {sign} {num} {data}, droped and resending."
                            )
                            continue
                        self.num += 1
                        break

                    self.num += 1
                    Server_fileinfo = data.split(spliter)
                    file = check_fileinfo(self.file, Server_fileinfo)
                    self.log.info(
                        f"got the respond, {'file exist' if file == cosend else ''}"
                    )
                    ans = resend
                    while True and file == cosend:
                        # 询问用户是否续传
                        ans = str(
                            input(
                                f"File exist but imperfect, type {resend} to resend or {cosend} to continuing send:\n"
                            )
                        )
                        if ans == resend or ans == cosend:
                            break
                        print("Invaild input, please try it again.")
                    if ans == resend:
                        self.offset = 0
                    elif ans == cosend:
                        self.offset = int(Server_fileinfo[0])
                    pkg = self.package.pack(
                        self.sign, self.rwnd, self.num, ans.encode()
                    )
                    status = 1
                    cnt = 0

                except timeout:
                    cnt += 1
                    # 超时5次，握手失败，退出
                    if cnt == 5:
                        self.log.err(
                            f"Timeout after resending {cnt} time{'s' if cnt > 1 else ''}, aborted."
                        )
                        return False
                    self.log.warning(
                        f"Timeout while receiving respond from {self.destaddr} for resend or cosend, resending for {cnt} tr{'y' if cnt == 1 else 'ies'}......"
                    )
                    continue

                except Exception as e:
                    self.log.err(f"Error occure while shanking: {e}, aborted.")
                    return False

            return True

        elif self.identify == "Receive":
            # 发送自身文件信息，如果不存在，info=['0', '0']
            pkg = self.package.pack(
                self.sign,
                self.rwnd,
                self.num,
                f"{spliter.join([receive_command, self.file, *info])}".encode(),
            )
            cnt = 0
            status = 0
            while True:

                try:
                    self.log.info(f"sending the first handshake package")
                    self.udpsocket.sendto(pkg, self.destaddr)
                except Exception as e:
                    self.log.err(
                        f"Error occured while handling sending in Shakehand: {e}, aborted."
                    )
                    return False

                try:
                    raw, destaddr = self.udpsocket.recvfrom(self.MSS_size)
                    self.destaddr = destaddr
                    self.log.info(f"got the respond")
                    try:
                        sign, rwnd, num, data = self.package.unpack(raw)
                    except Exception as e:
                        self.log.warning(
                            f"Unable to unpack received package due to the error : {e}, droped."
                        )
                        continue
                    if sign != self.sign or num != self.num:
                        self.log.warning(
                            f"got an uncorrect message, Expected sign num : {self.sign} {self.num}, but got {sign} {num}, droped and resending."
                        )
                        continue
                    if status == 1:
                        self.log.info(f"got the data, starting receiving......")
                        self.data = raw
                        break
                    ans, self.filesize, self.filemd5 = data.decode().strip(b"\x00".decode()).split(spliter)
                    # 如果服务器回复的是File not found,结束进程
                    if ans == FILENOTFOUND:
                        self.log.warning(f"File not exise, aborted.")
                        return False
                    # 如果文件数据一致，可以续传
                    if ans == cosend:
                        while True:
                            # 询问用户是否续传
                            ans = str(
                                input(
                                    f"File exist but imperfect, type {resend} to resend or {cosend} to continuing send:\n"
                                )
                            )
                            if ans == resend or ans == cosend:
                                break
                            print("Invaild input, please try it again.")
                        if ans == resend:
                            self.offset = 0
                        elif ans == cosend:
                            self.offset = int(info[0])
                    elif ans == resend:
                        ans = resend
                        self.offset = 0
                    else:
                        self.log.warning(f"Unknown respond: {data}, droping......")
                        continue
                    pkg = self.package.pack(
                        self.sign, self.rwnd, self.num, ans.encode()
                    )
                    self.num += 1
                    status = 1

                except timeout:
                    cnt += 1
                    if cnt == 5:
                        # 超时五次退出
                        self.log.err(
                            f"Timeout after resending {cnt} time{'s' if cnt > 1 else ''}, aborted."
                        )
                        return False
                    self.log.warning(
                        f"Timeout while receiving respond from {self.destaddr} for {'getting' if status else 'filedata'}, resending for {cnt} tr{'y' if cnt == 1 else 'ies'}......"
                    )
                    continue

                except Exception as e:
                    self.log.err(
                        f"Error occure while handling Shakehand: {e}, aborted."
                    )
                    return False
            return True

        else:
            self.log.err(f"Unreachable error while handling Shakehand, aborted.")

    def Getport(self):
        """获取端口函数
        - 向服务端发送文件传输请求，获取服务端处理该请求相应端口，这是由于NAT技术所致
        """

        self.sign = randint(1, 60000)
        # 请求端口报文，并附上自己的MSS
        pkg = repackage.pack(
            self.sign,
            self.rwnd,
            self.num,
            spliter.join([REQUESTPORT, str(MSS)]).encode(),
        )
        self.log.info(f"Try to get a port from Server")
        cnt = 0
        while True:
            try:
                self.udpsocket.sendto(pkg, self.destaddr)
            except Exception as e:
                self.log.err(f"Error occured while handling Getport: {e}, aborted.")
                return False

            try:
                raw, destaddr = self.udpsocket.recvfrom(reMSS_size)
                self.destaddr = destaddr
                try:
                    sign, self.rwnd, num, data = repackage.unpack(raw)
                except Exception as e:
                    self.log.warning(
                        f"Unable to unpack received package due to the error : {e}, droped."
                    )
                    continue
                data = data.decode().strip(b"\x00".decode())

                if sign != self.sign or num != self.num:
                    self.log.warning(
                        f"got an uncorrect message, Expected sign num : {self.sign} {self.num}, but got {sign} {num}, droped and resending."
                    )
                    continue
                # 回复RESET，一般为签名sign重复，重新生成sign
                if data == RESET:
                    self.log.warning(f"sign duplicated, regening...")
                    self.sign = randint(1, 60000)
                    pkg = repackage.pack(
                        self.sign,
                        self.rwnd,
                        self.num,
                        spliter.join([REQUESTPORT, str(MSS)]).encode(),
                    )
                    continue

                self.num += 1
                self.log.info(f"got the port : {data}")
                self.destaddr = (self.destaddr[0], int(data))
                break

            except timeout:
                cnt += 1
                if cnt == 5:
                    # 超时五次退出
                    self.log.err(
                        f"Timeout after resending {cnt} time{'s' if cnt > 1 else ''}, aborted."
                    )
                    return False
                self.log.warning(
                    f"Timeout while receiving respond from {self.destaddr} for port, resending for {cnt} tr{'y' if cnt == 1 else 'ies'}......"
                )
                continue

            except Exception as e:
                self.log.err(f"Error occure while handling Getport: {e}, aborted.")
                return False

        return True

    def start(self):
        """启动函数
        - 获取端口 & 握手
        - 均成功后开始发送/接收文件
        """

        if self.Getport() == False or self.Shakehand() == False:
            return
        self.log.info(f"Finish shakehand! Start {self.identify} {self.file}.....")
        if self.identify == "Send":
            Sender(
                self.destaddr,
                self.sign,
                self.file,
                self.rwnd,
                self.offset,
                self.udpsocket,
                self.num,
                self.log,
                self.MSS,
                self.filesize
            ).start()
        elif self.identify == "Receive":
            Receiver(
                self.destaddr,
                self.sign,
                self.file,
                self.offset,
                self.udpsocket,
                self.num,
                self.data,
                self.log,
                self.MSS,
                self.filesize,
                self.filemd5
            ).start()
        else:
            self.log.err(f"Unreachable error while starting send/receice job")
            return
        self.log.info(f"{self.identify} {self.file} Finished!")


# 主客户端log
Client_log = Logger("Client")


# 创建的进程列表
thread_list = []

# 发送的文件列表
file_list = []


def scanfile(path):
    """扫描文件函数
    - path 传输的相对路径的文件或文件夹
    - 如果传输的是文件，则创建一个进程传输
    - 如果传输的是文件夹，递归处理该文件夹里的文件和文件夹，创建多个进程实现并发传输
    """

    global index
    if os.path.isfile(path):
        t = Thread(target=Client(index, "Send", path).start)
        thread_list.append(t)
        file_list.append(path)
        t.start()
        index += 1
    else:
        for file in os.listdir(path):
            filepath = os.path.join(path, file)
            scanfile(filepath)


def draw(file):
    """画图函数
    - file 传输的文件
    - 主要是以图形化方式呈现Client作为发送方时，发送过程的rwnd,cwnd,rto的变化情况
    """

    with open(f"{file}_data.log", "r") as f:
        rwnddata = f.readline().split()
        cwnddata = f.readline().split()
        rtodata = f.readline().split()

    rwnddata = [np.ceil(float(i)) for i in rwnddata]
    cwnddata = [np.ceil(float(i)) for i in cwnddata]
    rtodata = [np.ceil(float(i)) for i in rtodata]

    plt.subplot(311)
    (l1,) = plt.plot(
        range(len(rwnddata)), rwnddata, c="steelblue", linewidth=2.0, linestyle="-"
    )
    plt.xlabel("Time")  # x标签
    plt.ylabel("MSS")  # y标签
    plt.legend(handles=[l1], labels=["rwnd"])
    plt.subplot(312)
    (l2,) = plt.plot(
        range(len(cwnddata)), cwnddata, c="seagreen", linewidth=2.0, linestyle="-"
    )
    plt.xlabel("Time")  # x标签
    plt.ylabel("MSS")  # y标签
    plt.legend(handles=[l2], labels=["cwnd"])
    plt.subplot(313)
    (l3,) = plt.plot(
        range(len(rtodata)), rtodata, c="red", linewidth=2.0, linestyle="-"
    )
    plt.xlabel("Time")  # x标签
    plt.ylabel("Time")  # y标签
    plt.legend(handles=[l3], labels=["RTO"])
    plt.savefig(f"{file}_data.png", dpi=720)


def summary(path):
    """总结函数
    - path 传输的相对路径的文件或文件夹
    - 对发送每个文件过程中的rwnd,cwnd,rto的变化过程绘制图表
    """

    for file in file_list:
        draw(file)


index = 1


def main():
    """主函数
    - 接收命令行参数，创建进程传输文件
    """

    if len(argv) != 3 or (argv[1] != "send" and argv[1] != "receive"):
        print(f"usage: python3 {argv[0]} <send/receive> <file_name>")
        exit(0)

    command = argv[1]
    file = argv[2]
    Client_log.info("Welcome to use Lanly's file transsport software!")
    if command == "send":
        scanfile(file)
        for i in thread_list:
            i.join()
        summary(file)
    else:
        Client(index, "Receive", file).start()


if __name__ == "__main__":
    main()
