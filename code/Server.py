#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from math import degrees
from socket import AF_INET, SOCK_DGRAM, socket
from threading import *
from random import randint
from collections import defaultdict
from config.config import *
from config.Logger import *
from config.Receiver import *
from config.Sender import *
from config.util import *

# 用于sign和ip:port的一一映射，防止传输冲突
used = {}


class Server(object):
    """服务端类

    用于发送握手包，解析客服端请求，交换文件信息

    握手完毕后创建Sender类或者Receiver类发送或接受文件。
    """

    def __init__(self, index, hostport, destaddr, sign, client_MSS):
        """初始化函数

        - index 服务进程编号，用于并发时区分不同进程
        - hostport 该进程监听的端口号
        - destaddr 与进程通信的(ip, port)
        - sign 与进程通信时双方的签名，忽略签名不正确的包，防止干扰
        - client_MSS 客服端要求的握手包及之后数据传输包的数据段的最大大小
        """

        self.log = Logger(f"Server {index}")
        self.log.info(f"creating Server {index} on {hostport}")
        self.hostport = hostport
        self.destaddr = destaddr
        self.udpsocket = socket(AF_INET, SOCK_DGRAM)
        self.udpsocket.bind(("", self.hostport))
        self.udpsocket.settimeout(5)
        self.sign = sign
        self.num = startnum + 1
        self.MSS = client_MSS
        self.package = Struct(f"!HHI{self.MSS}s")
        self.MSS_size = self.package.size

    def Shakehand(self):
        """握手函数
        - 首先接收客户端请求是send还是receive
        - 然后判断自己的职责
        - 若自己为接收方，发送接收文件（如果有）的大小及md5码给客户端，接收客服端的请求（断点续传或重传），接收请求后创建receiver类接收文件
        - 若自己为发送方，接收客户端发来的发送文件（如果有）的大小及md5,比对自身文件，再询问客户端是否断点续传（如果已有数据一致）或重传，接收请求后创建send类发送文件
        - 若期间连续超时5次未收到包，则退出进程
        - 一次超时时间为5秒
        """

        # 用于记录连续超时次数
        cnt = 0

        while True:
            try:
                raw, destaddr = self.udpsocket.recvfrom(self.MSS_size)
                # 更新客户端地址，由于对称型NAT的原因，向主进程发送的数据包的源地址和向该进程发送数据包的源地址可能会不同
                self.destaddr = destaddr
                try:
                    sign, rwnd, num, data = self.package.unpack(raw)
                except Exception as e:
                    self.log.warning(
                        f"Unable to unpack received package due to the error : {e}, droped."
                    )
                    continue
                # 数据包解码，由于python的pack机制
                data = data.decode().strip(b"\x00".decode())
                # 如果签名不对或者报文序号不对，忽略，并重新发送一次报文，实际上报文序号不对的情况不该出现
                if sign != self.sign or num != self.num:
                    self.log.warning(
                        f"got an uncorrect message, Expected sign num : {self.sign} {self.num}, but got {sign} {num}, droped."
                    )
                    continue
                # 解析客户端请求，获取处理的文件名以及信息（如果有）
                command, file, *info = data.split(spliter)
                self.log.info(
                    f"receiving a {'sending' if command == send_command else 'receiving'} request {num} from {destaddr}"
                )
                self.file = file
                self.Client_fileinfo = info
                self.rwnd = rwnd
                if command == send_command:
                    self.identify = "Receive"
                elif command == receive_command:
                    self.identify = "Send"
                    self.rwnd = default_rwnd
                # 无法解析的请求
                else:
                    self.log.warning(
                        f"Receive an unknown message from {destaddr}: {command} {file} {info}"
                    )
                    continue
                break
            except timeout:
                cnt += 1
                # 超时次数达5次，握手失败，退出
                if cnt == 5:
                    self.log.err(
                        f"Timeout after {cnt} time{'s' if cnt > 1 else ''}, aborted."
                    )
                    return False
                self.log.warning(
                    f"Timeout while receiving request from {self.destaddr} for {cnt} time{'' if cnt == 1 else 's'}......"
                )
                continue
            # 意料之外的错误
            except Exception as e:
                self.log.err(f"Error occure while receiving request: {e}, aborted.")
                return False

        # 收到请求，判断服务端身份
        if self.identify == "Send":
            info = check_fileinfo(self.file, self.Client_fileinfo)
            # 如果服务端找不到该文件，则无法发送，返回File not Found
            if info == FILENOTFOUND:
                self.log.err(f"File not found: {self.file}, aborted.")
                self.udpsocket.sendto(
                    self.package.pack(self.sign, self.rwnd, self.num, FILENOTFOUND),
                    self.destaddr,
                )
                return False
            self.fileinfo = get_fileinfo(self.file)
            self.filesize = int(self.fileinfo[0])
            # 制作回复报文，如果数据一致，询问是否断点续传，否则就说重传
            pkg = self.package.pack(
                self.sign,
                self.rwnd,
                self.num,
                spliter.join([cosend if info == cosend else resend, *self.fileinfo]).encode(),
            )

            while True:

                try:
                    self.log.info(
                        f"sending the first handshake package{',file already exist! Asking continuing send or not' if info else ''}"
                    )
                    self.udpsocket.sendto(pkg, self.destaddr)
                except Exception as e:
                    self.log.err(
                        f"Error occured while handling sending in shakehand: {e}, aborted."
                    )
                    return False

                try:
                    raw, destaddr = self.udpsocket.recvfrom(self.MSS_size)
                    try:
                        sign, rwnd, num, data = self.package.unpack(raw)
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
                    self.log.info(
                        f"got the respond, {'(over)write' if data == resend else 'continuing sending'} {self.file}"
                    )
                    # 如果客户端回复是重传，文件读取时的偏移量为0,即从头开始
                    if data == resend:
                        self.offset = 0
                    # 否则，偏移量为客户端已有的文件大小
                    elif data == cosend:
                        self.offset = int(self.Client_fileinfo[0])
                    else:
                        self.log.warning(f"Unknown respond: {data}, droping......")
                        continue
                    self.num += 1
                    break

                except timeout:
                    # 超时五次退出
                    cnt += 1
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
            info = get_fileinfo(self.file)
            # 接收文件，发送客户端服务器上相关文件的信息，如果文件不存在则info=['0', '0']
            pkg = self.package.pack(
                self.sign, self.rwnd, self.num, spliter.join([*info]).encode()
            )
            self.num += 1
            cnt = 0
            self.filesize = self.Client_fileinfo[0]
            self.filemd5 = self.Client_fileinfo[1]

            # 记录当前状态，由于握手有两次
            status = 0
            while True:

                try:
                    self.log.info(
                        f"sending {'ACK ' if status == 1 else ''}handshake package {self.num}{', file already exist! Asking continuing send or not' if info[0] != str(0) and status == 0 else ''} to {self.destaddr} "
                    )
                    self.udpsocket.sendto(pkg, self.destaddr)
                except Exception as e:
                    self.log.err(
                        f"Error occured while handling sending in shakehand: {e}, aborted."
                    )
                    return False

                try:
                    raw, destaddr = self.udpsocket.recvfrom(self.MSS_size)
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
                    # 收到第一次握手的回复（重传还是继续传），状态为1,此时如果收到客户端数据，则说明开始传输文件
                    if status == 1:
                        self.log.info(f"got the data, starting receiving......")
                        self.data = raw
                        break
                    ans = data.decode().strip(b"\x00".decode())
                    if ans == resend:
                        self.offset = 0
                    elif ans == cosend:
                        self.offset = int(info[0])
                    else:
                        self.log.warning(f"Unknown respond: {ans}, droping......")
                        continue
                    self.log.info(
                        f"got the respond, {'(over)write' if ans == resend else 'continuing sending'} {self.file}"
                    )
                    # 发送ACK报文，表明已收到客户端的要求（断点续传或重传），状态变为1,如果客户端发送数据，表明该报文收到，就可以开始接收文件，否则需要重传ACK报文
                    pkg = self.package.pack(
                        self.sign, self.rwnd, self.num, ans.encode()
                    )
                    self.num += 1
                    # 切换状态为1
                    status = 1
                    cnt = 0

                except timeout:
                    # 超时5次，握手失败，退出
                    cnt += 1
                    if cnt == 5:
                        self.log.err(
                            f"Timeout after resending {cnt} time{'s' if cnt > 1 else ''}, aborted."
                        )
                        return False
                    self.log.warning(
                        f"Timeout while receiving respond from {self.destaddr} for {'getting' if status else 'filedata'}, resending for {cnt} tr{'y' if cnt == 1 else 'ies'}......"
                    )
                    continue

                # except Exception as e:
                    # self.log.err(f"Error occure while shanking: {e}, aborted.")
                    # return False
            return True

        else:
            self.log.err(f"Unreachable error while shanking, aborted.")

    def start(self):
        """启动函数
        先进行握手，握手成功就开始传输或接收文件，否则退出
        """

        global used
        # 如果握手失败，终止此次处理
        if self.Shakehand() == False:
            used.pop(self.sign)
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
            used.pop(self.sign)
            return
        self.log.info(f"{self.identify} {self.file} Finished!")
        used.pop(self.sign)


# 主服务端log
Server_log = Logger("Serverd")


if __name__ == "__main__":
    """主函数
    - 一个服务进程监听hostport端口(定义在config.config)
    - 如果收到请求，则创建一个新进程处理该请求，并回复该进程监听的端口号
    - 以达到高并发处理
    """
    Server_log.info("Welcome to use Lanly's file transsport software!")
    udp = socket(AF_INET, SOCK_DGRAM)
    index = 1
    udp.bind(("", hostport))
    Server_log.info(f"Start service, listening to port {hostport}......")
    destaddr = ""
    while True:
        try:
            data, destaddr = udp.recvfrom(reMSS_size)
            try:
                sign, rwnd, num, data = repackage.unpack(data)
            except Exception as e:
                Server_log.warning(
                    f"Unable to unpack received package due to the error : {e}, droped."
                )
                continue
            data, client_MSS = data.decode().strip(b"\x00".decode()).split(spliter)
            # 如果签名重复，且不是对应ip,则发送重置报文，如果ip是对应的，那么已有进程处理该ip,故此处就忽略
            if sign in used or num != startnum:
                Server_log.warning(
                    f"receiving an {'duplicated sign' if sign in used else 'uncorrect'} message from {destaddr}, droping."
                )
                if used[sign] != destaddr:
                    udp.sendto(
                        repackage.pack(sign, rwnd, num, RESET.encode()), destaddr
                    )
                continue
            used[sign] = destaddr
            Server_log.info(
                f"receiving a request from {destaddr}, deliver a port {startport} for it. {destaddr}"
            )
            # 回复报文，包括新进程的端口号
            udp.sendto(
                repackage.pack(sign, rwnd, num, str(startport).encode()), destaddr
            )
            Thread(
                target=Server(index, startport, destaddr, sign, client_MSS).start
            ).start()
            index += 1
            startport += gapport
            # 避免端口号不合法
            if startport > 65535:
                startport = 10001
        except Exception as e:
            Server_log.err(
                f"Error occurred while handing the message from {destaddr} : {e}"
            )
