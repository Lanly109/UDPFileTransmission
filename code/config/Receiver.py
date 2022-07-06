#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from config.util import check_fileinfo
from genericpath import exists
from threading import *
from socket import AF_INET, SOCK_DGRAM, socket, timeout
from collections import deque
import time
import numpy as np
import os
from .config import *
from .Logger import *


class Receiver(object):
    """接收类
    - 用于接收文件
    - 一个进程负责接收数据，取出数据端放入缓冲区，并发送ACK报文
    - 一个进程负责从缓冲区取出数据，写入文件
    """

    def __init__(self, destaddr, sign, file, offset, udpsocket, num, data, log, MSS, filesize, filemd5):
        """初始化函数
        - destaddr 发送方(ip, port)
        - sign 传输的报文签名
        - file 传输文件
        - offset 传输文件的起始偏移地址，用于判断重传或者断点续传
        - udpsocket 复用服务/客户端的udpsocket
        - num 发送数据报文的起始序号
        - data 从发送方接收到的第一份数据
        - log 复用服务/客户端的log
        - MSS 发送的数据报文的数据段的最大长度
        - filesize 要接收的文件大小
        - filemd5 接收文件的md5码
        """

        self.destaddr = destaddr
        self.sign = sign
        self.udpsocket = udpsocket
        self.udpsocket.settimeout(time_limit)
        self.file = file
        self.offset = offset
        # 要发送的报文使用的编号
        self.seq = num
        self.data = data
        self.rwnd = default_rwnd
        self.log = log
        # buffer用双端队列实现，python文档说是进程安全的，内部已经实现了锁
        self.buffer = deque()
        self.status = status.CLOSE
        # 锁，因为有两个进程会更新rwnd,防止写冲突
        self.lock = Lock()
        self.MSS = int(MSS)
        # 数据报文结构
        self.package = Struct(f"!HHI{self.MSS}s")
        self.MSS_size = self.package.size
        self.file_size = int(filesize)
        self.total_package = int(np.ceil((self.file_size - self.offset) / self.MSS) + self.seq)
        self.filemd5 = filemd5

    def receive(self):
        """接收数据函数
        - 先处理上级（客户端或服务端）收到的第一份数据
        - 然后接收数据
        - 如果接收到的报文是结束报文，则接收完毕
        - 如果接收到的报文序号不对，则重发上次ACK
        - 如果接收到的报文序号正确，取出数据段放入缓冲区buffer，并发送ACK报文
        - 如果接收到的报文序号正确且是请求rwnd报文，则正常回复ACK，不放入缓冲区buffer
        """

        self.log.info("Receiving start")
        self.log.info(f"Expect package {self.seq}")
        rwnd = 0
        data = []
        sign = 0
        seq = 0
        try:
            sign, rwnd, seq, data = self.package.unpack(self.data)
        except Exception as e:
            self.log.warning(
                f"Unable to unpack received package due to the error : {e}, droped."
            )
        # 判断是否是终止报文，由特殊的rwnd标识，因为发送方的rwnd是无用的
        if rwnd != DONE:
            self.buffer.append(data[:rwnd])
        # 防止与write函数里的rwnd修改产生写冲突造成数据不对
        self.lock.acquire()
        self.rwnd -= 1
        self.lock.release()
        # 发送数据段为空的ACK报文
        pkg = self.package.pack(sign, self.rwnd, seq, "".encode())
        self.udpsocket.sendto(pkg, self.destaddr)
        self.log.info(f"Receive package {self.seq}/{self.total_package}")
        self.seq += 1
        cnt = 0
        # 是否因为三次不正确报文而重发
        ok = 0
        while True:
            # 如果收到的是结束报文，结束接收
            if rwnd == DONE:
                self.status = status.CLOSE
                break
            # 如果自身buffer已满，暂停接收
            while self.rwnd == 0:
                self.log.info(f"Buffer is full, sleeping for 0.05s......")
                time.sleep(0.05)
            try:
                raw, dstaddr = self.udpsocket.recvfrom(self.MSS_size)
            except timeout:
                # 预留了较长接收时间，如果无数据接收，尝试重发一遍ACK报文
                self.udpsocket.sendto(pkg, self.destaddr)
                cnt += 1
                if cnt == 5:
                    self.log.warning(
                        f"{time_limit} seconds not receive package, there may be something wrong. Aborted."
                    )
                    self.status = status.CLOSE
                    break
                self.log.warning(
                    f"{time_limit} seconds not receive package, Resending ACK."
                )
                continue
            try:
                sign, rwnd, seq, data = self.package.unpack(raw)
            except Exception as e:
                self.log.warning(
                    f"Unable to unpack received package due to the error : {e}, droped."
                )
            # 防止无关报文影响
            if sign != self.sign:
                self.log.warning(f"Receive an unknown sign package, droped.")
            # 收到乱序数据包，重发ACK
            elif seq > self.seq:
                ok += 1
                self.log.warning(
                    f"Receive an uncorrect seq package: Expect {self.seq}, but got {seq}, {'resend ACK' if ok <= 3 else 'droped'}"
                )
                # 如果已经重发了，没必要再重发，不然导致发送端重发很多次
                if ok <= 3:
                    pkg = self.package.pack(self.sign, self.rwnd, self.seq - 1, "".encode())
                    self.udpsocket.sendto(pkg, self.destaddr)
                rwnd = 0
            # 收到正确数据包
            elif seq == self.seq:
                self.log.info(f"Receive package {self.seq}/{self.total_package}")
                if rwnd != DONE and rwnd != GetWindowsSize:
                    self.buffer.append(data[:rwnd])
                    self.lock.acquire()
                    self.rwnd -= 1
                    self.lock.release()
                elif rwnd == GetWindowsSize:
                    self.total_package += 1
                pkg = self.package.pack(self.sign, self.rwnd, self.seq, "".encode())
                self.udpsocket.sendto(pkg, self.destaddr)
                self.log.info(
                    f"Sending {'Final ' if rwnd == DONE else ''}ACK {self.seq}/{self.total_package}"
                )
                self.seq += 1
                ok = 0
            # 收到重复数据包，重发ACK
            elif seq < self.seq:
                ok += 1
                self.log.warning(
                    f"Receive an duplicated package {seq}, expect {self.seq}, resending {self.seq - 1} ACK"
                    # f"Receive an duplicated package {seq}, expect {self.seq}, droped"
                )
                if ok <= 1:
                    self.udpsocket.sendto(pkg, self.destaddr)
            # 逻辑上不会到这里
            else:
                self.log.warning(f"Here shouldn't reach!")
            cnt = 0
        # 保存最后一个结束报文的ACK
        self.pkg = pkg

    def write(self):
        """写文件
        - 从缓冲区buffer中取出数据写入文件
        """

        # 文件所在文件夹不存在，先创建文件夹
        if not os.path.exists(self.file):
            self.log.info(f"{self.file} not exist, create")
            if "/" in self.file:
                os.makedirs("/".join(self.file.split("/")[0:-1]), exist_ok=True)
        # 判断是断电续传还是重传
        f = open(self.file, "wb") if self.offset == 0 else open(self.file, "ab")
        self.log.info(f"Open file {self.file}")

        while self.status != status.CLOSE or len(self.buffer) != 0:
            if len(self.buffer) == 0:
                continue
            data = self.buffer.popleft()
            f.write(data)
            self.lock.acquire()
            self.rwnd += 1
            self.lock.release()
        f.close()
        self.log.info(f"Close file {self.file}")
        self.log.info(f"check md5 {self.file}")
        ans = check_fileinfo(self.file, [self.file_size, self.filemd5])
        if ans == cosend:
            self.log.info(f"File CORRECT!")
        else:
            self.log.warning(f"File UNCORRECT!")


    def start(self):
        """启动函数
        - 创建一个进程负责接收数据包
        - 创建一个进程负责将数据写入文件
        - 最后再发一遍结束包的ACK，防止意外丢包
        """

        self.status = status.WORK
        t1 = Thread(target=self.receive)
        self.log.info(f"start receive data/sending ack thread")
        t1.start()
        t2 = Thread(target=self.write)
        self.log.info(f"start writing thread")
        t2.start()
        t1.join()
        t2.join()
        if self.total_package != self.seq - 1:
            self.log.warning(f"Stop unnormally!")
        self.udpsocket.sendto(self.pkg, self.destaddr)
