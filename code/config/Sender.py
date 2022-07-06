#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from threading import *
from socket import AF_INET, SOCK_DGRAM, socket, timeout
from collections import deque
import time
import numpy as np
from .config import *
from .Logger import *


class Sender(object):
    """发送类
    - 用于发送文件
    - 实现了流量控制、阻塞控制、动态调整RTT(RTO)，超时重传，回退N步的快速重传
    - 一个进程负责发送数据
    - 一个进程负责接收ACK并作出相应反应（如重传）
    """

    def __init__(self, destaddr, sign, file, rwnd, offset, udpsocket, num, log, MSS, filesize):
        """初始化函数
        - destaddr 接收方(ip, port)
        - sign 传输的报文签名
        - file 传输文件
        - rwnd 滑动窗口大小
        - offset 传输文件的起始偏移地址，用于判断重传或者断点续传
        - udpsocket 复用服务/客户端的udpsocket
        - num 发送数据报文的起始序号
        - log 复用服务/客户端的log
        - MSS 发送的数据报文的数据段的最大长度
        - filesize 要发送的文件大小
        """

        self.destaddr = destaddr
        self.sign = sign
        self.udpsocket = udpsocket
        self.file = file
        self.offset = offset
        # 未确认报文的编号
        self.unackseq = num
        # 要发送的报文使用的编号
        self.nextseq = num
        self.rwnd = rwnd
        # 拥塞窗口长度初始为1,慢启动
        self.cwnd = 1
        self.windowsize = np.ceil(np.min([self.rwnd, self.cwnd]))
        # buffer用双端队列实现，python文档说是进程安全的，内部已经实现了锁
        self.buffer = deque()
        self.dupack = 0
        self.ssthresh = 32
        self.status = status.CLOSE
        self.log = log
        # 估计RTO的参数
        self.SRTT = 0
        self.DevRTT = 1
        self.RTO = 0.1
        self.udpsocket.settimeout(self.RTO)
        # 记录ACK接收超时次数
        self.totaltimeout = 0
        # 记录超过3次冗余ACK触发的快速重传的次数
        self.totalfastresend = 0
        # 记录rwnd,cwnd,rto在发送过程中的数据，用于后续汇总
        self.rtodata = []
        self.rwnddata = []
        self.cwnddata = []
        self.MSS = int(MSS)
        # 数据报文结构
        self.package = Struct(f"!HHI{self.MSS}s")
        self.MSS_size = self.package.size
        self.file_size = int(filesize)
        self.total_package = int(np.ceil((self.file_size - self.offset) / self.MSS) + self.unackseq)


    def send(self):
        """发送数据函数
        - 每次从文件中读取MSS长度的数据
        - 若读取到的数据为空，说明已经发送完毕，则再发送一个结束报文标志传输完成
        - 若当前窗口长度为0,则发送空报文询问接收方当前rwnd
        - 制作数据报文并发送，将发送的报文放进缓冲区里面，直到收到相应的ACK时才从缓冲区里去掉
        """

        self.log.info(f"Open file {self.file}")
        f = open(self.file, "rb")
        f.seek(self.offset)
        self.log.info("----------------Sending start----------------")
        self.log.info(f"change status from **Close** to **Slow_Start**")
        while True:
            # 读取MSS长度的数据
            data = f.read(self.MSS)
            # 如果data为空，说明读取完毕，发送结束报文，结束标志用发送方报文的rwnd的特殊数字表示
            size = DONE if data == b"" else len(data)
            self.windowsize = np.ceil(np.min([self.rwnd, self.cwnd]))

            while (
                self.nextseq - self.unackseq >= self.windowsize
                and self.status != status.CLOSE
            ):
                self.log.info(
                    f"WindowSize is 0, flushing window size and sleeping for 0.2s......"
                )
                time.sleep(0.2)
                # 如果发送过快，则暂停发送数据，发送空报文获取接收方最新rwnd
                if self.nextseq - self.unackseq >= self.rwnd:
                # 空数据报文
                    pkg = self.package.pack(
                    self.sign, GetWindowsSize, self.nextseq, "".encode()
                )
                    self.buffer.append(pkg)
                    self.nextseq += 1
                    self.udpsocket.sendto(pkg, self.destaddr)
                    # 不属于文件数据的报文
                    self.total_package += 1
                self.windowsize = np.ceil(np.min([self.rwnd, self.cwnd]))

            if self.status == status.CLOSE:
                break
            pkg = self.package.pack(self.sign, size, self.nextseq, data)
            self.buffer.append(pkg)
            self.udpsocket.sendto(pkg, self.destaddr)
            self.log.info(
                f"Sending {'FIN ' if size == DONE else ''}package {self.nextseq}/{self.total_package}"
            )
            self.nextseq += 1
            if size == DONE:
                break
        self.status = status.CLOSE
        self.log.info(f"change status to **Close**")

        self.log.info("----------------Sending complete----------------")

        f.close()
        self.log.info(f"Close file {self.file}")

    def resend(self, cnt):
        """重传函数
        - 由三个冗余ACK触发的快速重传
        - 或者超时重传
        - 重传缓冲区buffer里的所有数据包
        """

        try:
            cnt = int(cnt)
            for pkg in self.buffer:
                cnt -= 1
                self.udpsocket.sendto(pkg, self.destaddr)
                if cnt == 0:
                    break
        except Exception as e:
            self.log.err(f"Error occurred while handling resend: {e}, ignore.")

    def update_cwnd(self, ack):
        """阻塞控制函数
        - ack 收到的ACK报文序号或特殊标识
        - 根据TCP的阻塞控制的逻辑，根据当前ACK接收情况从慢启动，阻塞避免，快速恢复状态之间切换
        - 更新cwnd
        """

        # 当传输完毕时，不需要更新cwnd
        if self.status == status.CLOSE:
            return

        # 超时，ssthresh变为cwnd的一半，cwnd置为1,变为慢启动状态
        if ack == TIMEOUT_ACK:
            self.log.warning(f"change status to **Slow_Start** due to Timeout")
            self.status = status.SLOW_START
            self.ssthresh = np.max([self.cwnd // 2, 1])
            self.cwnd = 1
            self.log.warning(f"change cwnd to {self.cwnd}")

        # 快速恢复，ssthresh变为cwnd的一半，cwnd置为ssthresh + 3,变为快速恢复状态
        elif ack == DUP_ACK:
            self.log.warning(
                f"change status to **Faster_Recover** due to duplicated ack"
            )
            self.status = status.FASTRE_RECOVERY
            self.ssthresh = np.max([self.cwnd // 2, 1])
            self.cwnd = self.ssthresh
            self.log.warning(f"change cwnd to {self.cwnd}")

        # 当前为慢启动状态，更新cwnd,每收到一个报文就增加一个MSS,这样在一个RTT时间，窗口内的报文全收到，cwnd就会翻倍
        elif self.status == status.SLOW_START:
            if ack == self.unackseq:
                if self.cwnd + 1 < self.ssthresh:
                    self.cwnd += 1
                    self.log.info(f"change cwnd to {self.cwnd}")
                else:
                    self.cwnd = self.ssthresh
                    self.log.info(f"change cwnd to {self.cwnd}")
                    self.status = status.AVOID
                    self.log.info(
                        f"change status from **Slow_Start** to **Congestion_Avoid**"
                    )

        # 当前为阻塞避免状态，经过一个RTT才会cwnd才会增加1个MSS
        elif self.status == status.AVOID:
            self.cwnd += 1.0 / self.cwnd
            self.log.info(f"change cwnd to {self.cwnd}")

        # 当前为快速恢复状态，每收到一个冗余ACK,cwnd就增加1,直到收到正确ACK,cwnd变为ssthresh, 切换到阻塞避免状态
        elif self.status == status.FASTRE_RECOVERY:
            if ack == self.unackseq:
                self.cwnd += 1
                self.log.info(f"change cwnd to {self.cwnd}")
                self.status = status.AVOID
                self.log.info(
                    f"change status from **Faster_Recovery** to **Congestion_Avoid**"
                )
            else:
                self.cwnd += 1
                self.log.info(f"change cwnd to {self.cwnd}")

        self.cwnddata.append(self.cwnd)

    def update_RTO(self, RTT):
        """更新RTO
        - RTT 数据包来回时间
        - 实现自Jacobson/Karels算法
        - 参数定义在config.config中
        """

        self.SRTT = self.SRTT + alpha * (RTT - self.SRTT)
        self.DevRTT = (1 - beta) * self.DevRTT + beta * (np.abs(RTT - self.SRTT))
        self.RTO = np.max([mu * self.SRTT + rao * self.DevRTT, Minimum_RTO])
        self.udpsocket.settimeout(self.RTO)
        self.log.info(f"RTO is updated to {self.RTO}")
        self.rtodata.append(self.RTO)

    def receive(self):
        """接收ACK
        - 接收到的ACK小于unacked - 1，忽略
        - 接收到的ACK等于unacked - 1，记录冗余次数
        - 接收到的ACK大于unacked - 1, 更新unacked为接收到的ACK,并从缓冲区删除相应数据包
        - **注意**，此处ACK与TCP中ACK的定义**不同**，此处ACK定义为接收到的最后一个数据包编号，即等于TCP中定义的ACK-1
        """

        # 记录连续超时次数
        cnt = 0
        while self.status != status.CLOSE or len(self.buffer) != 0:
            try:
                # 记录RTT
                rec_st = time.time()
                raw, dstaddr = self.udpsocket.recvfrom(self.MSS_size)
                rec_en = time.time()
                # 更新RTO
                self.update_RTO(rec_en - rec_st)
                sign = 0
                seq = 0
                rwnd = 0
                try:
                    sign, rwnd, seq, data = self.package.unpack(raw)
                except Exception as e:
                    self.log.warning(
                        f"Unable to unpack received package due to the error : {e}, droped."
                    )
                if sign != self.sign:
                    self.log.warning(f"Receive an unknown sign package, droped.")
                elif seq == self.unackseq - 1:
                    self.dupack += 1
                    self.update_cwnd(seq)
                    # 三次冗余ACK,快速重传
                    if self.dupack == 3:
                        time.sleep(0.5)
                        self.totalfastresend += 1
                        cnt = np.ceil(np.min([self.rwnd, self.cwnd]))
                        self.log.warning(
                            f"Receive uncorrect ACK {self.unackseq} three times, Resending package from {self.unackseq} to {self.unackseq + cnt - 1}"
                        )
                        # Thread(target = self.resend).start()
                        self.resend(cnt)
                        self.update_cwnd(DUP_ACK)
                        self.dupack = 0
                    # 大于等于当前unackseq,更新unackseq,并删除相应数据包
                elif seq >= self.unackseq:
                    self.log.info(f"Receive ACK {seq}/{self.total_package}")
                    for _ in range(seq - self.unackseq + 1):
                        self.update_cwnd(self.unackseq)
                        self.unackseq += 1
                        self.buffer.popleft()
                    self.rwnd = rwnd
                    self.rwnddata.append(rwnd)
                    self.dupack = 0
                else:
                    # 小于unackseq - 1,忽略
                    self.log.info(f"Receive smaller ACK {seq}, droped.")
                    self.dupack = 0
                # 接收到数据包，超时次数清零
                cnt = 0

            except timeout:
                # 如果当前未发送包，自然也不会收到ACK,此处防止出现罕见bug
                if self.unackseq == self.nextseq:  # windows size is zero
                    continue
                self.totaltimeout += 1
                cnt += 1
                if cnt == timeout_count:
                    # 连续超时timeout_count(定义在config.config)次，认为网络出现问题，停止传输
                    self.log.warning(
                        f"Timeout for receiving ACK for {timeout_count} times!!! Aborted."
                    )
                    self.status = status.CLOSE
                    return
                self.update_RTO(self.RTO)
                cnt = np.ceil(np.min([self.rwnd, self.cwnd]))
                self.log.warning(
                    f"Receive ACK {self.unackseq} Timeout, Resending package from {self.unackseq} to {self.unackseq + cnt - 1}"
                )
                self.update_cwnd(TIMEOUT_ACK)
                # 超时重传
                self.resend(cnt)
                # Thread(target = self.resend).start()
                continue

            except Exception as e:
                self.log.err(f"Error occured when handling receive ACK: {e}, ignore.")
                continue
        self.log.info("----------------Receiving complete----------------")

    def summary(self):
        """总结
        - 将rwnd，cwnd, rto输出到文件中，用于主进程中汇总制表
        - 因为plot库不能在非主进程运行
        """

        with open(f"{self.file}_data.log", "w") as f:
            f.write(" ".join([str(i) for i in self.rwnddata]))
            f.write("\n")
            f.write(" ".join([str(i) for i in self.cwnddata]))
            f.write("\n")
            f.write(" ".join([str(i) for i in self.rtodata]))
            f.write("\n")
            f.write(f"Total lost times is {self.totaltimeout}")
            f.write("\n")
            f.write(f"Total duplicate times is {self.totalfastresend}")
        print(f"Total lost times is {self.totaltimeout}")
        print(f"Total duplicate times is {self.totalfastresend}")

    def start(self):
        """启动函数
        - 切换状态为慢启动
        - 启动发送数据进程
        - 启动接收ACK进程
        - 等待前两个进程执行完毕
        - 执行summary函数，保存数据
        """

        self.status = status.SLOW_START
        t1 = Thread(target=self.send)
        self.log.info(f"start send data thread")
        t1.start()
        t2 = Thread(target=self.receive)
        self.log.info(f"start receive ack thread")
        t2.start()
        t1.join()
        t2.join()
        if self.total_package != self.unackseq - 1:
            self.log.warning(f"Stop unnormally!")
        self.summary()
