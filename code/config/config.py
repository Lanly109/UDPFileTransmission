#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from struct import Struct, pack


class status(Enum):
    """状态类"""

    CLOSE = 0
    SLOW_START = 1
    AVOID = 2
    FASTRE_RECOVERY = 3
    WORK = 4


# 数据报文中数据段最大长度
MSS = 1024 * 5
# 起始报文序号
startnum = 0

# 请求报文数据段最大长度
reMSS = 64
# 请求报文结构
repackage = Struct(f"!HHI{reMSS}s")
# 请求报文大小
reMSS_size = repackage.size

# 超时ACK标识，用于update_cwnd
TIMEOUT_ACK = -1
# 冗余ACK标识，用于update_cwnd
DUP_ACK = -2

# 最小RTO防止RTO过小易触发超时
Minimum_RTO = 0.5

# 发送指令的数据段报文内容
send_command = "s"
# 接收指令的数据段报文内容
receive_command = "r"
# 重传指令的数据段报文内容
resend = "0"
# 续传指令的数据段报文内容
cosend = "1"
# 重置指令的数据段报文内容
RESET = "-1"
# 文件未找到的数据段报文内容
FILENOTFOUND = "2"
# 允许的数据段报文内容
OK = "3"
# 请求端口号的数据段报文内容
REQUESTPORT = "4"

# 服务端监听端口
hostport = 22222
# 服务端创建进程监听的起始端口
startport = 12000
# 服务端创建进程监听端口的间隔
gapport = 1

# 数据段不同内容的分隔符，用于起初的握手报文
spliter = "$^!&"

# 默认的滑动窗口大小
default_rwnd = 128

# 结束报文的窗口大小标识
DONE = 65532
# 获取接收方窗口大小的报文的窗口大小标识
GetWindowsSize = 65534

# 接收数据的超时时间
time_limit = 10
# 接收ACK允许的连续超时次数
timeout_count = 5

# 计算RTO的一些参数
alpha = 0.125
beta = 0.25
mu = 1
rao = 4
