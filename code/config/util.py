#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from hashlib import md5 as md5sum
from . import config


def check_fileinfo(file, info):
    """检查文件信息
    - file 要检查的文件
    - info[0] 要检查的文件前多少字节
    - info[1] 该内容对应的md5码
    """

    size = int(info[0])
    md5 = str(info[1])
    if not os.path.exists(file):
        return config.FILENOTFOUND
    with open(file, "rb") as f:
        data = f.read(size)
    file_md5 = str(md5sum(data).hexdigest())
    if file_md5 != md5:
        return config.resend
    # md5码一致，可以续传
    return config.cosend


def get_fileinfo(file):
    """获取文件信息
    - file 要检查的文件
    - 返回值：[文件大小， md5码]
    """

    # 文件不存在，返回['0', '0']
    if not os.path.exists(file):
        return ["0", "0"]
    # 获取已有的文件大小
    size = str(os.path.getsize(file))
    with open(file, "rb") as f:
        data = f.read()
    md5 = str(md5sum(data).hexdigest())
    return [size, md5]
