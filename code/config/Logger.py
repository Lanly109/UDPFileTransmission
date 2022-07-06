#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import os


class Logger:
    '''日志类
    - 保存在log文件夹
    - info 正常记录
    - warning 警告记录
    - err 错误记录
    '''

    def __init__(self, prefix):
        self.prefix = prefix
        os.makedirs("./log", exist_ok = True)
        self.inf = open(f"./log/{prefix}_info.log", "a")
        self.warn = open(f"./log/{prefix}_warning.log", "a")
        self.er = open(f"./log/{prefix}_error.log", "a") 

    def info(self, info):
        print(f"\033[38m[{datetime.datetime.now()} {self.prefix}] INFO: {info}")
        self.inf.write(f"{datetime.datetime.now()} {self.prefix}] INFO: {info}\n")
    
    def warning(self, warning):
        print(f"\033[33m[{datetime.datetime.now()} {self.prefix}] WARNING: {warning}\033[38m")
        self.warn.write(f"{datetime.datetime.now()} {self.prefix}] WARNING: {warning}\n")

    def err(self, err):
        print(f"\033[31m[{datetime.datetime.now()} {self.prefix}] ERROR: {err}\033[38m")
        self.er.write(f"{datetime.datetime.now()} {self.prefix}] ERROR: {err}\n")
