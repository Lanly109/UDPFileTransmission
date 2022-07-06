# 基于UDP的可靠文件传输程序

## 安装依赖

```bash
pip install -r requirements.txt
```

## 运行服务端

```bash
python3 Server.py
```

服务端监听端口在`config/config.py`文件中修改。

## 客户端使用

```bash
python3 Client.py <send/receive> <filename/dirname>
```

客户端连接服务端的地址在`Client.py`文件中修改。

`<filename/dirname>`请采用相对路径。
