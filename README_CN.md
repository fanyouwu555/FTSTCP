# FileTransferServer 使用说明文档

本文档详细介绍了 FileTransferServer 项目的功能、API 使用方法、配置项说明以及可视化工具的操作指南。

## 1. 项目简介

FileTransferServer 是一个基于 C# (Socket TCP) 实现的高性能文件传输库。它支持大文件传输、断点续传、并发控制、数据压缩与加密等功能，适用于需要稳定可靠文件传输服务的场景。

## 2. 核心功能

*   **断点续传**：自动记录传输进度，网络中断或重启后可从上次位置继续传输。
*   **并发控制**：服务端和客户端均支持并发连接限制，防止资源耗尽。
*   **高效传输**：
    *   支持 **二进制协议 (Binary Protocol)**，减少序列化开销。
    *   支持 **GZip 压缩**，节省带宽。
    *   支持 **AES 加密**，保障数据安全。
*   **完整性校验**：支持 MD5 校验，确保文件一致性。
*   **可视化工具**：提供 WinForms 客户端/服务端一体化测试工具。

## 3. API 使用指南

### 3.1 核心命名空间
```csharp
using Framework.LocalTransfer;
```

### 3.2 服务端 (TransferHost)

服务端负责监听端口，接收来自客户端的上传或下载请求。

**启动服务端：**
```csharp
// 1. 创建配置
var config = new TransferConfig
{
    UploadDirectory = @"C:\ServerData\Uploads",   // 客户端上传文件的保存位置
    TempDirectory = @"C:\ServerData\Temp",        // 临时文件目录
    MaxConcurrentSessions = 20,                   // 最大并发数
    UseBinaryProtocol = true                      // 启用二进制协议
};

// 2. 初始化 Host
// 参数: 配置, 压缩处理(可选), 加密处理(可选), 根目录(可选), 日志记录器(可选)
var server = new TransferHost(config);

// 3. 启动监听
int port = 6666;
server.Start(port);
Console.WriteLine($"Server started on port {port}");

// 4. 停止服务
// server.Stop();
// server.Dispose();
```

### 3.3 客户端 (TransferManager)

客户端用于主动发起上传或下载任务。

**初始化管理器：**
```csharp
// 1. 创建配置
var config = new TransferConfig
{
    DownloadDirectory = @"C:\ClientData\Downloads",
    TempDirectory = @"C:\ClientData\Temp",
    UseBinaryProtocol = true
};

// 2. 初始化 Manager
var manager = new TransferManager(config);
```

**发起上传任务：**
```csharp
string serverIp = "127.0.0.1";
int serverPort = 6666;
string localFilePath = @"C:\Docs\Report.pdf";

// 创建并启动上传会话
var session = await manager.CreateUploadSession(serverIp, serverPort, null, localFilePath);

// 监听进度（可选）
while (session.Status == TransferStatus.InProgress)
{
    Console.WriteLine($"Progress: {session.Progress:P0}");
    await Task.Delay(500);
}
```

**发起下载任务：**
```csharp
string remotePath = "Report.pdf"; // 相对于服务端 UploadDirectory 的路径
string localSavePath = @"C:\ClientData\Downloads\Report.pdf";

// 创建并启动下载会话
var session = await manager.CreateDownloadSession(serverIp, serverPort, remotePath, localSavePath);
```

## 4. 配置详解 (TransferConfig)

`TransferConfig` 类控制传输的核心行为，以下是主要属性说明：

| 属性名 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| **基础路径配置** | | | |
| `UploadDirectory` | string | "Uploads" | 服务端：上传文件存储根目录；下载文件的源目录。 |
| `DownloadDirectory` | string | "Downloads" | 客户端：默认下载目录（API调用时可覆盖）。 |
| `TempDirectory` | string | "TransferTemp" | 存放传输过程中的 `.tmp` 或 `.part` 临时文件。 |
| **传输行为** | | | |
| `UseBinaryProtocol` | bool | `true` | **推荐开启**。使用自定义二进制协议，性能优于 JSON 协议。 |
| `ChunkSize` | int | 512 KB | 默认分块大小。大文件传输时库会自动动态调整此值。 |
| `MaxConcurrentSessions`| int | 5 | 并发限制。服务端建议设大（如20），客户端建议设小（如5）。 |
| `MaxPacketSize` | int | 10 MB | 单个网络包的最大限制，防止内存溢出。 |
| **功能开关** | | | |
| `EnableCompression` | bool | `true` | 是否启用 GZip 压缩。适合文本或非压缩格式文件。 |
| `EnableEncryption` | bool | `false` | 是否启用数据加密（需提供 EncryptionHandler）。 |
| `VerifyMD5` | bool | `false` | 传输完成后是否计算全文件 MD5 并校验。 |
| **网络参数** | | | |
| `ConnectionTimeoutMs` | int | 30000 | 连接/读写超时时间 (毫秒)。 |
| `RetryCount` | int | 5 | 自动重试次数。 |
| `RetryDelayMs` | int | 2000 | 重试基础延迟 (毫秒)。 |

## 5. 可视化工具 (FileTransferTool) 使用说明

项目包含一个名为 `FileTransferTool` 的 WinForms 程序，用于测试和演示。

### 5.1 服务端模式 (Server Mode)
1.  打开程序，选择 **Server Mode** 选项卡。
2.  **Port**: 输入监听端口（默认 6666）。
3.  点击 **Start** 按钮启动服务。
4.  此时下方的 Logs 窗口会显示服务启动日志。

### 5.2 客户端模式 (Client Mode)
1.  选择 **Client Mode** 选项卡。
2.  **Connection 设置**:
    *   **IP**: 输入服务端 IP（本地测试用 127.0.0.1）。
    *   **Port**: 输入服务端端口。
    *   点击 **Connect**：测试连接连通性。成功后会锁定设置并启用上传/下载功能。
3.  **Upload (上传)**:
    *   点击 **Browse...** 选择本地文件。
    *   点击 **Upload**：文件将被上传到服务端的 `data\uploads` 目录。
4.  **Download (下载)**:
    *   **Remote Path**: 输入想要下载的文件名（相对于服务端 `uploads` 目录）。例如 `test.mp4` 或 `folder/test.txt`。
    *   **Save As...**: 输入本地保存的完整路径。
    *   点击 **Download**。

### 5.3 Settings (设置)
在 **Settings** 选项卡中可以实时调整传输策略（仅在未连接/未启动时生效）：
*   **Enable Compression**: 开启压缩。
*   **Use Binary Protocol**: 切换协议模式。
*   **Verify MD5**: 开启强校验。

## 6. 常见问题与注意事项

1.  **并发限制**：
    *   支持多客户端同时上传/下载。
    *   **同名文件上传**：后完成的任务会覆盖旧文件（旧文件会被重命名为 `.bak`）。
    *   **同路径下载**：客户端会自动排队写入同一个本地路径，防止文件损坏。
2.  **路径说明**：
    *   服务端根目录默认为程序运行目录下的 `data` 文件夹。
    *   下载时的 `Remote Path` 是相对路径，基于服务端的 `UploadDirectory`。
3.  **日志查看**：
    *   无论是服务端还是客户端操作，详细的进度、速度、耗时都会实时输出到工具底部的 Logs 窗口。
