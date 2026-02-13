using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Framework.LocalTransfer;

namespace Framework.LocalTransfer.Tests
{
    /// <summary>
    /// 断点续传测试
    /// 模拟传输过程中断并恢复，验证数据一致性
    /// </summary>
    public class TransferResumeTest
    {
        private static string TestRoot = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ResumeTest_Data");
        private static string ServerDir = Path.Combine(TestRoot, "ServerRepo");
        private static string ClientDir = Path.Combine(TestRoot, "ClientRepo");
        private static string TempDir = Path.Combine(TestRoot, "SystemTemp");

        public static async Task RunAsync()
        {
            Console.WriteLine("==================================================");
            Console.WriteLine("        局域网超大文件传输 - 断点续传测试        ");
            Console.WriteLine("==================================================");

            try
            {
                SetupDirectories();
                
                // 生成一个 128MB 的随机测试文件
                int fileSizeMB = 128; 
                string fileName = "ResumeTestData.bin";
                var sourceFilePath = GenerateLargeFile(fileName, fileSizeMB);

                var config = new TransferConfig
                {
                    UploadDirectory = ServerDir,
                    DownloadDirectory = ServerDir,
                    TempDirectory = TempDir,
                    UseBinaryProtocol = true,
                    VerifyMD5 = true,
                    MaxParallelConnectionsPerSession = 4, 
                    ChunkSize = 1024 * 1024 * 2 // 2MB 块大小
                };
                
                var logger = new ConsoleLogger();
                using var host = new TransferHost(config, logger: logger, baseDirectory: ServerDir);
                host.Start(0);
                int port = host.Port;
                Console.WriteLine($"[Server] 服务端已启动，监听端口: {port}");

                var progress = new TestProgressCallback();
                using var manager = new TransferClient(config, progressCallback: progress, logger: logger);

                // --- 1. 上传续传测试 ---
                Console.WriteLine("\n>>> 步骤 1: 上传续传测试...");
                
                // 第一部分：开始上传并中途取消
                var uploadSession1 = await manager.CreateUploadSession("127.0.0.1", port, null, sourceFilePath);
                Console.WriteLine($"[Test] 开始第一次上传 (Session: {uploadSession1.SessionId})...");
                
                // 等待到进度约 40% 时取消
                await WaitUntilProgress(uploadSession1, 0.4f);
                Console.WriteLine($"\n[Test] 进度达到 {uploadSession1.Progress:P2}, 正在主动取消会话...");
                uploadSession1.Cts.Cancel();
                
                try { await uploadSession1.WaitAsync(); } catch { }
                Console.WriteLine($"[Test] 第一次上传已停止. 状态: {uploadSession1.Status}");

                // 第二部分：重新开始上传（续传）
                Console.WriteLine("\n[Test] 正在尝试续传上传...");
                var uploadSession2 = await manager.CreateUploadSession("127.0.0.1", port, null, sourceFilePath);
                
                // 等待直到状态变为 InProgress 且进度已同步（或者超时）
                int waitCount = 0;
                while (uploadSession2.Status == TransferStatus.Pending && waitCount < 100)
                {
                    await Task.Delay(50);
                    waitCount++;
                }
                
                // 再给一点时间让 ExecuteUploadBinaryAsync 完成响应处理并设置进度
                await Task.Delay(500);
                
                Console.WriteLine($"[Test] 续传会话已启动 (Session: {uploadSession2.SessionId}), 状态: {uploadSession2.Status}, 初始进度: {uploadSession2.Progress:P2}");
                await MonitorProgressAsync(uploadSession2);

                if (uploadSession2.Status == TransferStatus.Completed)
                {
                    Console.WriteLine($"\n[OK] 续传上传完成!");
                    VerifyFileIntegrity(sourceFilePath, Path.Combine(ServerDir, fileName));
                }
                else
                {
                    Console.WriteLine($"\n[FAILED] 续传上传失败: {uploadSession2.Error?.Message}");
                    return;
                }

                // --- 2. 下载续传测试 ---
                Console.WriteLine("\n>>> 步骤 2: 下载续传测试...");
                string downloadSavePath = Path.Combine(ClientDir, "Downloaded_ResumeData.bin");
                
                // 第一部分：开始下载并中途取消
                var downloadSession1 = await manager.CreateDownloadSession("127.0.0.1", port, fileName, downloadSavePath);
                Console.WriteLine($"[Test] 开始第一次下载 (Session: {downloadSession1.SessionId})...");
                
                await WaitUntilProgress(downloadSession1, 0.4f);
                Console.WriteLine($"\n[Test] 进度达到 {downloadSession1.Progress:P2}, 正在主动取消会话...");
                downloadSession1.Cts.Cancel();
                
                try { await downloadSession1.WaitAsync(); } catch { }
                Console.WriteLine($"[Test] 第一次下载已停止. 状态: {downloadSession1.Status}");

                // 第二部分：重新开始下载（续传）
                Console.WriteLine("\n[Test] 正在尝试续传下载...");
                var downloadSession2 = await manager.CreateDownloadSession("127.0.0.1", port, fileName, downloadSavePath);
                await MonitorProgressAsync(downloadSession2);

                if (downloadSession2.Status == TransferStatus.Completed)
                {
                    Console.WriteLine($"\n[OK] 续传下载完成!");
                    VerifyFileIntegrity(sourceFilePath, downloadSavePath);
                }
                else
                {
                    Console.WriteLine($"\n[FAILED] 续传下载失败: {downloadSession2.Error?.Message}");
                }

                Console.WriteLine("\n==================================================");
                Console.WriteLine("               断点续传测试已通过               ");
                Console.WriteLine("==================================================");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[FATAL ERROR] 测试执行中断: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        private static void SetupDirectories()
        {
            if (Directory.Exists(TestRoot)) Directory.Delete(TestRoot, true);
            Directory.CreateDirectory(ServerDir);
            Directory.CreateDirectory(ClientDir);
            Directory.CreateDirectory(TempDir);
        }

        private static string GenerateLargeFile(string fileName, int sizeInMB)
        {
            string path = Path.Combine(ClientDir, fileName);
            byte[] buffer = new byte[1024 * 1024];
            Random rand = new Random();
            using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write))
            {
                for (int i = 0; i < sizeInMB; i++)
                {
                    rand.NextBytes(buffer);
                    fs.Write(buffer, 0, buffer.Length);
                }
            }
            return path;
        }

        private static async Task WaitUntilProgress(TransferSession session, float targetProgress)
        {
            while (session.Progress < targetProgress && (session.Status == TransferStatus.Pending || session.Status == TransferStatus.InProgress))
            {
                // Console.Write($"\r当前进度: {session.Progress:P2}   ");
                await Task.Delay(100);
            }
        }

        private static async Task MonitorProgressAsync(TransferSession session)
        {
            int lastPercent = -1;
            while (session.Status == TransferStatus.Pending || session.Status == TransferStatus.InProgress)
            {
                int currentPercent = (int)(session.Progress * 100);
                if (currentPercent != lastPercent)
                {
                    Console.WriteLine($"进度: {session.Progress:P2} | 速度: {GetSpeed(session)} MB/s");
                    lastPercent = currentPercent;
                }
                await Task.Delay(500);
            }
            Console.WriteLine($"最终进度: {session.Progress:P2}");
        }

        private static string GetSpeed(TransferSession session)
        {
            double mb = session.GetTransferredSize() / 1024.0 / 1024.0;
            double sec = session.Duration.TotalSeconds;
            return sec > 0.1 ? (mb / sec).ToString("F2") : "0.00";
        }

        private static void VerifyFileIntegrity(string originalPath, string transferredPath)
        {
            string md5Orig = CalculateMD5(originalPath);
            string md5Trans = CalculateMD5(transferredPath);
            if (md5Orig != md5Trans) throw new Exception("MD5校验失败");
            Console.WriteLine("[Verify] MD5 校验通过");
        }

        private static string CalculateMD5(string filename)
        {
            using (var md5 = MD5.Create())
            {
                using (var stream = File.OpenRead(filename))
                {
                    var hash = md5.ComputeHash(stream);
                    return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
                }
            }
        }

        private class TestProgressCallback : ITransferProgressCallback
        {
            public void OnProgress(TransferSession session) { }
            public void OnCompleted(TransferSession session) { }
            public void OnFailed(TransferSession session, Exception ex) { }
            public void OnStarted(TransferSession session) { }
        }
    }
}
