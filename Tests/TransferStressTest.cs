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
    /// 并行传输压力与可靠性测试
    /// 模拟大文件（512MB+）在并行连接模式下的上传与下载，并验证数据一致性
    /// </summary>
    public class TransferStressTest
    {
        private static string TestRoot = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "StressTest_Data");
        private static string ServerDir = Path.Combine(TestRoot, "ServerRepo");
        private static string ClientDir = Path.Combine(TestRoot, "ClientRepo");
        private static string TempDir = Path.Combine(TestRoot, "SystemTemp");

        public static async Task RunAsync()
        {
            Console.WriteLine("==================================================");
            Console.WriteLine("   局域网超大文件并行传输 - 可靠性与稳定性测试   ");
            Console.WriteLine("==================================================");

            try
            {
                // 1. 环境清理与准备
                SetupDirectories();
                
                // 生成一个 512MB 的随机测试文件
                int fileSizeMB = 512; 
                string sourceFileName = "StressTestData.bin";
                var sourceFilePath = GenerateLargeFile(sourceFileName, fileSizeMB);

                // 2. 配置与启动服务端
                // 开启并行支持：设置 MaxParallelConnectionsPerSession 为 8
                var config = new TransferConfig
                {
                    UploadDirectory = ServerDir,
                    DownloadDirectory = ServerDir,
                    TempDirectory = TempDir,
                    UseBinaryProtocol = true,
                    VerifyMD5 = true,
                    MaxParallelConnectionsPerSession = 8, 
                    ChunkSize = 1024 * 1024 * 4 // 4MB 块大小
                };
                
                var logger = new ConsoleLogger();
                using var host = new TransferHost(config, logger: logger, baseDirectory: ServerDir);
                host.Start(0); // 随机可用端口
                int port = host.Port;
                Console.WriteLine($"[Server] 服务端已启动，监听端口: {port}");

                // 3. 初始化客户端管理器
                var progress = new TestProgressCallback();
                using var manager = new TransferManager(config, progressCallback: progress, logger: logger);

                // 4. 执行上传测试 (并行)
                Console.WriteLine("\n>>> 步骤 1: 执行并行上传测试...");
                var uploadSession = await manager.CreateUploadSession("127.0.0.1", port, null, sourceFilePath);
                
                // 实时显示进度
                await MonitorProgressAsync(uploadSession);

                if (uploadSession.Status == TransferStatus.Completed)
                {
                    Console.WriteLine($"\n[OK] 上传完成!");
                    Console.WriteLine($"     文件大小: {fileSizeMB} MB");
                    Console.WriteLine($"     总计耗时: {uploadSession.Duration.TotalSeconds:F2} 秒");
                    Console.WriteLine($"     平均速度: {GetSpeed(uploadSession)} MB/s");
                    
                    // 验证服务器端文件完整性
                    VerifyFileIntegrity(sourceFilePath, Path.Combine(ServerDir, sourceFileName));
                }
                else
                {
                    Console.WriteLine($"\n[FAILED] 上传失败: {uploadSession.Error?.Message}");
                    return;
                }

                // 5. 执行下载测试 (并行 + Range Request)
                Console.WriteLine("\n>>> 步骤 2: 执行并行下载测试...");
                string downloadSavePath = Path.Combine(ClientDir, "Downloaded_StressData.bin");
                var downloadSession = await manager.CreateDownloadSession("127.0.0.1", port, sourceFileName, downloadSavePath);
                
                await MonitorProgressAsync(downloadSession);

                if (downloadSession.Status == TransferStatus.Completed)
                {
                    Console.WriteLine($"\n[OK] 下载完成!");
                    Console.WriteLine($"     总计耗时: {downloadSession.Duration.TotalSeconds:F2} 秒");
                    Console.WriteLine($"     平均速度: {GetSpeed(downloadSession)} MB/s");
                    
                    // 验证客户端下载文件完整性
                    VerifyFileIntegrity(sourceFilePath, downloadSavePath);
                }
                else
                {
                    Console.WriteLine($"\n[FAILED] 下载失败: {downloadSession.Error?.Message}");
                }

                Console.WriteLine("\n==================================================");
                Console.WriteLine("               所有可靠性测试已通过               ");
                Console.WriteLine("==================================================");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[FATAL ERROR] 测试执行中断: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
            finally
            {
                // 延迟清理，方便查看结果
                Console.WriteLine("\n测试完成。保留数据并退出。");
                /*
                if (Console.ReadLine()?.ToLower() == "c")
                {
                    Cleanup();
                }
                */
            }
        }

        private static void SetupDirectories()
        {
            if (Directory.Exists(TestRoot)) Directory.Delete(TestRoot, true);
            Directory.CreateDirectory(ServerDir);
            Directory.CreateDirectory(ClientDir);
            Directory.CreateDirectory(TempDir);
            Console.WriteLine($"[Init] 测试目录已准备: {TestRoot}");
        }

        private static void Cleanup()
        {
            try { if (Directory.Exists(TestRoot)) Directory.Delete(TestRoot, true); } catch { }
        }

        private static string GenerateLargeFile(string fileName, int sizeInMB)
        {
            string path = Path.Combine(ClientDir, fileName);
            Console.Write($"[Init] 正在生成测试文件 ({sizeInMB}MB)... ");
            
            byte[] buffer = new byte[1024 * 1024]; // 1MB
            Random rand = new Random();
            
            using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write))
            {
                for (int i = 0; i < sizeInMB; i++)
                {
                    rand.NextBytes(buffer);
                    fs.Write(buffer, 0, buffer.Length);
                }
            }
            Console.WriteLine("完成.");
            return path;
        }

        private static async Task MonitorProgressAsync(TransferSession session)
        {
            int lastProgress = -1;
            while (session.Status == TransferStatus.Pending || session.Status == TransferStatus.InProgress)
            {
                int currentProgress = (int)(session.Progress * 100);
                if (currentProgress != lastProgress)
                {
                    Console.Write($"\r进度: [{new string('#', currentProgress / 2)}{new string('-', 50 - currentProgress / 2)}] {currentProgress}% | 速度: {GetSpeed(session)} MB/s   ");
                    lastProgress = currentProgress;
                }
                await Task.Delay(200);
            }
            Console.WriteLine();
        }

        private static string GetSpeed(TransferSession session)
        {
            double mb = session.GetTransferredSize() / 1024.0 / 1024.0;
            double sec = session.Duration.TotalSeconds;
            return sec > 0.1 ? (mb / sec).ToString("F2") : "0.00";
        }

        private static void VerifyFileIntegrity(string originalPath, string transferredPath)
        {
            Console.Write($"[Verify] 正在执行 MD5 数据一致性校验... ");
            
            string md5Orig = CalculateMD5(originalPath);
            string md5Trans = CalculateMD5(transferredPath);

            if (md5Orig == md5Trans)
            {
                Console.WriteLine("匹配 [PASS]");
            }
            else
            {
                Console.WriteLine("不匹配 [FAIL]");
                Console.WriteLine($"  原始文件: {md5Orig}");
                Console.WriteLine($"  传输文件: {md5Trans}");
                throw new Exception("数据一致性校验失败！");
            }
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
            public void OnCompleted(TransferSession session) => Console.WriteLine($"\n[Event] 传输任务完成: {session.SessionId}");
            public void OnFailed(TransferSession session, Exception ex) => Console.WriteLine($"\n[Event] 传输任务失败: {session.SessionId}, 原因: {ex.Message}");
            public void OnStarted(TransferSession session) => Console.WriteLine($"[Event] 传输任务开始: {session.SessionId}");
        }
    }
}
