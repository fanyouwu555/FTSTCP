using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Framework.LocalTransfer;

namespace Framework.LocalTransfer.Tests
{
    public class CleanupTest
    {
        public static async Task RunAsync()
        {
            Console.WriteLine("==================================================");
            Console.WriteLine("               资源清理与文件锁释放测试");
            Console.WriteLine("==================================================");

            string baseDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "CleanupTest_Data");
            string serverRepo = Path.Combine(baseDir, "ServerRepo");
            string clientRepo = Path.Combine(baseDir, "ClientRepo");
            string tempDir = Path.Combine(baseDir, "Temp");

            if (Directory.Exists(baseDir)) Directory.Delete(baseDir, true);
            Directory.CreateDirectory(serverRepo);
            Directory.CreateDirectory(clientRepo);
            Directory.CreateDirectory(tempDir);

            var config = new TransferConfig
            {
                UploadDirectory = serverRepo,
                DownloadDirectory = clientRepo,
                TempDirectory = tempDir,
                UseBinaryProtocol = true,
                MaxParallelConnectionsPerSession = 4
            };

            using var host = new TransferHost(config);
            host.Start(0);
            int port = host.Port;

            var manager = new TransferClient(config);

            // 1. 测试异常断开后的清理
            Console.WriteLine("\n[Test 1] 测试异常断开后的清理...");
            
            string testFile = Path.Combine(serverRepo, "CleanupTestData.bin");
            byte[] data = new byte[256 * 1024 * 1024]; // 256MB
            new Random().NextBytes(data);
            await File.WriteAllBytesAsync(testFile, data);

            var session = await manager.CreateDownloadSession("127.0.0.1", port, "CleanupTestData.bin", Path.Combine(clientRepo, "Downloaded_Cleanup.bin"));
            
            // 等待一段时间后取消（模拟异常断开）
            await Task.Delay(200);
            Console.WriteLine("[Action] 正在取消下载任务 (模拟异常断开)...");
            session.Cancel();

            try
            {
                await session.WaitAsync();
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("[Info] 下载任务已取消");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Info] 下载任务结束: {ex.Message}");
            }

            // 等待服务器清理
            Console.WriteLine("[Wait] 等待服务器清理会话 (预计 1 秒)...");
            await Task.Delay(1000);

            if (host.ActiveSessions == 0)
            {
                Console.WriteLine("[OK] 服务器已清理活跃会话");
            }
            else
            {
                Console.WriteLine($"[FAIL] 服务器仍有 {host.ActiveSessions} 个活跃会话");
            }

            // 检查文件锁是否释放
            Console.WriteLine("[Check] 检查文件锁是否已释放...");
            try
            {
                using (var fs = new FileStream(testFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                {
                    Console.WriteLine("[OK] 源文件锁已释放，可以独占访问");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FAIL] 源文件锁未释放: {ex.Message}");
            }

            // 检查临时文件是否可以删除
            string[] tempFiles = Directory.GetFiles(tempDir, "*.part");
            if (tempFiles.Length > 0)
            {
                Console.WriteLine($"[Check] 检查临时文件锁是否已释放: {tempFiles[0]}");
                try
                {
                    File.Delete(tempFiles[0]);
                    Console.WriteLine("[OK] 临时文件已成功删除，锁已释放");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[FAIL] 临时文件锁未释放: {ex.Message}");
                }
            }

            Console.WriteLine("\n==================================================");
            Console.WriteLine("               清理测试完成");
            Console.WriteLine("==================================================");
        }
    }
}
