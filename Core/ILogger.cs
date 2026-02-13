using System;

namespace Framework.LocalTransfer
{
    /// <summary>
    /// 日志接口，用于解耦 Unity 依赖
    /// </summary>
    public interface ILogger
    {
        void LogInfo(string message);
        void LogWarning(string message);
        void LogError(string message);
        void LogDebug(string message);
    }

    /// <summary>
    /// 默认的控制台日志实现
    /// </summary>
    public class ConsoleLogger : ILogger
    {
        public void LogInfo(string message) => Console.WriteLine($"[Info] {message}");
        public void LogWarning(string message) => Console.WriteLine($"[Warn] {message}");
        public void LogError(string message) => Console.WriteLine($"[Error] {message}");
        public void LogDebug(string message) 
        {
#if DEBUG
            Console.WriteLine($"[Debug] {message}");
#endif
        }
    }
}
