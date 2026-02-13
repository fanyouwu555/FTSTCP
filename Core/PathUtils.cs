using System;
using System.IO;
using System.Linq;

namespace Framework.LocalTransfer
{
    /// <summary>
    /// 路径处理工具类，提供安全检查
    /// </summary>
    public static class PathUtils
    {
        public static string CombineAndValidatePath(string baseDirectory, params string[] parts)
        {
            if (parts == null || parts.Length == 0)
                return CombineAndValidatePath(baseDirectory, string.Empty);

            var filtered = parts.Where(p => !string.IsNullOrWhiteSpace(p)).ToArray();
            if (filtered.Length == 0)
                return CombineAndValidatePath(baseDirectory, string.Empty);

            return CombineAndValidatePath(baseDirectory, Path.Combine(filtered));
        }

        /// <summary>
        /// 规范化并验证路径是否安全（防止目录遍历）
        /// </summary>
        /// <param name="baseDirectory">基础目录</param>
        /// <param name="relativePath">相对路径</param>
        /// <returns>安全的绝对路径</returns>
        /// <exception cref="UnauthorizedAccessException">当路径尝试跳出基础目录时抛出</exception>
        public static string CombineAndValidatePath(string baseDirectory, string relativePath)
        {
            if (string.IsNullOrWhiteSpace(baseDirectory))
                throw new ArgumentNullException(nameof(baseDirectory));

            // 1. 清理文件名中的非法字符
            if (string.IsNullOrWhiteSpace(relativePath))
                return Path.GetFullPath(baseDirectory);

            // 移除路径中可能的非法字符（根据操作系统）
            foreach (char c in Path.GetInvalidPathChars())
            {
                if (relativePath.IndexOf(c) >= 0)
                    throw new ArgumentException($"路径包含非法字符: {c}");
            }

            // 2. 获取绝对路径
            string fullBasePath = Path.GetFullPath(baseDirectory);
            
            // 简单防范：不允许绝对路径作为 relativePath
            if (Path.IsPathRooted(relativePath))
            {
                // 如果传入的是绝对路径，检查它是否在 baseDirectory 下
                string fullTarget = Path.GetFullPath(relativePath);
                if (!fullTarget.StartsWith(fullBasePath, StringComparison.OrdinalIgnoreCase))
                {
                    throw new UnauthorizedAccessException("禁止访问基础目录以外的路径");
                }
                return fullTarget;
            }

            // 3. 组合路径
            string combinedPath = Path.Combine(fullBasePath, relativePath);
            string fullCombinedPath = Path.GetFullPath(combinedPath);

            // 4. 关键检查：确保最终路径以基础路径开头
            if (!fullCombinedPath.StartsWith(fullBasePath, StringComparison.OrdinalIgnoreCase))
            {
                throw new UnauthorizedAccessException($"检测到目录遍历攻击尝试: {relativePath}");
            }

            return fullCombinedPath;
        }

        /// <summary>
        /// 确保目录存在
        /// </summary>
        public static void EnsureDirectory(string path)
        {
            if (!string.IsNullOrEmpty(path))
            {
                string dir = Path.HasExtension(path) ? Path.GetDirectoryName(path) : path;
                if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                {
                    Directory.CreateDirectory(dir);
                }
            }
        }
    }
}
