using System;
using System.Threading.Tasks;

namespace Framework.LocalTransfer.Tests
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            if (args.Length > 0 && args[0].Equals("StressTest", StringComparison.OrdinalIgnoreCase))
            {
                await TransferStressTest.RunAsync();
            }
            else if (args.Length > 0 && args[0].Equals("CleanupTest", StringComparison.OrdinalIgnoreCase))
            {
                await CleanupTest.RunAsync();
            }
            else
            {
                await TransferResumeTest.RunAsync();
            }
        }
    }
}
