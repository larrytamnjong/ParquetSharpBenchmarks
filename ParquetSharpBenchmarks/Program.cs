class Program
{
    //static void Main(string[] args)
    //{
    //    BenchmarkRunner.Run<ParquetDotNetBenchmarks>();
    //}

    static async Task Main(string[] args)
    {
        await ParquetBenchmarks.RunAsync(args);
    }
}