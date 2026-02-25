using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using ParquetSharp;
using ParquetSharp.Arrow;
using System.Buffers;



namespace ParquetSharpBenchmarks
{

    [MemoryDiagnoser]
    [Config(typeof(Config))]
    public class ParquetDotNetBenchmarks 
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(Job.Default
                    .WithRuntime(CoreRuntime.Core10_0)
                    .WithWarmupCount(2)
                    .WithIterationCount(5)
                    .WithLaunchCount(1)
                    .WithId("ParquetBenchmarks"));
            }
        }

        private const string FilePath = "float_timeseries_large.parquet";
        private const int RowCount = 10_000_000;
        private const int RowGroups = 10;
        private const int RowsPerGroup = RowCount / RowGroups;

        // Buffer sizes for testing (KB to bytes)
        private const int Buffer512KB = 512 * 1024;
        private const int Buffer1MB = 1024 * 1024;
        private const int Buffer4MB = 4 * 1024 * 1024;
        private const int Buffer8MB = 8 * 1024 * 1024;
        private const int Buffer32MB = 32 * 1024 * 1024;

        // Chunk sizes for reading
        private const int ChunkSize10K = 10_000;
        private const int ChunkSize50K = 50_000;
        private const int ChunkSize100K = 100_000;

        private long _fileSizeMB;

        [Params(false, true)]
        public bool UseExistingFile { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            if (!UseExistingFile || !File.Exists(FilePath))
            {
                GenerateLargeFile();
            }
            else
            {
                Console.WriteLine($"Using existing file: {FilePath}");
            }

            _fileSizeMB = new FileInfo(FilePath).Length / (1024 * 1024);
            Console.WriteLine($"File size: {_fileSizeMB} MB, Row groups: {RowGroups}, Rows per group: {RowsPerGroup:N0}");
        }

        private void GenerateLargeFile()
        {
            Console.WriteLine($"Generating test file with {RowCount:N0} rows...");

            var timestamps = new DateTime[RowsPerGroup];
            var objectIds = new int[RowsPerGroup];
            var values = new float[RowsPerGroup];

            var columns = new Column[]
            {
                new Column<DateTime>("Timestamp"),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };

            // Use writer properties for better compression
            var writerProperties = new WriterPropertiesBuilder()
                .Compression(Compression.Snappy)
                .Encoding(Encoding.Plain)
                .Build();

            using var file = new ParquetFileWriter(FilePath, columns, writerProperties);

            for (int rg = 0; rg < RowGroups; rg++)
            {
                var baseTime = DateTime.UtcNow.AddDays(rg);

                for (int i = 0; i < RowsPerGroup; i++)
                {
                    timestamps[i] = baseTime.AddSeconds(i);
                    objectIds[i] = i % 1000;
                    values[i] = (rg * RowsPerGroup + i) * 0.001f;
                }

                using var rowGroup = file.AppendRowGroup();

                rowGroup.NextColumn().LogicalWriter<DateTime>().WriteBatch(timestamps);
                rowGroup.NextColumn().LogicalWriter<int>().WriteBatch(objectIds);
                rowGroup.NextColumn().LogicalWriter<float>().WriteBatch(values);
            }

            file.Close();
            Console.WriteLine("File generation complete");
        }

        #region LogicalColumnReader Benchmarks

        [Benchmark]
        public void LogicalReader_Default()
        {
            using var file = new ParquetFileReader(FilePath);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; ++rg)
            {
                using var rowGroupReader = file.RowGroup(rg);
                var numRows = (int)rowGroupReader.MetaData.NumRows;

                var timestamps = rowGroupReader.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
                var objectIds = rowGroupReader.Column(1).LogicalReader<int>().ReadAll(numRows);
                var values = rowGroupReader.Column(2).LogicalReader<float>().ReadAll(numRows);
            }
        }

        [Benchmark]
        public void LogicalReader_Chunked50K()
        {
            using var file = new ParquetFileReader(FilePath);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; ++rg)
            {
                using var rowGroupReader = file.RowGroup(rg);
                int numRows = (int)rowGroupReader.MetaData.NumRows;

                using (var reader = rowGroupReader.Column(0).LogicalReader<DateTime>())
                {
                    var buffer = new DateTime[ChunkSize50K];
                    while (reader.HasNext)
                        reader.ReadBatch(buffer);
                }

                using (var reader = rowGroupReader.Column(1).LogicalReader<int>())
                {
                    var buffer = new int[ChunkSize50K];
                    while (reader.HasNext)
                        reader.ReadBatch(buffer);
                }

                using (var reader = rowGroupReader.Column(2).LogicalReader<float>())
                {
                    var buffer = new float[ChunkSize50K];
                    while (reader.HasNext)
                        reader.ReadBatch(buffer);
                }
            }
        }

        [Benchmark]
        public void LogicalReader_Chunked10K()
        {
            using var file = new ParquetFileReader(FilePath);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; ++rg)
            {
                using var rowGroupReader = file.RowGroup(rg);
                int numRows = (int)rowGroupReader.MetaData.NumRows;

                using (var reader = rowGroupReader.Column(0).LogicalReader<DateTime>())
                {
                    var buffer = new DateTime[ChunkSize10K];
                    while (reader.HasNext)
                        reader.ReadBatch(buffer);
                }

                using (var reader = rowGroupReader.Column(1).LogicalReader<int>())
                {
                    var buffer = new int[ChunkSize10K];
                    while (reader.HasNext)
                        reader.ReadBatch(buffer);
                }

                using (var reader = rowGroupReader.Column(2).LogicalReader<float>())
                {
                    var buffer = new float[ChunkSize10K];
                    while (reader.HasNext)
                        reader.ReadBatch(buffer);
                }
            }
        }

        [Benchmark]
        public void LogicalReader_Chunked100K()
        {
            using var file = new ParquetFileReader(FilePath);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; ++rg)
            {
                using var rowGroupReader = file.RowGroup(rg);
                int numRows = (int)rowGroupReader.MetaData.NumRows;

                using (var reader = rowGroupReader.Column(0).LogicalReader<DateTime>())
                {
                    var buffer = new DateTime[ChunkSize100K];
                    while (reader.HasNext)
                        reader.ReadBatch(buffer);
                }

                using (var reader = rowGroupReader.Column(1).LogicalReader<int>())
                {
                    var buffer = new int[ChunkSize100K];
                    while (reader.HasNext)
                        reader.ReadBatch(buffer);
                }

                using (var reader = rowGroupReader.Column(2).LogicalReader<float>())
                {
                    var buffer = new float[ChunkSize100K];
                    while (reader.HasNext)
                        reader.ReadBatch(buffer);
                }
            }
        }

        [Benchmark]
        public void LogicalReader_BufferedStream_1MB()
        {
            var readerProperties = ReaderProperties.GetDefaultReaderProperties();
            readerProperties.EnableBufferedStream();
            readerProperties.BufferSize = Buffer1MB;

            using var file = new ParquetFileReader(FilePath, readerProperties);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; ++rg)
            {
                using var rowGroupReader = file.RowGroup(rg);
                var numRows = (int)rowGroupReader.MetaData.NumRows;

                rowGroupReader.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
                rowGroupReader.Column(1).LogicalReader<int>().ReadAll(numRows);
                rowGroupReader.Column(2).LogicalReader<float>().ReadAll(numRows);
            }
        }

        [Benchmark]
        public void LogicalReader_BufferedStream_512KB()
        {
            var readerProperties = ReaderProperties.GetDefaultReaderProperties();
            readerProperties.EnableBufferedStream();
            readerProperties.BufferSize = Buffer512KB;

            using var file = new ParquetFileReader(FilePath, readerProperties);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; ++rg)
            {
                using var rowGroupReader = file.RowGroup(rg);
                var numRows = (int)rowGroupReader.MetaData.NumRows;

                rowGroupReader.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
                rowGroupReader.Column(1).LogicalReader<int>().ReadAll(numRows);
                rowGroupReader.Column(2).LogicalReader<float>().ReadAll(numRows);
            }
        }

        [Benchmark]
        public void LogicalReader_BufferedStream_32MB()
        {
            var readerProperties = ReaderProperties.GetDefaultReaderProperties();
            readerProperties.EnableBufferedStream();
            readerProperties.BufferSize = Buffer32MB;

            using var file = new ParquetFileReader(FilePath, readerProperties);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; ++rg)
            {
                using var rowGroupReader = file.RowGroup(rg);
                var numRows = (int)rowGroupReader.MetaData.NumRows;

                rowGroupReader.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
                rowGroupReader.Column(1).LogicalReader<int>().ReadAll(numRows);
                rowGroupReader.Column(2).LogicalReader<float>().ReadAll(numRows);
            }
        }

        [Benchmark]
        public void LogicalReader_ArrayPool()
        {
            using var file = new ParquetFileReader(FilePath);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; ++rg)
            {
                using var rowGroupReader = file.RowGroup(rg);
                int numRows = (int)rowGroupReader.MetaData.NumRows;

                using (var reader = rowGroupReader.Column(0).LogicalReader<DateTime>())
                {
                    var buffer = ArrayPool<DateTime>.Shared.Rent(ChunkSize50K);
                    try
                    {
                        while (reader.HasNext)
                            reader.ReadBatch(buffer.AsSpan(0, ChunkSize50K));
                    }
                    finally
                    {
                        ArrayPool<DateTime>.Shared.Return(buffer);
                    }
                }

                using (var reader = rowGroupReader.Column(1).LogicalReader<int>())
                {
                    var buffer = ArrayPool<int>.Shared.Rent(ChunkSize50K);
                    try
                    {
                        while (reader.HasNext)
                            reader.ReadBatch(buffer.AsSpan(0, ChunkSize50K));
                    }
                    finally
                    {
                        ArrayPool<int>.Shared.Return(buffer);
                    }
                }

                using (var reader = rowGroupReader.Column(2).LogicalReader<float>())
                {
                    var buffer = ArrayPool<float>.Shared.Rent(ChunkSize50K);
                    try
                    {
                        while (reader.HasNext)
                            reader.ReadBatch(buffer.AsSpan(0, ChunkSize50K));
                    }
                    finally
                    {
                        ArrayPool<float>.Shared.Return(buffer);
                    }
                }
            }
        }

        #endregion

        #region Arrow API Benchmarks

        [Benchmark]
        public async Task Arrow_Default()
        {
            using var fileReader = new FileReader(FilePath);
            using var batchReader = fileReader.GetRecordBatchReader();

            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    for (int col = 0; col < batch.ColumnCount; col++)
                    {
                        var array = batch.Column(col);
                        GC.KeepAlive(array);
                    }
                }
            }
        }

        [Benchmark]
        public async Task Arrow_RowGroupIteration()
        {
            using var parquetReader = new ParquetFileReader(FilePath);
            int numRowGroups = parquetReader.FileMetaData.NumRowGroups;

            using var fileReader = new FileReader(FilePath);

            for (int rg = 0; rg < numRowGroups; rg++)
            {
                using var batchReader = fileReader.GetRecordBatchReader(rowGroups: new[] { rg });

                Apache.Arrow.RecordBatch batch;
                while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
                {
                    using (batch)
                    {
                        for (int col = 0; col < batch.ColumnCount; col++)
                        {
                            var array = batch.Column(col);
                            GC.KeepAlive(array);
                        }
                    }
                }
            }
        }

        [Benchmark]
        public async Task Arrow_PreBufferDisabled()
        {
            using var parquetReader = new ParquetFileReader(FilePath);

            var arrowProperties = ArrowReaderProperties.GetDefault();
            arrowProperties.PreBuffer = false;

            using var fileReader = new FileReader(FilePath, arrowProperties: arrowProperties);

            int numRowGroups = parquetReader.FileMetaData.NumRowGroups;

            for (int rg = 0; rg < numRowGroups; rg++)
            {
                using var batchReader = fileReader.GetRecordBatchReader(rowGroups: new[] { rg });

                Apache.Arrow.RecordBatch batch;
                while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
                {
                    using (batch)
                    {
                        for (int col = 0; col < batch.ColumnCount; col++)
                        {
                            var array = batch.Column(col);
                            GC.KeepAlive(array);
                        }
                    }
                }
            }
        }

        [Benchmark]
        public async Task Arrow_PreBufferDisabled_SmallBatch()
        {
            using var parquetReader = new ParquetFileReader(FilePath);

            var arrowProperties = ArrowReaderProperties.GetDefault();
            arrowProperties.PreBuffer = false;
            arrowProperties.BatchSize = ChunkSize50K;

            using var fileReader = new FileReader(FilePath, arrowProperties: arrowProperties);

            int numRowGroups = parquetReader.FileMetaData.NumRowGroups;

            for (int rg = 0; rg < numRowGroups; rg++)
            {
                using var batchReader = fileReader.GetRecordBatchReader(rowGroups: new[] { rg });

                Apache.Arrow.RecordBatch batch;
                while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
                {
                    using (batch)
                    {
                        for (int col = 0; col < batch.ColumnCount; col++)
                        {
                            var array = batch.Column(col);
                            GC.KeepAlive(array);
                        }
                    }
                }
            }
        }

        [Benchmark]
        public async Task Arrow_PreBufferDisabled_BufferedStream()
        {
            var readerProperties = ReaderProperties.GetDefaultReaderProperties();
            readerProperties.EnableBufferedStream();
            readerProperties.BufferSize = Buffer1MB;

            var arrowProperties = ArrowReaderProperties.GetDefault();
            arrowProperties.PreBuffer = false;

            using var parquetReader = new ParquetFileReader(FilePath, readerProperties);
            using var fileReader = new FileReader(
                FilePath,
                properties: readerProperties,
                arrowProperties: arrowProperties);

            int numRowGroups = parquetReader.FileMetaData.NumRowGroups;

            for (int rg = 0; rg < numRowGroups; rg++)
            {
                using var batchReader = fileReader.GetRecordBatchReader(rowGroups: new[] { rg });

                Apache.Arrow.RecordBatch batch;
                while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
                {
                    using (batch)
                    {
                        for (int col = 0; col < batch.ColumnCount; col++)
                        {
                            GC.KeepAlive(batch.Column(col));
                        }
                    }
                }
            }
        }

        #endregion

        #region Row-Oriented Benchmarks

        [Benchmark]
        public void RowOriented_Default()
        {
            using var file = new ParquetFileReader(FilePath);
            var rowEnumerator = new ParquetRowEnumerator<ParquetRow>(file);

            foreach (var row in rowEnumerator)
            {
                GC.KeepAlive(row);
            }
        }

        [Benchmark]
        public void RowOriented_Chunked()
        {
            const int chunkSize = 10000;
            using var file = new ParquetFileReader(FilePath);
            var rowEnumerator = new ParquetRowEnumerator<ParquetRow>(file);

            var chunk = new List<ParquetRow>(chunkSize);
            foreach (var row in rowEnumerator)
            {
                chunk.Add(row);
                if (chunk.Count >= chunkSize)
                {
                    chunk.Clear();
                }
            }
        }

        #endregion
    }

    // Simple row class for row-oriented access
    public readonly struct ParquetRow
    {
        public ParquetRow(DateTime timestamp, int objectId, float value)
        {
            Timestamp = timestamp;
            ObjectId = objectId;
            Value = value;
        }

        public readonly DateTime Timestamp;
        public readonly int ObjectId;
        public readonly float Value;
    }

    // Simple row enumerator for row-oriented access
    public class ParquetRowEnumerator<T> : IEnumerable<T> where T : struct
    {
        private readonly ParquetFileReader _reader;

        public ParquetRowEnumerator(ParquetFileReader reader)
        {
            _reader = reader;
        }

        public IEnumerator<T> GetEnumerator()
        {
            for (int rg = 0; rg < _reader.FileMetaData.NumRowGroups; rg++)
            {
                using var rowGroup = _reader.RowGroup(rg);
                var numRows = (int)rowGroup.MetaData.NumRows;

                var timestamps = rowGroup.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
                var objectIds = rowGroup.Column(1).LogicalReader<int>().ReadAll(numRows);
                var values = rowGroup.Column(2).LogicalReader<float>().ReadAll(numRows);

                for (int i = 0; i < numRows; i++)
                {
                    yield return (T)(object)new ParquetRow(timestamps[i], objectIds[i], values[i]);
                }
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
