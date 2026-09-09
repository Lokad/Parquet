using Parquet;
using Parquet.Schema;
using BaselineParquetSchema = Parquet.Schema.ParquetSchema;
using BaselineParquetWriter = Parquet.ParquetWriter;

namespace Lokad.Parquet.Benchmarks;

// Bench-side truth qualification (PLAN B01). The checksum scheme must bind column
// identity, column order, and nullness, and the checks below invoke the actual Core,
// Parity, and Census consumers against crafted fixtures: the shared scheme is pinned
// directly, then each pipeline must agree with an independent oracle built from known
// values while adversarial orderings fail loudly.
public static class ScanTruthVerification
{
    private const int RetiredNullMarker = unchecked((int)0xA5A5A5A5);

    // Uneven multi-row-group fixture with a fully known oracle: values are a
    // deterministic function of global row and column, so every consumer output
    // is pinned without trusting the writers under test. Shared by truth
    // verification and the work census uneven case.
    internal static async Task<(byte[] Bytes, long[] Folded, long Expected)> WriteUnevenTwoColumnFixtureAsync(int firstGroupRows, int secondGroupRows)
    {
        var first = new[] { Series(0, firstGroupRows, 0), Series(0, firstGroupRows, 1) };
        var second = new[] { Series(firstGroupRows, secondGroupRows, 0), Series(firstGroupRows, secondGroupRows, 1) };
        var fields = new[] { new DataField<int>("value-0", nullable: false), new DataField<int>("value-1", nullable: false) };
        using var stream = new MemoryStream();
        var options = new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 0 };
        await using (var writer = await BaselineParquetWriter.CreateAsync(new BaselineParquetSchema(fields), stream, options))
        {
            using (var firstGroup = writer.CreateRowGroup())
            {
                await firstGroup.WriteAsync<int>(fields[0], first[0]);
                await firstGroup.WriteAsync<int>(fields[1], first[1]);
                firstGroup.CompleteValidate();
            }
            using (var secondGroup = writer.CreateRowGroup())
            {
                await secondGroup.WriteAsync<int>(fields[0], second[0]);
                await secondGroup.WriteAsync<int>(fields[1], second[1]);
                secondGroup.CompleteValidate();
            }
        }
        var folded = new long[2];
        for (var column = 0; column < folded.Length; column++)
        {
            var chain = ScanChecksum.Seed;
            foreach (var value in first[column])
                chain = ScanChecksum.Mix(chain, value);
            foreach (var value in second[column])
                chain = ScanChecksum.Mix(chain, value);
            folded[column] = ScanChecksum.CombineColumn(chain, ScanChecksum.Seed);
        }
        return (stream.ToArray(), folded, ScanChecksum.CombineColumns(folded));

        static int[] Series(int startRow, int count, int column)
        {
            var values = new int[count];
            for (var row = 0; row < values.Length; row++)
                values[row] = checked((startRow + row) * 3 + column * 1000 + 1);
            return values;
        }
    }

    public static async Task<int> RunAsync()
    {
        var failures = new List<string>();
        CheckScheme(failures);
        CheckStudentTQuantile(failures);
        await CheckCoreConsumersAsync(failures);
        await CheckParityConsumersAsync(failures);
        await CheckCensusConsumersAsync(failures);
        foreach (var failure in failures)
            Console.WriteLine("truth-verify FAIL: " + failure);
        if (failures.Count == 0)
            Console.WriteLine("truth-verify: all scheme and consumer checks passed.");
        return failures.Count == 0 ? 0 : 1;

        static void CheckScheme(List<string> failures)
        {
            // Reproduced B01 failure: swapped per-column hashes combined equally because
            // XOR(Mix(hash, ordinal)) separates into XORs of hashes and ordinals.
            var ordered = ScanChecksum.CombineColumns([123, 456]);
            var swapped = ScanChecksum.CombineColumns([456, 123]);
            if (ordered == swapped)
                failures.Add("CombineColumns ignores column order: [123,456] and [456,123] both combine to " + ordered + ".");
            var duplicated = ScanChecksum.CombineColumns([123, 123]);
            if (duplicated == ordered)
                failures.Add("CombineColumns confuses duplicated columns with distinct columns.");
            var truncated = ScanChecksum.CombineColumns([123]);
            if (ordered == truncated)
                failures.Add("CombineColumns confuses omitted columns with the full projection.");
            // A null must never equal a value, including the retired marker: column one
            // holds [7, marker] with no nulls, column two holds [7] with a null at row 1.
            var markerColumn = ScanChecksum.CombineColumn(ScanChecksum.Mix(ScanChecksum.Mix(ScanChecksum.Seed, 7), RetiredNullMarker), ScanChecksum.Seed);
            var nullColumn = ScanChecksum.CombineColumn(ScanChecksum.Mix(ScanChecksum.Seed, 7), ScanChecksum.Mix(ScanChecksum.Seed, 1));
            if (markerColumn == nullColumn)
                failures.Add("CombineColumn confuses a null with the retired null marker.");
        }

        // Pins the report interval quantile against published Student-t table values:
        // exact textbook spots at df 1..30, expansion range at df 31..399, the normal
        // limit from above, and rejection of non-positive degrees of freedom.
        static void CheckStudentTQuantile(List<string> failures)
        {
            RequireNear(failures, "t(1)", PairedParityRunner.OneSided95StudentT(1), 6.31375, 0.001);
            RequireNear(failures, "t(2)", PairedParityRunner.OneSided95StudentT(2), 2.91999, 0.001);
            RequireNear(failures, "t(5)", PairedParityRunner.OneSided95StudentT(5), 2.01505, 0.001);
            RequireNear(failures, "t(10)", PairedParityRunner.OneSided95StudentT(10), 1.81246, 0.001);
            RequireNear(failures, "t(30)", PairedParityRunner.OneSided95StudentT(30), 1.69726, 0.001);
            RequireNear(failures, "t(31)", PairedParityRunner.OneSided95StudentT(31), 1.6955, 0.001);
            RequireNear(failures, "t(60)", PairedParityRunner.OneSided95StudentT(60), 1.6706, 0.001);
            RequireNear(failures, "t(100)", PairedParityRunner.OneSided95StudentT(100), 1.6602, 0.001);
            RequireNear(failures, "t(200)", PairedParityRunner.OneSided95StudentT(200), 1.6525, 0.001);
            RequireNear(failures, "t(399)", PairedParityRunner.OneSided95StudentT(399), 1.6487, 0.001);
            var limit = PairedParityRunner.OneSided95StudentT(100000);
            if (limit <= 1.644854 || limit >= 1.6455)
                failures.Add("t(df) does not approach the normal 1.644854 limit from above: " + limit + ".");
            try
            {
                PairedParityRunner.OneSided95StudentT(0);
                failures.Add("t(0) did not reject non-positive degrees of freedom.");
            }
            catch (ArgumentOutOfRangeException)
            {
            }
        }

        static void RequireNear(List<string> failures, string name, double actual, double expected, double tolerance)
        {
            if (Math.Abs(actual - expected) > tolerance)
                failures.Add(name + ": got " + actual + ", expected " + expected + " within " + tolerance + ".");
        }

        static void RequireEqual(List<string> failures, string name, long actual, long expected)
        {
            if (actual != expected)
                failures.Add(name + ": got " + actual + ", expected " + expected + ".");
        }

        static void RequireDifferent(List<string> failures, string name, long actual, long unexpected)
        {
            if (actual == unexpected)
                failures.Add(name + ": unexpectedly equals " + unexpected + "; ordering or identity is not detected.");
        }

        static async Task CheckCoreConsumersAsync(List<string> failures)
        {
            try
            {
                var fixture = await ScanFixture.CreateAsync(ScanWorkload.TwoRequiredInt32Plain, 64);
                await fixture.AssertLokadScanAsync();
                var values = new long[2];
                var nulls = new long[2];
                var full = await CoreScanBenchmarks.ReadLokadAsync(fixture.Bytes, null, null, null, 64, 0, values, nulls);
                RequireEqual(failures, "core full projection", full, fixture.Checksum);
                var fullBaseline = await CoreScanBenchmarks.ReadParquetNetAsync(fixture.Bytes, ScanWorkload.TwoRequiredInt32Plain, null, null, 64, 0, values, nulls);
                RequireEqual(failures, "core baseline full projection", fullBaseline, fixture.Checksum);
                var tiny = await CoreScanBenchmarks.ReadLokadAsync(fixture.Bytes, null, 1, null, 64, 0, values, nulls);
                RequireEqual(failures, "core tiny target", tiny, fixture.Checksum);
                var left = fixture.ColumnChecksums[0];
                var right = fixture.ColumnChecksums[1];
                var swapped = await CoreScanBenchmarks.ReadLokadAsync(fixture.Bytes, [1, 0], null, null, 64, 0, values, nulls);
                RequireEqual(failures, "core swapped projection", swapped, ScanChecksum.CombineColumns([right, left]));
                RequireDifferent(failures, "core swapped projection", swapped, fixture.Checksum);
                var swappedBaseline = await CoreScanBenchmarks.ReadParquetNetAsync(fixture.Bytes, ScanWorkload.TwoRequiredInt32Plain, [1, 0], null, 64, 0, values, nulls);
                RequireEqual(failures, "core baseline swapped projection", swappedBaseline, swapped);
                var duplicatedBaseline = await CoreScanBenchmarks.ReadParquetNetAsync(fixture.Bytes, ScanWorkload.TwoRequiredInt32Plain, [0, 0], null, 64, 0, values, nulls);
                RequireEqual(failures, "core baseline duplicated projection", duplicatedBaseline, ScanChecksum.CombineColumns([left, left]));
                RequireDifferent(failures, "core baseline duplicated projection", duplicatedBaseline, fixture.Checksum);
                // The Lokad reader rejects duplicate projected columns at validation, so
                // duplication sensitivity on that path rests on the scheme check above.
                var omitted = await CoreScanBenchmarks.ReadLokadAsync(fixture.Bytes, [1], null, null, 64, 0, values, nulls);
                RequireEqual(failures, "core omitted projection", omitted, right);
                RequireDifferent(failures, "core omitted projection", omitted, fixture.Checksum);
                var (markerBytes, markerValues, markerExpected, markerConflated, markerNulls) = await WriteNullableMarkerFixtureAsync();
                if (markerNulls != 2)
                    failures.Add("core marker oracle has the wrong null count.");
                var markerLokad = await CoreScanBenchmarks.ReadLokadAsync(markerBytes, null, null, null, markerValues.Length, 0, new long[1], new long[1]);
                RequireEqual(failures, "core marker values", markerLokad, markerExpected);
                RequireDifferent(failures, "core marker values", markerLokad, markerConflated);
                var markerBaseline = await CoreScanBenchmarks.ReadParquetNetAsync(markerBytes, ScanWorkload.NullableInt32Plain, null, null, markerValues.Length, 0, new long[1], new long[1]);
                RequireEqual(failures, "core baseline marker values", markerBaseline, markerExpected);
                var (unevenBytes, unevenFolded, unevenExpected) = await WriteUnevenTwoColumnFixtureAsync(5, 9);
                var unevenFull = await CoreScanBenchmarks.ReadLokadAsync(unevenBytes, null, null, null, 14, 0, new long[2], new long[2]);
                RequireEqual(failures, "core uneven row groups", unevenFull, unevenExpected);
                var unevenTiny = await CoreScanBenchmarks.ReadLokadAsync(unevenBytes, null, 1, null, 14, 0, new long[2], new long[2]);
                RequireEqual(failures, "core uneven row groups at target one", unevenTiny, unevenExpected);
                var unevenBaseline = await CoreScanBenchmarks.ReadParquetNetAsync(unevenBytes, ScanWorkload.TwoRequiredInt32Plain, null, null, 14, 0, new long[2], new long[2]);
                RequireEqual(failures, "core baseline uneven row groups", unevenBaseline, unevenExpected);
            }
            catch (Exception exception)
            {
                failures.Add("core consumers: " + exception.Message);
            }
        }

        static async Task CheckParityConsumersAsync(List<string> failures)
        {
            try
            {
                await using var kase = await ParityScanCase.CreateAsync(ScanWorkload.TwoRequiredInt32Plain, 64);
                var fixture = kase.Fixture;
                var columns = kase.SchemaColumns;
                var full = await kase.ReadLokadAsync(new ParquetScanOptions(columns));
                RequireEqual(failures, "parity full projection", full, fixture.Checksum);
                var tiny = await kase.ReadLokadAsync(new ParquetScanOptions(columns, null, null, 1));
                RequireEqual(failures, "parity tiny target", tiny, fixture.Checksum);
                var left = fixture.ColumnChecksums[0];
                var right = fixture.ColumnChecksums[1];
                var swapped = await kase.ReadLokadAsync(new ParquetScanOptions([columns[1], columns[0]]));
                RequireEqual(failures, "parity swapped projection", swapped, ScanChecksum.CombineColumns([right, left]));
                RequireDifferent(failures, "parity swapped projection", swapped, fixture.Checksum);
                var duplicatedBaseline = await kase.ReadParquetNetAsync([0, 0]);
                RequireEqual(failures, "parity baseline duplicated projection", duplicatedBaseline, ScanChecksum.CombineColumns([left, left]));
                RequireDifferent(failures, "parity baseline duplicated projection", duplicatedBaseline, fixture.Checksum);
                var omitted = await kase.ReadLokadAsync(new ParquetScanOptions([columns[1]]));
                RequireEqual(failures, "parity omitted projection", omitted, right);
                RequireDifferent(failures, "parity omitted projection", omitted, fixture.Checksum);
                var fullBaseline = await kase.ReadParquetNetAsync();
                RequireEqual(failures, "parity baseline full projection", fullBaseline, fixture.Checksum);
                var swappedBaseline = await kase.ReadParquetNetAsync([1, 0]);
                RequireEqual(failures, "parity baseline swapped projection", swappedBaseline, swapped);
            }
            catch (Exception exception)
            {
                failures.Add("parity consumers: " + exception.Message);
            }
        }

        static async Task CheckCensusConsumersAsync(List<string> failures)
        {
            try
            {
                var fixture = await ScanFixture.CreateAsync(ScanWorkload.TwoRequiredInt32Plain, 128);
                await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)fixture.Bytes);
                var columns = file.Metadata.Schema.Columns;
                await WorkCensusRunner.RunCensusPassAsync(file, fixture.Workload, 128, 0, columns, 128, fixture.Checksum, fixture.ColumnChecksums, fixture.NullCounts, null);
                var secondHalf = columns.Skip(1).ToArray();
                await WorkCensusRunner.RunCensusPassAsync(file, fixture.Workload, 128, 0, secondHalf, 4, WorkCensusRunner.CensusExpectedChecksum(fixture.Checksum, fixture.ColumnChecksums, columns.Count, secondHalf), fixture.ColumnChecksums, fixture.NullCounts, null);
                try
                {
                    await WorkCensusRunner.RunCensusPassAsync(file, fixture.Workload, 128, 0, [columns[1], columns[0]], 128, fixture.Checksum, fixture.ColumnChecksums, fixture.NullCounts, null);
                    failures.Add("census swapped projection did not fail the truth check.");
                }
                catch (InvalidOperationException exception) when (exception.Message.Contains("truth check", StringComparison.Ordinal))
                {
                }
                var (markerBytes, markerValues, markerExpected, markerConflated, markerNulls) = await WriteNullableMarkerFixtureAsync();
                await using var markerFile = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)markerBytes);
                var markerColumns = markerFile.Metadata.Schema.Columns;
                await WorkCensusRunner.RunCensusPassAsync(markerFile, ScanWorkload.NullableInt32Plain, markerValues.Length, 0, markerColumns, markerValues.Length, markerExpected, [markerExpected], [markerNulls], null);
                var (unevenBytes, unevenFolded, unevenExpected) = await WriteUnevenTwoColumnFixtureAsync(5, 9);
                await using var unevenFile = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)unevenBytes);
                var unevenColumns = unevenFile.Metadata.Schema.Columns;
                await WorkCensusRunner.RunCensusPassAsync(unevenFile, ScanWorkload.TwoRequiredInt32Plain, 14, 0, unevenColumns, 14, unevenExpected, unevenFolded, [0, 0], null);
            }
            catch (Exception exception)
            {
                failures.Add("census consumers: " + exception.Message);
            }
        }
        static async Task<(byte[] Bytes, int?[] Values, long Expected, long Conflated, int Nulls)> WriteNullableMarkerFixtureAsync()
        {
            int?[] values = [null, RetiredNullMarker, 3, 4, 5, 6, 7, null, 9, RetiredNullMarker, 11, 12, 13, 14, 15, 16];
            var field = new DataField<int?>("value");
            using var stream = new MemoryStream();
            var options = new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 0 };
            await using (var writer = await BaselineParquetWriter.CreateAsync(new BaselineParquetSchema(field), stream, options))
            {
                using var rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteAsync<int>(field, values);
                rowGroup.CompleteValidate();
            }
            var valueChain = ScanChecksum.Seed;
            var nullChain = ScanChecksum.Seed;
            var plainChain = ScanChecksum.Seed;
            var plainNulls = ScanChecksum.Seed;
            var nulls = 0;
            for (var row = 0; row < values.Length; row++)
            {
                if (values[row] is int value)
                {
                    valueChain = ScanChecksum.Mix(valueChain, value);
                    if (value == RetiredNullMarker)
                        plainNulls = ScanChecksum.Mix(plainNulls, row);
                    else
                        plainChain = ScanChecksum.Mix(plainChain, value);
                }
                else
                {
                    nullChain = ScanChecksum.Mix(nullChain, row);
                    plainNulls = ScanChecksum.Mix(plainNulls, row);
                    nulls++;
                }
            }
            return (stream.ToArray(), values, ScanChecksum.CombineColumn(valueChain, nullChain), ScanChecksum.CombineColumn(plainChain, plainNulls), nulls);
        }


    }
}
