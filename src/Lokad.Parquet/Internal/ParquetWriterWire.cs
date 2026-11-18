using System.Buffers.Binary;

namespace Lokad.Parquet.Internal;

internal readonly record struct WriterColumn(string Name, byte[] Utf8Name, ParquetPhysicalType Type);

internal readonly record struct WrittenColumnChunk(long Offset, long UncompressedBytes, long CompressedBytes);

internal readonly record struct WrittenRowGroup(int Rows, WrittenColumnChunk[] Chunks);

internal static class ParquetWriterWire
{
    public static int WritePageHeader(Span<byte> buffer, int values, int uncompressedBytes, int compressedBytes)
    {
        var writer = new ThriftCompactWriter(buffer);
        var field = 0;
        writer.Int32Field(ref field, 1, 0); // DATA_PAGE
        writer.Int32Field(ref field, 2, uncompressedBytes);
        writer.Int32Field(ref field, 3, compressedBytes);
        writer.StructField(ref field, 5); // data_page_header
        field = 0;
        writer.Int32Field(ref field, 1, values);
        writer.Int32Field(ref field, 2, (int)ParquetEncoding.Plain);
        writer.Int32Field(ref field, 3, (int)ParquetEncoding.RunLength);
        writer.Int32Field(ref field, 4, (int)ParquetEncoding.RunLength);
        writer.Stop();
        writer.Stop();
        return writer.Length;
    }

    public static int WriteFooter(
        Span<byte> buffer,
        WriterColumn[] columns,
        List<WrittenRowGroup> groups,
        long rows,
        CancellationToken cancellationToken)
    {
        var writer = new ThriftCompactWriter(buffer);
        var field = 0;
        writer.Int32Field(ref field, 1, 1); // file metadata version
        writer.ListField(ref field, 2, CompactType.Struct, columns.Length + 1);
        WriteSchema(ref writer, columns);
        writer.Int64Field(ref field, 3, rows);
        writer.ListField(ref field, 4, CompactType.Struct, groups.Count);
        foreach (var group in groups)
        {
            cancellationToken.ThrowIfCancellationRequested();
            WriteRowGroup(ref writer, columns, group);
        }
        writer.Stop();
        BinaryPrimitives.WriteInt32LittleEndian(buffer[writer.Length..], writer.Length);
        "PAR1"u8.CopyTo(buffer[(writer.Length + sizeof(int))..]);
        return writer.Length + 8;

        static void WriteSchema(ref ThriftCompactWriter writer, WriterColumn[] columns)
        {
            var field = 0;
            writer.BinaryField(ref field, 4, "schema"u8);
            writer.Int32Field(ref field, 5, columns.Length);
            writer.Stop();

            foreach (var column in columns)
            {
                field = 0;
                writer.Int32Field(ref field, 1, (int)column.Type);
                writer.Int32Field(ref field, 3, (int)ParquetRepetition.Required);
                writer.BinaryField(ref field, 4, column.Utf8Name);
                if (column.Type is ParquetPhysicalType.ByteArray or ParquetPhysicalType.Int32)
                {
                    var isString = column.Type == ParquetPhysicalType.ByteArray;
                    writer.Int32Field(ref field, 6, (int)(isString ? ParquetConvertedType.Utf8 : ParquetConvertedType.Date));
                    writer.StructField(ref field, 10); // logicalType
                    field = 0;
                    writer.StructField(ref field, isString ? 1 : 6);
                    writer.Stop(); // StringType or DateType
                    writer.Stop(); // LogicalType union
                }
                writer.Stop();
            }
        }

        static void WriteRowGroup(ref ThriftCompactWriter writer, WriterColumn[] columns, WrittenRowGroup group)
        {
            var field = 0;
            writer.ListField(ref field, 1, CompactType.Struct, group.Chunks.Length);
            long uncompressed = 0;
            long compressed = 0;
            for (var i = 0; i < group.Chunks.Length; i++)
            {
                var chunk = group.Chunks[i];
                WriteChunk(ref writer, columns[i], chunk, group.Rows);
                uncompressed = checked(uncompressed + chunk.UncompressedBytes);
                compressed = checked(compressed + chunk.CompressedBytes);
            }
            writer.Int64Field(ref field, 2, uncompressed);
            writer.Int64Field(ref field, 3, group.Rows);
            writer.Int64Field(ref field, 5, group.Chunks[0].Offset);
            writer.Int64Field(ref field, 6, compressed);
            writer.Stop();
        }

        static void WriteChunk(
            ref ThriftCompactWriter writer, WriterColumn column, WrittenColumnChunk chunk, int rows)
        {
            var field = 0;
            writer.Int64Field(ref field, 2, 0); // deprecated column metadata offset
            writer.StructField(ref field, 3); // meta_data
            field = 0;
            writer.Int32Field(ref field, 1, (int)column.Type);
            writer.ListField(ref field, 2, CompactType.Int32, 2);
            writer.Int32((int)ParquetEncoding.Plain);
            writer.Int32((int)ParquetEncoding.RunLength);
            writer.ListField(ref field, 3, CompactType.Binary, 1);
            writer.Binary(column.Utf8Name);
            writer.Int32Field(ref field, 4, (int)ParquetCompressionCodec.Snappy);
            writer.Int64Field(ref field, 5, rows);
            writer.Int64Field(ref field, 6, chunk.UncompressedBytes);
            writer.Int64Field(ref field, 7, chunk.CompressedBytes);
            writer.Int64Field(ref field, 9, chunk.Offset);
            writer.Stop(); // ColumnMetaData
            writer.Stop(); // ColumnChunk
        }
    }
}
