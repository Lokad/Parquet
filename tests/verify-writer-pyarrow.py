"""Check the synthetic files exported by ParquetWriterTests with PyArrow 22.0.0."""

import struct
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def verify(path: Path, row_groups: list[int]) -> None:
    file = pq.ParquetFile(path)
    names = ["flag", "Flag", "ratio", "quantity", "label", "day"]
    physical_types = ["BOOLEAN", "BOOLEAN", "FLOAT", "INT64", "BYTE_ARRAY", "INT32"]
    arrow_types = [pa.bool_(), pa.bool_(), pa.float32(), pa.int64(), pa.string(), pa.date32()]
    assert file.schema.names == names
    assert file.schema_arrow == pa.schema(
        [pa.field(name, kind, nullable=False) for name, kind in zip(names, arrow_types)]
    )
    for ordinal, physical_type in enumerate(physical_types):
        column = file.schema.column(ordinal)
        assert column.physical_type == physical_type
        assert column.max_definition_level == 0
        assert column.max_repetition_level == 0
    assert file.schema.column(4).converted_type == "UTF8"
    assert file.schema.column(4).logical_type.type == "STRING"
    assert file.schema.column(5).converted_type == "DATE"
    assert file.schema.column(5).logical_type.type == "DATE"

    assert file.num_row_groups == len(row_groups)
    assert file.metadata.num_rows == sum(row_groups)
    for ordinal, rows in enumerate(row_groups):
        group = file.metadata.row_group(ordinal)
        assert group.num_rows == rows
        for index in range(group.num_columns):
            column = group.column(index)
            assert column.compression == "SNAPPY"
            assert "PLAIN" in column.encodings
            assert column.num_values == rows
    table = file.read()
    rows = range(sum(row_groups))
    assert table.column("flag").to_pylist() == [i % 3 == 0 for i in rows]
    assert table.column("Flag").to_pylist() == [i % 2 == 0 for i in rows]
    assert table.column("quantity").to_pylist() == [
        (-(1 << 63), (1 << 63) - 1, -17 * i)[i % 3] for i in rows
    ]
    labels = ["", "café", "مرحبا世界", "über😀", "a value larger than the page target"]
    assert table.column("label").to_pylist() == [labels[i % len(labels)] for i in rows]
    assert table.column("day").cast(pa.int32()).to_pylist() == [i % 7 - 3 for i in rows]
    expected_floats = [
        (0, 0x80000000, 0x7FC01234 + i, struct.unpack("<I", struct.pack("<f", i + 0.25))[0])[i % 4]
        for i in rows
    ]
    floats = table.column("ratio").combine_chunks()
    assert list(struct.unpack(f"<{len(floats)}I", floats.buffers()[1].to_pybytes())) == expected_floats


if __name__ == "__main__":
    assert pa.__version__ == "22.0.0", pa.__version__
    directory = Path(sys.argv[1])
    verify(directory / "mixed.parquet", [11, 12])
    verify(directory / "empty.parquet", [])
    print(f"PyArrow {pa.__version__} verified the mixed and schema-only files.")
