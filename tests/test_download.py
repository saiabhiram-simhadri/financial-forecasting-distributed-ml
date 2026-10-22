"""No-network regression tests for UCI archive layouts and size limits."""
import io
from pathlib import Path
import sys
import unittest
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from fetch_data import extract_bank_csv


def archive_bytes(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in members:
            archive.writestr(name, data)
    return buffer.getvalue()


class ArchiveTests(unittest.TestCase):
    def test_direct_and_nested_archive_select_only_bank_csv(self):
        data = b'"age";"job"\n30;"admin."\n'
        inner = archive_bytes([('bank.csv', data), ('bank-full.csv', b'not selected')])
        self.assertEqual(extract_bank_csv(inner), data)
        outer = archive_bytes([('bank.zip', inner), ('bank-additional.zip', b'not selected')])
        self.assertEqual(extract_bank_csv(outer), data)

    def test_missing_or_ambiguous_layout_is_rejected(self):
        for members in [
            [('bank-additional.csv', b'wrong schema')],
            [('bank.csv', b'data'), ('bank.zip', b'other layout')],
            [('bank.zip', archive_bytes([('bank-full.csv', b'wrong file')]))],
        ]:
            with self.assertRaises(ValueError):
                extract_bank_csv(archive_bytes(members))

    def test_compressed_and_decompressed_limits_are_checked(self):
        compressed = archive_bytes([('bank.csv', b'x' * 5000)])
        self.assertLess(len(compressed), 1000)
        with self.assertRaises(ValueError):
            extract_bank_csv(compressed, maximum=1000)
        with self.assertRaises(ValueError):
            extract_bank_csv(compressed, maximum=10)
        inner = archive_bytes([('bank.csv', b'x' * 5000)])
        nested = archive_bytes([('bank.zip', inner)])
        with self.assertRaises(ValueError):
            extract_bank_csv(nested, maximum=1000)


if __name__ == '__main__':
    unittest.main()
