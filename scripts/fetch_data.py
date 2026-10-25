"""Download bank.csv from the UCI Bank Marketing archive."""
import argparse
import hashlib
import io
from pathlib import Path
import urllib.request
import zipfile

URL = 'https://archive.ics.uci.edu/static/public/222/bank+marketing.zip'
MAXIMUM_BYTES = 5 * 1024 * 1024


def _read_member(archive, member, maximum):
    if member.file_size > maximum:
        raise ValueError('Archive member exceeds size limit')
    with archive.open(member) as handle:
        content = handle.read(maximum + 1)
    if len(content) > maximum:
        raise ValueError('Archive member exceeds size limit')
    return content


def extract_bank_csv(payload, maximum=MAXIMUM_BYTES):
    """Read bank.csv from a direct or nested ZIP, with size limits."""
    if len(payload) > maximum:
        raise ValueError('Archive exceeds size limit')
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        direct = [i for i in archive.infolist() if i.filename == 'bank.csv']
        nested = [i for i in archive.infolist() if i.filename == 'bank.zip']
        if len(direct) + len(nested) != 1:
            raise ValueError('Expected one bank.csv member or one bank.zip wrapper')
        if direct:
            return _read_member(archive, direct[0], maximum)
        inner_payload = _read_member(archive, nested[0], maximum)
    with zipfile.ZipFile(io.BytesIO(inner_payload)) as archive:
        candidates = [i for i in archive.infolist() if i.filename == 'bank.csv']
        if len(candidates) != 1:
            raise ValueError('Expected exactly one bank.csv inside bank.zip')
        return _read_member(archive, candidates[0], maximum)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--allow-download', action='store_true')
    args = parser.parse_args()
    if not args.allow_download:
        parser.error('Downloading is opt-in: pass --allow-download on your compute host')
    output = Path(args.output)
    if output.exists():
        parser.error('Output already exists; choose a fresh file')
    request = urllib.request.Request(URL, headers={'User-Agent': 'BankingAnalysis/1.0'})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read(MAXIMUM_BYTES + 1)
    data = extract_bank_csv(payload)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(data)
    print({'source': URL, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
           'dataset': 'UCI Bank Marketing'})


if __name__ == '__main__':
    main()
