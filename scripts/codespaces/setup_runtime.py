"""Install checksummed archives inside a Codespace, never on a local workstation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request

MAX_DOWNLOAD = 1200 * 1024 * 1024
APACHE = [
    ('hadoop', '3.3.6',
     'https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz', 'sha512'),
    ('hive', '4.0.1',
     'https://archive.apache.org/dist/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz', 'sha256'),
]
# The Java major versions are fixed; Adoptium selects current GA patch releases.
# Exact release/URL/checksum metadata is recorded beside each installed runtime.
JAVA_MAJORS = (8, 17)


def read_small(url):
    request = urllib.request.Request(url, headers={'User-Agent': 'BankingCodespacesSetup/1.0'})
    with urllib.request.urlopen(request, timeout=90) as response:
        data = response.read(2 * 1024 * 1024 + 1)
    if len(data) > 2 * 1024 * 1024:
        raise ValueError('Metadata response too large')
    return data


def checksum_from_text(text, algorithm):
    length = hashlib.new(algorithm).digest_size * 2
    candidates = re.findall(r'(?i)\b[0-9a-f]{' + str(length) + r'}\b', text)
    if len(candidates) != 1:
        raise ValueError(f'Expected exactly one {algorithm} checksum')
    return candidates[0].lower()


def download(url, path, expected, algorithm):
    partial = path.with_suffix(path.suffix + '.partial')
    # A failed transfer is retried from the start, never trusted as an installation.
    for attempt in range(3):
        digest = hashlib.new(algorithm)
        size = 0
        try:
            request = urllib.request.Request(url, headers={'User-Agent': 'BankingCodespacesSetup/1.0'})
            with urllib.request.urlopen(request, timeout=120) as response, partial.open('wb') as output:
                while True:
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    size += len(block)
                    if size > MAX_DOWNLOAD:
                        raise ValueError('Download exceeds 1200 MiB bound')
                    digest.update(block)
                    output.write(block)
            if digest.hexdigest() != expected:
                raise ValueError('Checksum mismatch')
            partial.replace(path)
            return size
        except (OSError, ValueError):
            if attempt == 2:
                raise
            time.sleep(2 * (attempt + 1))
    raise AssertionError('Unreachable')


def unpack(archive_path, destination):
    if destination.exists():
        raise ValueError(f'Incomplete/existing destination: {destination}; preserve it before retrying')
    parent = destination.parent
    # Cleanup only our own temporary extraction directory, not existing runtimes.
    with tempfile.TemporaryDirectory(prefix='extract-', dir=parent) as temporary:
        temporary = Path(temporary)
        with tarfile.open(archive_path, 'r:gz') as archive:
            if not hasattr(tarfile, 'data_filter'):
                raise RuntimeError('Python needs tarfile.data_filter; use the supplied dev container')
            archive.extractall(temporary, filter='data')
        roots = list(temporary.iterdir())
        if len(roots) != 1 or not roots[0].is_dir():
            raise ValueError('Expected one archive root directory')
        roots[0].rename(destination)


def install(state, name, version, url, checksum, algorithm):
    destination = state / 'tools' / name
    marker = destination / '.bank-install.json'
    if marker.is_file():
        previous = json.loads(marker.read_text())
        if previous['version'] != version:
            raise ValueError(f'{name} version changed; use a new state directory')
        print(f'Reuse {name}: {destination}', flush=True)
        return
    if destination.exists():
        raise ValueError(f'{destination} exists without an install marker; preserve/rename it before retrying')
    archive = state / 'downloads' / f'{name}.tar.gz'
    print(f'Download {name} {version}: {url}', flush=True)
    size = download(url, archive, checksum, algorithm)
    unpack(archive, destination)
    marker.write_text(json.dumps({'name': name, 'version': version, 'url': url,
                                 'checksum': checksum, 'algorithm': algorithm, 'download_bytes': size},
                                indent=2), encoding='utf-8')
    archive.unlink()  # Delete only this verified installer archive after extraction.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    state = Path(args.state).resolve()
    if (os.environ.get('CODESPACES') != 'true' or platform.system() != 'Linux'
            or platform.machine() != 'x86_64' or not str(repo).startswith('/workspaces/')):
        parser.error('Installation is restricted to an x86_64 GitHub Codespace')
    if state != repo / '.codespaces-state':
        parser.error('State must be this repository\'s .codespaces-state directory')
    state.mkdir(exist_ok=True)
    if shutil.disk_usage(state).free < 10 * 1024 ** 3:
        parser.error('Need 10 GiB free for installation/extraction. Inspect disk use first.')
    for folder in ('downloads', 'tools', 'logs', 'tmp'):
        (state / folder).mkdir(exist_ok=True)
    for major in JAVA_MAJORS:
        name = f'java{major}'
        marker = state / 'tools' / name / '.bank-install.json'
        if marker.is_file():
            print(f'Reuse Java {major}', flush=True)
            continue
        url = (f'https://api.adoptium.net/v3/assets/latest/{major}/hotspot'
               '?architecture=x64&heap_size=normal&image_type=jdk&os=linux&vendor=eclipse')
        assets = json.loads(read_small(url))
        if not assets:
            raise ValueError(f'No Adoptium Java {major} asset returned')
        asset = assets[0]
        package = asset['binary']['package']
        install(state, name, asset['release_name'], package['link'], package['checksum'], 'sha256')
    for name, version, url, algorithm in APACHE:
        marker = state / 'tools' / name / '.bank-install.json'
        if marker.is_file():
            previous = json.loads(marker.read_text())
            if previous['version'] != version:
                raise ValueError(f'Unexpected installed {name} version')
            print(f'Reuse {name} {version}', flush=True)
            continue
        checksum = checksum_from_text(read_small(url + '.' + algorithm).decode(), algorithm)
        install(state, name, version, url, checksum, algorithm)
    print('Verified Java/Hadoop/Hive archives installed. No daemon was started.', flush=True)


if __name__ == '__main__':
    main()
