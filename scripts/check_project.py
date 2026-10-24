"""Dependency-free structural checks: Python, DOCX XML, local links."""
import ast
import json
from pathlib import Path
import re
import zipfile
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]


def main():
    python_files = [p for folder in ('src', 'scripts', 'mapreduce', 'tests')
                    for p in (ROOT / folder).rglob('*.py')]
    for path in python_files:
        ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    copies = list((ROOT / 'submissions').glob('*.docx'))
    reports = json.loads((ROOT / 'docs' / 'requirements.json').read_text(encoding='utf-8'))
    assert len(copies) == len(reports) == 1
    for report in reports:
        path = ROOT / 'submissions' / (Path(report['file']).stem + ' - Implementation Guide.docx')
        with zipfile.ZipFile(path) as archive:
            assert archive.testzip() is None
            xml = archive.read('word/document.xml')
            ElementTree.fromstring(xml)
            text = xml.decode('utf-8')
            assert text.count('Implementation ') == len(report['items'])
            assert text.count('EVIDENCE PENDING:') == len(report['items'])
    markdown_files = [ROOT / 'README.md'] + list((ROOT / 'docs').glob('*.md'))
    for path in markdown_files:
        for target in re.findall(r'\[[^\]]+\]\(([^)]+)\)', path.read_text(encoding='utf-8')):
            if '://' in target or target.startswith('#'):
                continue
            target = target.split('#')[0]
            assert (path.parent / target).exists(), f'Broken link: {path}: {target}'
    print(f'OK: {len(python_files)} Python files, '
          f'{len(copies)} DOCX copies, {sum(len(r["items"]) for r in reports)} mapped prompts, local Markdown links')


if __name__ == '__main__':
    main()
