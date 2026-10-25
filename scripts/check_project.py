"""Check Python syntax and local documentation links."""
import ast
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def main():
    python_files = [p for folder in ('src', 'scripts', 'mapreduce', 'tests')
                    for p in (ROOT / folder).rglob('*.py')]
    for path in python_files:
        ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    markdown_files = [ROOT / 'README.md'] + list((ROOT / 'docs').glob('*.md'))
    for path in markdown_files:
        for target in re.findall(r'\[[^\]]+\]\(([^)]+)\)', path.read_text(encoding='utf-8')):
            if '://' in target or target.startswith('#'):
                continue
            target = target.split('#')[0]
            assert (path.parent / target).exists(), f'Broken link: {path}: {target}'
    print(f'OK: {len(python_files)} Python files, local Markdown links')


if __name__ == '__main__':
    main()
