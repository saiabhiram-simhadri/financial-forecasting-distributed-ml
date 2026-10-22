"""Generate annotated DOCX copies and report guides.

Standard library only. Original DOCX files are never changed. No result is invented.
"""
import html
import json
from pathlib import Path
import re
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PENDING = ('EVIDENCE PENDING: Execute on the documented environment. Insert screenshots '
           'of this question\'s code and actual output, then add observed values and interpretation. '
           'No execution result is claimed by this template.')


def paragraph(text):
    return '<w:p><w:r><w:t xml:space="preserve">' + html.escape(text) + '</w:t></w:r></w:p>'


def main():
    reports = json.loads((ROOT / 'docs' / 'requirements.json').read_text(encoding='utf-8'))
    submissions = ROOT / 'submissions'
    submissions.mkdir(exist_ok=True)
    for report in reports:
        source = ROOT / report['file']
        destination = submissions / (source.stem + ' - Implementation Guide.docx')
        with zipfile.ZipFile(source) as archive:
            document = archive.read('word/document.xml').decode('utf-8')
            found = set()

            def annotate(match):
                block = match.group()
                text = ''.join(html.unescape(t) for t in re.findall(r'<w:t(?:\s[^>]*)?>(.*?)</w:t>', block, re.DOTALL)).strip()
                for index, (prefix, answer) in enumerate(report['items'], 1):
                    if text.startswith(prefix):
                        found.add(index)
                        return block + paragraph(f'Implementation {index:02d}: {answer}') + paragraph(PENDING)
                return block

            updated = re.sub(r'<w:p(?:\s[^>]*)?>.*?</w:p>', annotate, document, flags=re.DOTALL)
            if len(found) != len(report['items']):
                missing = [q for i, (q, _) in enumerate(report['items'], 1) if i not in found]
                raise ValueError(f'Unmatched questions in {source.name}: {missing}')
            with zipfile.ZipFile(destination, 'w', compression=zipfile.ZIP_DEFLATED) as output:
                for item in archive.infolist():
                    output.writestr(item, updated.encode('utf-8') if item.filename == 'word/document.xml' else archive.read(item.filename))
        guide = '# ' + source.stem + '\n\nStatus: implementation ready; full-stack execution and screenshots pending.\n\n'
        guide += '\n\n'.join(f'## {i:02d}. {q}\n\n{a}\n\n{PENDING}'
                              for i, (q, a) in enumerate(report['items'], 1))
        (ROOT / 'docs' / f'report_{report["key"]}.md').write_text(guide + '\n', encoding='utf-8')
        print(f'{len(found):2d} questions annotated: {destination.relative_to(ROOT)}')


if __name__ == '__main__':
    main()
