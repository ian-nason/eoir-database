"""Audit unquoted source records, including CR-only terminators, without extracting."""
from pathlib import Path
import json,zipfile,re,codecs
ROOT=Path(__file__).resolve().parent;out=ROOT/'artifacts-2026-11-16';results=[]
with zipfile.ZipFile(ROOT/'data-2026-11-16/eoir-september-2026.zip') as z:
 for info in z.infolist():
  if not info.filename.lower().endswith('.csv'):continue
  separators=blanks=replacements=0;carry=b'';last=None;decoder=codecs.getincrementaldecoder('utf-8')('replace')
  with z.open(info) as f:
   while block:=f.read(8*1024*1024):
    replacements+=decoder.decode(block).count('\ufffd')
    block=carry+block;carry=b''
    if block.endswith(b'\r'):block,carry=block[:-1],b'\r'
    normal=block.replace(b'\r\n',b'\n').replace(b'\r',b'\n')
    separators+=normal.count(b'\n')
    blanks+=sum(len(m[0])-1 for m in re.finditer(b'\n{2,}',normal))
    if last==10 and normal.startswith(b'\n'):blanks+=1
    if normal:last=normal[-1]
   if carry:
    separators+=1
    if last==10:blanks+=1
    last=10
   replacements+=decoder.decode(b'',final=True).count('\ufffd')
  records=separators+(last!=10)-1-blanks
  rec={'file':info.filename,'nonempty_records_excluding_header':records,'empty_lines':blanks,'replacement_characters_when_decoded_as_utf8':replacements}
  results.append(rec);(out/'raw-record-audit.json').write_text(json.dumps(results,indent=2));print(info.filename,records,'utf8_replacements',replacements,flush=True)
