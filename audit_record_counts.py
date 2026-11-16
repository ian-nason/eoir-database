"""Independent streaming record audit for the publisher's unquoted TSV export."""
from pathlib import Path
import io,json,zipfile,re
ROOT=Path(__file__).resolve().parent;out=ROOT/'artifacts-2026-11-16';results=[]
with zipfile.ZipFile(ROOT/'data-2026-11-16/eoir-september-2026.zip') as z:
 for info in z.infolist():
  if not info.filename.lower().endswith('.csv'):continue
  # Universal newline handling includes the source's occasional lone CR.
  with z.open(info) as raw,io.TextIOWrapper(raw,encoding='latin1',newline=None) as f:
   header=f.readline();columns=header.count('\t')+1;records=empty=excess=short=invalid_utf8=0
   for line in f:
    assert len(line)<1024*1024,'Unexpected unbounded source record'
    line=line.rstrip('\r\n')
    if not line:empty+=1;continue
    if re.search(r'[\x80-\xff]',line):
     try:line.encode('latin1').decode('utf8')
     except UnicodeDecodeError:invalid_utf8+=1;continue
    count=line.count('\t')+1
    if count>columns:excess+=1
    else:records+=1;short+=count<columns
  results.append({'file':info.filename,'columns':columns,'accepted_unquoted_records':records,'excess_field_records':excess,'invalid_utf8_records':invalid_utf8,'short_padded_records':short,'empty_lines':empty})
  (out/'independent-record-counts.json').write_text(json.dumps(results,indent=2));print(info.filename,records,excess,flush=True)
