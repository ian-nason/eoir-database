"""Rebuild from a pinned ZIP, extracting only one CSV at a time to bound disk use."""
from pathlib import Path
import argparse,datetime,hashlib,json,os,shutil,time,zipfile
import duckdb,requests
import build_database as b
ROOT=Path(__file__).resolve().parent;OUT=ROOT/'artifacts-2026-11-16';RAW=ROOT/'data-2026-11-16'
URL=b.EOIR_ZIP_URL;EXPECTED_BYTES=4596977100
EXPECTED_SHA256='a1d0394d31a6b810b81ae91a676adef5d6839ead7ca33c8e3ee38653687a7d2f'
def sha(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def main(archive_path=None):
    RAW.mkdir(exist_ok=True);OUT.mkdir(exist_ok=True)
    archive=Path(archive_path).resolve() if archive_path else RAW/'eoir-september-2026.zip'
    if archive_path:assert archive.is_file(),'Requested source archive is missing'
    assert not (OUT/'eoir.partial.duckdb').exists(),'Preserve any prior build for inspection'
    assert shutil.disk_usage(ROOT).free>25*1024**3,'Need 25 GiB free for archive, largest member, database, bounded spill, and reserve'
    if not archive.exists():
        part=archive.with_suffix('.part');etag='"9e25306bc51dd1:0"'
        for attempt in range(4):
            head=requests.head(URL,timeout=30);head.raise_for_status()
            assert int(head.headers['Content-Length'])==EXPECTED_BYTES and head.headers['ETag']==etag
            total=part.stat().st_size if part.exists() else 0
            if total==EXPECTED_BYTES:break
            headers={'Range':f'bytes={total}-','If-Range':etag} if total else {}
            try:
                with requests.get(URL,headers=headers,stream=True,timeout=(15,90)) as r,part.open('ab' if total else 'xb') as f:
                    r.raise_for_status()
                    if total:assert r.status_code==206 and r.headers['Content-Range'].startswith(f'bytes {total}-')
                    assert int(r.headers['Content-Length'])==EXPECTED_BYTES-total
                    for block in r.iter_content(4*1024*1024):
                        total+=len(block);assert total<=EXPECTED_BYTES;f.write(block)
                assert total==EXPECTED_BYTES;break
            except requests.RequestException:
                print('Download interruption; retained verified-identity partial',part.stat().st_size,flush=True)
                if attempt==3:raise
        assert part.stat().st_size==EXPECTED_BYTES
        archive.with_suffix('.part').rename(archive)
    archive_sha=sha(archive)
    assert archive.stat().st_size==EXPECTED_BYTES and archive_sha==EXPECTED_SHA256,'Source archive identity mismatch'
    print('archive verified',archive_sha,flush=True)
    con=duckdb.connect(str(OUT/'eoir.partial.duckdb'),config={'threads':'1','memory_limit':'1GiB','max_temp_directory_size':'1GiB'})
    con.execute('SET preserve_insertion_order=false')
    con.execute(f"SET temp_directory='{OUT}/spill'")
    built=set();table_sources={};lookup=set();members=[]
    with zipfile.ZipFile(archive) as z:
        mapped=[]
        for info in z.infolist():
            if not info.filename.lower().endswith('.csv'):continue
            p=Path(info.filename);is_lookup=p.parent.name.lower()=='lookup'
            mapping=b.LOOKUP_FILE_MAP if is_lookup else b.CORE_FILE_MAP
            match=next((v for k,v in mapping.items() if k.lower()==p.name.lower()),None)
            table=match['table'] if match else b.derive_table_name(p.name,prefix='lu_' if is_lookup else '')
            mapped.append((is_lookup,table,info))
        assert len({x[1] for x in mapped})==len(mapped),'Duplicate table mapping'
        order={t:i for i,t in enumerate(b.CORE_BUILD_ORDER)}
        mapped.sort(key=lambda x:(not x[0],order.get(x[1],999),x[1]))
        for is_lookup,table,info in mapped:
            assert shutil.disk_usage(ROOT).free>info.file_size+5*1024**3,'Insufficient physical disk reserve'
            temp=RAW/Path(info.filename).name;assert not temp.exists()
            digest=hashlib.sha256();physical_lines=0
            with z.open(info) as src,temp.open('xb') as dst:
                while block:=src.read(4*1024*1024):dst.write(block);digest.update(block);physical_lines+=block.count(b'\n')
            assert temp.stat().st_size==info.file_size
            n=b.build_table(con,table,temp)
            assert n>0,table
            built.add(table);table_sources[table]=temp.name
            if is_lookup:lookup.add(table)
            members.append({'file':info.filename,'table':table,'bytes':info.file_size,'sha256':digest.hexdigest(),'rows':n,'physical_newlines':physical_lines})
            con.execute('CHECKPOINT')
            # This exact temporary extraction was created above; the source ZIP is retained.
            assert temp.parent.resolve()==RAW.resolve();temp.unlink()
            (OUT/'progress.json').write_text(json.dumps(members,indent=2));print(table,n,flush=True)
    assert not b.FALLBACK_TABLES
    views=b.create_views(con,built);b.build_metadata(con,table_sources,lookup);b.build_columns_table(con)
    b.export_dictionary(con,OUT/'DICTIONARY.md');assert b.run_sanity_checks(con,OUT/'eoir.partial.duckdb')==0
    con.execute('CHECKPOINT');con.close();db=OUT/'eoir.duckdb';(OUT/'eoir.partial.duckdb').rename(db)
    rec={'source_snapshot':'2026-11-16','source_release':'September 2026, archive dated October 1','source_url':URL,'archive':{'bytes':EXPECTED_BYTES,'sha256':archive_sha},'members':members,'rows':sum(x['rows'] for x in members),'tables':len(members),'views':views,'artifact':{'bytes':db.stat().st_size,'sha256':sha(db)},'validation':'passed','limits':{'threads':1,'duckdb_memory':'1GiB','spill':'1GiB','extract':'one member at a time'}}
    (OUT/'sources-2026-11-16.json').write_text(json.dumps(rec,indent=2));print('VERIFIED',rec['rows'],flush=True)
if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',type=Path,help='Use the archived September 2026 ZIP; verify its full hash before parsing')
    main(parser.parse_args().archive)
