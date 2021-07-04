from typing import List, Optional, Dict
import getpass
import sqlite3

import click




PATH_DB = "/Users/paul/Library/Application Support/AddressBook/Sources/692171AE-40FF-4D24-8DF5-3FC8A986925A/AddressBook-v22.abcddb"

SQL_FIND = """SELECT 
    r.z_pk as "uid",
    r.zlastname, 
    r.zfirstname, 
    p.zfullnumber,
    a.zaddress
FROM ZABCDRECORD r 
    LEFT JOIN ZABCDPHONENUMBER p ON r.z_pk = p.zowner  
    LEFT JOIN ZABCDEMAILADDRESS a ON r.z_pk = a.zowner  
    
WHERE 
    r.zlastname LIKE ?
    AND r.z_pk NOT IN (SELECT uid FROM exclude_list)
ORDER BY 
    r.zlastname, r.zfirstname
"""

SQL_BY_UID = """SELECT 
    r.z_pk as "uid",
    r.zlastname, 
    r.zfirstname, 
    p.zfullnumber,
    a.zaddress
FROM ZABCDRECORD r 
    LEFT JOIN ZABCDPHONENUMBER p ON r.z_pk = p.zowner  
    LEFT JOIN ZABCDEMAILADDRESS a ON r.z_pk = a.zowner      
WHERE 
    r.z_pk = ?
"""

import ipdb

def dict_factory(cursor, row) -> Dict:
    d = dict()
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


conn = sqlite3.connect(PATH_DB)
conn.row_factory = dict_factory

def print_address(a):
    print(f'{a["uid"]}, {a["ZLASTNAME"]}, {a["ZFIRSTNAME"]}, {a["ZFULLNUMBER"]}, {a["ZADDRESS"]}')

def print_address_dict(a):
    print(a["fullname"], f"[{a['uid']}]")
    for p in a["phones"]:
        if p:
            print(f"\t{p}")
    for e in a["emails"]:
        if e:
            print(f"\t{e}")
    
    
def find_address_by_pk(addresses, pk) -> Dict:
    if pk in addresses:
        return addresses[pk]
    return dict()

def gather_addresses(cursor) -> List[Dict]:
    addresses = dict()
    rows = cursor.fetchall()
    for row in rows:
        uid = row["uid"]
        a = find_address_by_pk(addresses, uid)
        if not a:
            a["emails"] = a.get("emails") or set()
            a["phones"] = a.get("phones") or set()
            a["fullname"] = f"{row.get('ZLASTNAME')}, {row.get('ZFIRSTNAME')}"
            a["uid"] = uid
            addresses[uid] = a
        a["phones"].add(row.get("ZFULLNUMBER"))
        a["emails"].add(row.get("ZADDRESS"))
    
    return addresses
        
    
    
@click.group()
@click.option("--user", help="User name", default=None, required=False)
@click.option("--debug", "-d", is_flag=True, help="Debug flag", required=False)
@click.pass_context
def cli(ctx, user, debug):
    username = user or getpass.getuser()
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug
    
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS exclude_list (uid data_type PRIMARY KEY)")
    
def insert_exclude(cursor, uid):

    cursor.execute("SELECT uid FROM exclude_list WHERE uid = ?", [uid])
    entry = cursor.fetchone()
    if entry is None:
        cursor.execute("INSERT INTO exclude_list (uid) VALUES (?)", [uid])
        conn.commit()

@cli.command()
@click.pass_context
@click.argument("spec", required=False)
@click.option("--query-exclude", "-d", is_flag=True, help="Query each entry if it should be listed", required=False)
def ls(ctx, spec, query_exclude):
    """List address entries with search spec."""
    
    spec = spec or ""
    spec += "%"
    
    cur = conn.cursor()
    cur.execute(SQL_FIND, [spec])
    addresses = gather_addresses(cur)
    for uid, contact in addresses.items():
        print_address_dict(contact)
        if query_exclude:
            if click.confirm('Exclude from future queries?'):
                insert_exclude(cur, uid)
                print(f"Excluding this address in future: {uid}")
        
@cli.command()
@click.pass_context
@click.argument("uid", required=True)
def exclude(ctx, uid):
    """Tell us to not include the record with uid in future 
    listings."""
    
    cur = conn.cursor()
    cur.execute(SQL_BY_UID, [uid])
    addresses = gather_addresses(cur)
    insert_exclude(cur, uid)
    print("Excluded: ")
    for k, v in addresses.items():
        print_address_dict(v)
    
# stats

# include

@cli.command()
@click.pass_context
def info(ctx):
    """Print simple stats."""
    
    cur = conn.cursor()
    sql = """SELECT count(*) as cnt FROM ZABCDRECORD
        WHERE z_pk NOT IN (SELECT uid FROM exclude_list)"""
    cur.execute(sql)
    # ipdb.set_trace()
    cnt = cur.fetchone()["cnt"]
    print(f"Total records not excluded: {cnt}")
    sql = """SELECT count(*) as cnt FROM exclude_list"""
    cur.execute(sql)
    cnt = cur.fetchone().get("cnt")
    print(f"Total records excluded: {cnt}")

    


if __name__ == "__main__":
    cli()
