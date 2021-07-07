import os
import sys

from typing import List, Dict
import getpass
import sqlite3

import click

import ipdb


BASE_DIR = f"/Users/{getpass.getuser()}/Library/Application Support/AddressBook/Sources/"
DB_NAME = "AddressBook-v22.abcddb"
def get_db_path(base_path):
    """Take the first directory in the base_dir."""
    dirs = os.listdir(base_path)
    return os.path.join(BASE_DIR, dirs.pop(), DB_NAME)

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
"""

SQL_FIND_WHERE_EXCLUDE = """ 

WHERE
    (r.zlastname LIKE ?
    OR r.zfirstname LIKE ?)
    AND r.z_pk NOT IN (SELECT uid FROM exclude_list)
"""

SQL_FIND_WHERE_INCLUDE = """ 

WHERE
    (r.zlastname LIKE ?
    OR r.zfirstname LIKE ?)
"""

SQL_FIND_ORDER = """

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

def get_search_sql(include=False):
    sql = SQL_FIND
    sql += SQL_FIND_WHERE_INCLUDE if include else SQL_FIND_WHERE_EXCLUDE
    sql += SQL_FIND_ORDER
    return sql

def dict_factory(cursor, row) -> Dict:
    d = dict()
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


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
    db_path = get_db_path(BASE_DIR)
    if not os.path.exists(db_path):
        print(f"Could not find database at path: \n\t{db_path}")
        raise SystemExit
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    ctx.obj["conn"] = conn    
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS exclude_list (uid data_type PRIMARY KEY)")
    
def insert_exclude(conn, cursor, uid):
    cursor.execute("SELECT uid FROM exclude_list WHERE uid = ?", [uid])
    entry = cursor.fetchone()
    if entry is None:
        cursor.execute("INSERT INTO exclude_list (uid) VALUES (?)", [uid])
        conn.commit()

@cli.command()
@click.pass_context
@click.argument("spec", required=False)
@click.option("--query-exclude", "-d", is_flag=True, help="Query each entry if it should be listed", required=False)
@click.option("--include", "-i", is_flag=True, help="Include excluded contact records", required=False)
def ls(ctx, spec, query_exclude, include):
    """List address entries with search spec."""
    
    spec = spec or ""
    spec += "%"
    conn = ctx.obj.get("conn")
    cur = conn.cursor()
    cur.execute(get_search_sql(include), [spec, spec])
    addresses = gather_addresses(cur)
    for uid, contact in addresses.items():
        print_address_dict(contact)
        if query_exclude:
            if click.confirm('Exclude from future queries?'):
                insert_exclude(conn, cur, uid)
                print(f"Excluding this address in future: {uid}")
        
@cli.command()
@click.pass_context
@click.argument("uid", required=True)
def exclude(ctx, uid):
    """Tell us to not include the record with uid in future 
    listings."""
    conn = ctx.obj.get("conn")    
    cur = conn.cursor()
    cur.execute(SQL_BY_UID, [uid])
    addresses = gather_addresses(cur)
    insert_exclude(cur, uid)
    print("Excluded: ")
    for k, v in addresses.items():
        print_address_dict(v)
    

# include

@cli.command()
@click.pass_context
def info(ctx):
    """Print simple stats."""
    conn = ctx.obj.get("conn")
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
    print(f"Database: {get_db_path(BASE_DIR)}")

    


if __name__ == "__main__":
    cli()
