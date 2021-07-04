import os
import json
from typing import List, Optional, Dict
import getpass


import click

import constants

import sqlite3


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
ORDER BY 
    r.zlastname, r.zfirstname
"""


def dict_factory(cursor, row) -> Dict:
    d = dict()
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


conn = sqlite3.connect(PATH_DB)
conn.row_factory = dict_factory

def print_address(a):
    print(f'{a["uid"]}, {a["ZLASTNAME"]}, {a["ZFIRSTNAME"]}, {a["ZFULLNUMBER"]}, {a["ZADDRESS"]}')
    
    
    
    
@click.group()
@click.option("--user", help="User name", default=None, required=False)
@click.option("--debug", "-d", is_flag=True, help="Debug flag", required=False)
@click.pass_context
def cli(ctx, user, debug):
    username = user or getpass.getuser()
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug


@cli.command()
@click.pass_context
@click.argument("spec", required=False)
def ls(ctx, spec):
    """List address entries with search spec."""
    
    spec = spec or ""
    spec += "%"
    
    cur = conn.cursor()
    cur.execute(SQL_FIND, [spec])
    rows = cur.fetchall()
    for row in rows:
        print_address(row)


if __name__ == "__main__":
    cli()
