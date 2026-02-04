$db = ".\firstlight.sqlite"

$py = @"
import sqlite3, sys, re
db = sys.argv[1]
con = sqlite3.connect(db)
con.row_factory = sqlite3.Row

rows = con.execute("""
select created_utc, object_id, candid, report_id, detail
from tns_actions
where action='failed_auth'
order by created_utc desc
limit 5
""").fetchall()

for r in rows:
    d = r["detail"] or ""
    http = re.findall(r'http=\d+', d, flags=re.I)
    idc  = re.findall(r'id_code=\d+', d, flags=re.I)
    print("="*90)
    print("ts:", r["created_utc"], "obj:", r["object_id"], "candid:", r["candid"], "rid:", r["report_id"])
    print("tokens_http:", http)
    print("tokens_id  :", idc)

    # context around auth markers
    found = False
    for m in re.finditer(r'(http=401|unauthorized|forbidden)', d, flags=re.I):
        found = True
        s = max(0, m.start()-140); e = min(len(d), m.end()+140)
        print("---- auth_context ----")
        print(d[s:e].replace("\n","\\n"))
    if not found:
        print("---- auth_context ----")
        print("<no explicit 401/unauthorized found in detail>")
"@

$tmp = Join-Path $env:TEMP ("tns_detail_probe_{0}.py" -f ([guid]::NewGuid().ToString("N")))
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($tmp, $py, $utf8NoBom)
python $tmp $db
Remove-Item -LiteralPath $tmp -ErrorAction SilentlyContinue
