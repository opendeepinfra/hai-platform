#!/usr/bin/env python3
"""Make every db_schemas/*.sql safe to re-run.

The migration runner replays all schema files on every container start, so each
one must be idempotent. PostgreSQL 12 supports IF NOT EXISTS for TABLE, INDEX,
UNIQUE INDEX, SCHEMA and MATERIALIZED VIEW, plus ADD COLUMN IF NOT EXISTS, but
NOT for CREATE TYPE or CREATE TRIGGER -- those use the standard idioms verified
against PostgreSQL 12.22:

    create type    -> DO block catching duplicate_object
    create trigger -> DROP TRIGGER IF EXISTS + CREATE TRIGGER

Usage: idempotentize.py [--check] [--diff]
"""
import difflib
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db_schemas")

# keyword -> already-idempotent guard
SIMPLE = [
    (r"\b(create\s+unique\s+index)", "if not exists"),
    (r"\b(create\s+index)", "if not exists"),
    (r"\b(create\s+materialized\s+view)", "if not exists"),
    (r"\b(create\s+table)", "if not exists"),
    (r"\b(create\s+schema)", "if not exists"),
]

TRIGGER_RE = re.compile(
    r"(?is)\bcreate\s+trigger\s+(\S+)\s+(.*?\bon\s+)(\S+)"
)
# anchored at line start (re.M) so the copy already indented inside a generated
# DO block is not wrapped a second time
TYPE_RE = re.compile(r"(?im)^create\s+type\s+([^;]*);")
ADDCOL_RE = re.compile(r"(?i)\badd\s+column\s+(?!if\s+not\s+exists)")


def transform(sql: str):
    notes = []

    for kw, guard in SIMPLE:
        pat = re.compile(kw + r"(\s+)(?!if\s+not\s+exists)", re.I)

        def sub(m, guard=guard):
            return m.group(1) + " " + guard + " "

        sql, n = pat.subn(sub, sql)
        if n:
            notes.append("%s x%d" % (kw.replace("\\b", "").replace("\\s+", " "), n))

    # alter table ... add column   (the pattern swallows the trailing space,
    # so the replacement must put one back)
    sql, n = ADDCOL_RE.subn("add column if not exists ", sql)
    if n:
        notes.append("add column x%d" % n)

    # create type  ->  DO block
    def type_sub(m):
        body = m.group(0).strip()
        indented = "\n".join("  " + ln for ln in body.splitlines())
        return ("do $$\nbegin\n%s\nexception when duplicate_object then null;\nend $$;"
                % indented)

    sql, n = TYPE_RE.subn(type_sub, sql)
    if n:
        notes.append("create type x%d (DO block)" % n)

    # create trigger  ->  drop + create
    # DROP TRIGGER takes only "<name> ON <table>"; the timing clause
    # (BEFORE INSERT ...) must NOT be carried over, otherwise the statement is
    # a syntax error. Guard against re-adding the drop on a second run.
    def trig_sub(m):
        name, table = m.group(1), m.group(3)
        drop = "drop trigger if exists %s on %s;" % (name, table)
        before = m.string[:m.start()]
        if re.search(re.escape(drop) + r"\s*$", before):
            return m.group(0)
        return drop + "\n" + m.group(0)

    sql, n = TRIGGER_RE.subn(trig_sub, sql)
    if n:
        notes.append("create trigger x%d (drop+create)" % n)

    return sql, notes


def main():
    check = "--check" in sys.argv
    show_diff = "--diff" in sys.argv
    files = sorted(f for f in os.listdir(ROOT) if f.endswith(".sql"))
    total = 0
    for fn in files:
        path = os.path.join(ROOT, fn)
        with open(path, encoding="utf-8") as f:
            src = f.read()
        out, notes = transform(src)
        if out == src:
            continue
        total += 1
        print("%-48s %s" % (fn, ", ".join(notes) if notes else "(changed)"))
        if show_diff:
            for ln in difflib.unified_diff(src.splitlines(), out.splitlines(),
                                           "a/" + fn, "b/" + fn, lineterm="", n=1):
                print("    " + ln)
        if not check:
            with open(path, "w", encoding="utf-8") as f:
                f.write(out)
    print("\n%d/%d files %s" % (total, len(files),
                                "would change" if check else "rewritten"))


main()
