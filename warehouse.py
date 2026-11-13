import json
import sqlite3


class Warehouse:
    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS authority(
            p TEXT PRIMARY KEY, epoch INTEGER, owner TEXT, pos INTEGER);
        CREATE TABLE IF NOT EXISTS records(
            id TEXT PRIMARY KEY, rev INTEGER, site TEXT, amount INTEGER, deleted INTEGER);
        CREATE TABLE IF NOT EXISTS totals(site TEXT PRIMARY KEY, n INTEGER, amount INTEGER);
        CREATE TABLE IF NOT EXISTS receipts(request TEXT PRIMARY KEY, body TEXT);
        """)

    def bind(self, token, source_floor):
        p, epoch, owner = token
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT epoch,pos FROM authority WHERE p=?", (p,)).fetchone()
            if row and epoch < row[0]:
                raise RuntimeError("STALE_BIND")
            pos = source_floor if row is None else max(source_floor, row[1])
            self.db.execute("INSERT OR REPLACE INTO authority VALUES(?,?,?,?)",
                            (p, epoch, owner, pos))
            self.db.execute("COMMIT")
            return pos
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def positions(self):
        return {p: pos for p, pos in self.db.execute("SELECT p,pos FROM authority ORDER BY p")}

    def receipt(self, request):
        row = self.db.execute("SELECT body FROM receipts WHERE request=?", (request,)).fetchone()
        return None if row is None else json.loads(row[0])

    def prepare(self, request, deliveries):
        # deliveries: [[token, after, [[pos, record], ...]], ...]
        finals = {}
        buckets = {}
        for token, after, rows in sorted(deliveries, key=lambda d: d[0][0]):
            for pos, incoming in rows:
                ident, rev, site, amount, deleted = incoming
                old = finals.get(ident)
                if old is None:
                    old = self.db.execute("SELECT id,rev,site,amount,deleted FROM records WHERE id=?",
                                          (ident,)).fetchone()
                if old is not None and rev <= old[1]:
                    continue
                for item, sign in ((old, -1), (incoming, 1)):
                    if item is None or item[4]:
                        continue
                    bucket = item[2]
                    if bucket not in buckets:
                        value = self.db.execute("SELECT n,amount FROM totals WHERE site=?",
                                                (bucket,)).fetchone()
                        buckets[bucket] = list(value or (0, 0))
                    buckets[bucket][0] += sign
                    buckets[bucket][1] += sign * item[3]
                finals[ident] = list(incoming)
        return {"request": request, "deliveries": deliveries,
                "records": list(finals.values()), "totals": buckets}

    def publish(self, plan, lose_reply=False):
        request = plan["request"]
        self.db.execute("BEGIN IMMEDIATE")
        try:
            old = self.receipt(request)
            if old is not None:
                self.db.execute("COMMIT")
                return old
            positions = {}
            for token, after, rows in plan["deliveries"]:
                p, epoch, owner = token
                current = self.db.execute(
                    "SELECT epoch,owner,pos FROM authority WHERE p=?", (p,)).fetchone()
                if current is None or list(current) != [epoch, owner, after]:
                    raise RuntimeError("STALE_PLAN")
                if [pos for pos, body in rows] != list(range(after + 1, after + 1 + len(rows))):
                    raise ValueError("NONCONTIGUOUS")
                positions[p] = after + len(rows)
            # Existing publisher rebases under its global write transaction.
            # Correct, but unavailable partitions reject the entire plan.
            plan = self.prepare(request, plan["deliveries"])
            for row in plan["records"]:
                self.db.execute("INSERT OR REPLACE INTO records VALUES(?,?,?,?,?)", row)
            for site, (n, amount) in plan["totals"].items():
                if n:
                    self.db.execute("INSERT OR REPLACE INTO totals VALUES(?,?,?)", (site, n, amount))
                else:
                    self.db.execute("DELETE FROM totals WHERE site=?", (site,))
            for p, pos in positions.items():
                self.db.execute("UPDATE authority SET pos=? WHERE p=?", (pos, p))
            result = {"request": request, "positions": positions}
            self.db.execute("INSERT INTO receipts VALUES(?,?)", (request, json.dumps(result, sort_keys=True)))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        if lose_reply:
            raise RuntimeError("PUBLISH_REPLY_LOST")
        return result

    def read(self):
        return {
            "positions": self.positions(),
            "records": [list(r) for r in self.db.execute("SELECT * FROM records ORDER BY id")],
            "totals": [list(r) for r in self.db.execute("SELECT * FROM totals ORDER BY site")]
        }
