import json
import sqlite3

# Bounded number of recompute passes when before_commit interference keeps
# changing ownership or local dependencies. Not a fixed single retry.
MAX_PUBLISH_PASSES = 100


class Warehouse:
    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path, isolation_level=None)
        # Short write transactions must never fail just because another
        # connection briefly holds the lock; wait rather than error out.
        self.db.execute("PRAGMA busy_timeout=5000")
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

    # ------------------------------------------------------------------
    # Partial, per-partition atomic publishing.
    # ------------------------------------------------------------------
    def _classify(self, deliveries):
        """Read authority outside any write lock and classify each segment.

        Returns (accepted_map, skipped_map) where accepted maps partition to
        (token, after, rows). Unknown partitions and ownership mismatches are
        STALE_OWNER (takes priority over POSITION_MOVED); a live owner whose
        position no longer matches the segment's after is POSITION_MOVED.
        """
        accepted, skipped = {}, {}
        for token, after, rows in deliveries:
            p, epoch, owner = token
            current = self.db.execute(
                "SELECT epoch,owner,pos FROM authority WHERE p=?", (p,)).fetchone()
            if current is None or current[0] != epoch or current[1] != owner:
                skipped[p] = "STALE_OWNER"
            elif current[2] != after:
                skipped[p] = "POSITION_MOVED"
            else:
                accepted[p] = (token, after, rows)
        return accepted, skipped

    def _compute_candidate(self, accepted):
        """Rebuild the publish result from original fetched segments only.

        Heavy work (highest-rev merge, bucket recomputation) runs here,
        outside the global write lock. Only IDs touched by accepted segments
        and the site buckets their old/new records intersect are recomputed;
        there is no whole-table scan and no global generation. Precomputed
        plan values that could include skipped data are never reused. The
        in-batch latest revision per ID is tracked so multiple revisions
        inside one segment collapse exactly like prepare() does.
        Returns (record_rows, bucket_values, deps) where deps is the exact
        local warehouse state this result depends on.
        """
        finals = {}
        buckets = {}
        deps_ids, deps_sites = set(), set()
        for token, after, rows in accepted.values():
            for pos, incoming in rows:
                ident, rev, site, amount, deleted = incoming
                deps_ids.add(ident)
                if ident not in finals:
                    value = self.db.execute(
                        "SELECT id,rev,site,amount,deleted FROM records WHERE id=?",
                        (ident,)).fetchone()
                    finals[ident] = list(value) if value is not None else None
                old = finals[ident]
                if old is not None and rev <= old[1]:
                    # Segment cannot supersede what is already in the warehouse.
                    continue
                for item, sign in ((old, -1), (incoming, 1)):
                    if item is None or item[4]:
                        continue
                    bucket = item[2]
                    deps_sites.add(bucket)
                    if bucket not in buckets:
                        value = self.db.execute(
                            "SELECT n,amount FROM totals WHERE site=?",
                            (bucket,)).fetchone()
                        buckets[bucket] = list(value or (0, 0))
                    buckets[bucket][0] += sign
                    buckets[bucket][1] += sign * item[3]
                finals[ident] = [ident, rev, site, amount, int(bool(deleted))]
        record_rows = [row for row in finals.values() if row is not None]
        deps = {
            "records": {
                ident: self.db.execute(
                    "SELECT id,rev,site,amount,deleted FROM records WHERE id=?",
                    (ident,)).fetchone()
                for ident in deps_ids},
            "totals": {
                site: self.db.execute(
                    "SELECT n,amount FROM totals WHERE site=?", (site,)).fetchone()
                for site in deps_sites},
        }
        return record_rows, buckets, deps

    def _dependencies_intact(self, deps):
        """Re-check the exact local dependencies inside the write transaction."""
        for ident, snapshot in deps["records"].items():
            now = self.db.execute(
                "SELECT id,rev,site,amount,deleted FROM records WHERE id=?",
                (ident,)).fetchone()
            if now != snapshot:
                return False
        for site, snapshot in deps["totals"].items():
            now = self.db.execute(
                "SELECT n,amount FROM totals WHERE site=?", (site,)).fetchone()
            if now != snapshot:
                return False
        return True

    def publish_available(self, plan, before_commit=None, lose_reply=False):
        request = plan["request"]
        deliveries = plan["deliveries"]
        # Contiguity is an intrinsic property of the opaque plan; reject before
        # touching anything so no half-commit can remain.
        for token, after, rows in deliveries:
            if [pos for pos, body in rows] != list(
                    range(after + 1, after + 1 + len(rows))):
                raise ValueError("NONCONTIGUOUS")

        # Idempotent reply: an existing receipt wins without callbacks or work.
        existing = self.receipt(request)
        if existing is not None:
            return existing

        attempt = 0
        while True:
            attempt += 1
            if attempt > MAX_PUBLISH_PASSES:
                raise RuntimeError("PUBLISH_RETRY_LIMIT")
            accepted, skipped = self._classify(deliveries)
            # All heavy business recomputation happens here, without the lock.
            record_rows, bucket_values, deps = self._compute_candidate(accepted)
            candidate = {
                "accepted": {p: token_after_rows[1] + len(token_after_rows[2])
                             for p, token_after_rows in accepted.items()},
                "skipped": skipped,
                "record_rows": record_rows,
                "bucket_values": bucket_values,
                "deps": deps,
            }
            if before_commit is not None:
                # A second connection may change ownership or publish here; it
                # must not be blocked by this connection's business calculation
                # because no write transaction is open yet.
                before_commit()
            self.db.execute("BEGIN IMMEDIATE")
            try:
                old = self.receipt(request)
                if old is not None:
                    # Lost reply racing a committed retry: return the original.
                    self.db.execute("COMMIT")
                    return old
                now_accepted, now_skipped = self._classify(deliveries)
                if now_accepted.keys() != accepted.keys() or \
                        now_skipped != skipped or \
                        any(now_accepted[p][1] != accepted[p][1]
                            for p in accepted) or \
                        not self._dependencies_intact(deps):
                    # Ownership, position or an intersecting dependency changed:
                    # discard this candidate and recompute outside the lock.
                    self.db.execute("ROLLBACK")
                    continue
                # Short atomic install of only the locally recomputed values.
                for row in record_rows:
                    self.db.execute(
                        "INSERT OR REPLACE INTO records VALUES(?,?,?,?,?)", row)
                for site, (n, amount) in bucket_values.items():
                    if n:
                        self.db.execute(
                            "INSERT OR REPLACE INTO totals VALUES(?,?,?)",
                            (site, n, amount))
                    else:
                        self.db.execute(
                            "DELETE FROM totals WHERE site=?", (site,))
                for p, (token, after, rows) in accepted.items():
                    self.db.execute(
                        "UPDATE authority SET pos=? WHERE p=?",
                        (after + len(rows), p))
                result = {
                    "request": request,
                    "accepted": candidate["accepted"],
                    "skipped": dict(skipped),
                }
                self.db.execute(
                    "INSERT INTO receipts VALUES(?,?)",
                    (request, json.dumps(result, sort_keys=True)))
                self.db.execute("COMMIT")
                break
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
