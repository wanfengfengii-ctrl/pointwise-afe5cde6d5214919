import json
import os
import sys
import tempfile
from warehouse import Warehouse


def oracle(prefixes):
    rows = {}
    for batch in prefixes:
        for incoming in batch:
            if incoming[0] not in rows or incoming[1] > rows[incoming[0]][1]:
                rows[incoming[0]] = list(incoming)
    totals = {}
    for row in rows.values():
        row[4] = int(row[4])
        if not row[4]:
            n, amount = totals.get(row[2], (0, 0))
            totals[row[2]] = (n+1, amount+row[3])
    return {'records': sorted(rows.values()),
            'totals': [[k, *v] for k, v in sorted(totals.items())]}


def segment(token, rows):
    return [[token, 0, [[i+1, r] for i, r in enumerate(rows)]]]


def prefix_audit(segments):
    """Independent prefix accounting: reconstruct expected state purely from
    the continuous prefixes that were actually accepted, using only the
    published record semantics, then compare against a live read()."""
    expected = oracle([rows for rows in segments])

    def check(w):
        actual = w.read()
        assert actual['records'] == expected['records'], \
            (actual['records'], expected['records'])
        assert actual['totals'] == expected['totals'], \
            (actual['totals'], expected['totals'])
    return check, expected


def normal():
    # The existing publisher is correct even for stale precomputation.
    for rows_a, rows_b in [
        ([['x', 1, 'east', 10, False]], [['x', 2, 'west', 7, False]]),
        ([['x', 1, 'east', 10, False]], [['y', 1, 'east', -10, False]])]:
        w = Warehouse(); a = ['a', 1, 'A']; b = ['b', 1, 'B']
        w.bind(a, 0); w.bind(b, 0)
        pa = w.prepare('a', segment(a, rows_a)); pb = w.prepare('b', segment(b, rows_b))
        rb = w.publish(pb); ra = w.publish(pa)
        assert w.publish(pb) == rb and w.publish(pa) == ra
        expected = oracle([rows_a, rows_b]); actual = w.read()
        assert actual['records'] == expected['records']
        assert actual['totals'] == expected['totals']
        assert actual['positions'] == {'a': 1, 'b': 1}
    w = Warehouse(); a = ['a', 1, 'A']; b = ['b', 1, 'B']
    w.bind(a, 0); w.bind(b, 0)
    plan = w.prepare('both', segment(a, [['x', 1, 'e', 2, False]])
                     + segment(b, [['y', 1, 'w', 3, False]]))
    w.bind(['a', 2, 'NEW'], 0)
    before = w.read()
    try:
        w.publish(plan)
    except RuntimeError as exc:
        assert str(exc) == 'STALE_PLAN'
    else:
        raise AssertionError('legacy publisher must reject whole stale plan')
    assert w.read() == before and w.receipt('both') is None
    w = Warehouse(); a = ['a', 1, 'A']; w.bind(a, 0)
    rows = [['x', 1, 'a', 10, False], ['x', 2, 'b', 7, False], ['y', 1, 'b', -7, False]]
    p = w.prepare('reply', segment(a, rows))
    try:
        w.publish(p, lose_reply=True)
    except RuntimeError as exc:
        assert str(exc) == 'PUBLISH_REPLY_LOST'
    else:
        raise AssertionError('expected lost reply')
    assert w.publish(p) == {'request': 'reply', 'positions': {'a': 3}}
    assert w.read()['totals'] == [['b', 2, 0]]
    print('BASELINE_PUBLISH_OK')


# ---------------------------------------------------------------------------
# publish_available verification
# ---------------------------------------------------------------------------

def verify_partial_reowner():
    """One partition re-owned mid-plan: its work is skipped with reason, the
    other is accepted; skipped records never leak into warehouse state."""
    w = Warehouse(); a = ['a', 1, 'A']; b = ['b', 1, 'B']
    w.bind(a, 0); w.bind(b, 0)
    ra = [['x', 1, 'east', 10, False]]
    rb = [['x', 2, 'west', 7, False]]
    plan = w.prepare('p1', segment(a, ra) + segment(b, rb))
    w.bind(['b', 2, 'NEXT'], 0)          # b changes owner before commit
    result = w.publish_available(plan)
    assert result == {'request': 'p1', 'accepted': {'a': 1},
                      'skipped': {'b': 'STALE_OWNER'}}, result
    assert w.receipt('p1') == result     # receipt persisted and readable
    check, expected = prefix_audit([ra])
    check(w)
    assert w.read()['positions'] == {'a': 1, 'b': 0}
    # Retrying after a lost response returns the original receipt, no re-apply.
    again = w.publish_available(plan)
    assert again == result
    assert w.read()['positions'] == {'a': 1, 'b': 0}
    print('  partial re-owner: receipt=%s' % json.dumps(result, sort_keys=True))


def verify_all_skipped():
    """Every partition stale: an empty accepted receipt is still saved."""
    w = Warehouse(); a = ['a', 1, 'A']
    w.bind(a, 0)
    plan = w.prepare('p2', segment(a, [['x', 1, 'e', 5, False]]))
    w.bind(['a', 2, 'NEW'], 0)
    before = w.read()
    result = w.publish_available(plan)
    assert result == {'request': 'p2', 'accepted': {},
                      'skipped': {'a': 'STALE_OWNER'}}
    assert w.receipt('p2') == result
    assert w.read() == before
    print('  all skipped: receipt=%s' % json.dumps(result, sort_keys=True))


def verify_position_moved(tmpdir):
    """Position advanced by another connection: POSITION_MOVED, retried once
    the interfering segment is published, then succeeds on a later request."""
    path = os.path.join(tmpdir, 'pos.db')
    w = Warehouse(path); other = Warehouse(path)
    a = ['a', 1, 'A']; w.bind(a, 0)
    plan = w.prepare('p3', segment(a, [['x', 1, 'e', 5, False]]))
    passes = []

    def push_once():
        if not passes:
            # Second connection really writes: not blocked by any write lock.
            p2 = other.prepare('p3-intruder',
                               segment(a, [['q', 1, 'e', 9, False]]))
            r = other.publish_available(p2)
            assert r['accepted'] == {'a': 1}, r
        passes.append(1)

    result = w.publish_available(plan, before_commit=push_once)
    assert len(passes) == 2 and result['skipped'] == {'a': 'POSITION_MOVED'}
    assert result['accepted'] == {}
    assert w.receipt('p3') == result
    check, _ = prefix_audit([[['q', 1, 'e', 9, False]]])
    check(w)
    assert w.read()['positions'] == {'a': 1}
    print('  position moved: candidates=%d receipt=%s'
          % (len(passes), json.dumps(result, sort_keys=True)))


def verify_reason_priority_and_unknown():
    """STALE_OWNER wins over POSITION_MOVED; unknown partition is STALE_OWNER."""
    w = Warehouse(); a = ['a', 1, 'A']; ghost = ['g', 1, 'G']
    w.bind(a, 0)
    # Move position AND change owner on a.
    intr = Warehouse()
    plan_move = w.prepare('p4-move', segment(a, [['m', 1, 'e', 1, False]]))
    w.publish_available(plan_move)
    w.bind(['a', 2, 'NEW'], 0)
    stale_plan = w.prepare('p4a', segment(a, [['n', 1, 'e', 1, False]]))
    r1 = w.publish_available(stale_plan)
    assert r1['skipped'] == {'a': 'STALE_OWNER'}, r1
    ghost_plan = w.prepare('p4b', segment(ghost, [['n', 1, 'e', 1, False]]))
    r2 = w.publish_available(ghost_plan)
    assert r2['skipped'] == {'g': 'STALE_OWNER'}, r2
    print('  reason priority: %s / %s'
          % (r1['skipped'], r2['skipped']))


def verify_same_id_across_partitions():
    """Same ID across partitions: highest rev wins; when the higher rev is
    skipped the accepted lower rev is merged against current warehouse state,
    and buckets of skipped data are never touched."""
    w = Warehouse(); a = ['a', 1, 'A']; b = ['b', 1, 'B']
    w.bind(a, 0); w.bind(b, 0)
    ra = [['x', 1, 'east', 10, False]]
    rb = [['x', 2, 'west', 7, False]]
    plan = w.prepare('p5', segment(a, ra) + segment(b, rb))
    w.bind(['b', 2, 'NEXT'], 0)
    result = w.publish_available(plan)
    assert result['accepted'] == {'a': 1} and result['skipped'] == {'b': 'STALE_OWNER'}
    check, _ = prefix_audit([ra])
    check(w)
    assert w.read()['positions'] == {'a': 1, 'b': 0}

    # Now a still-held partition delivers rev 3; it must supersede rev 1 and
    # move the bucket correctly. The segment starts after the current pos.
    rc = [['x', 3, 'north', 4, False]]
    plan2 = w.prepare('p5b', [[a, 1, [[2, rc[0]]]]])
    r2 = w.publish_available(plan2)
    assert r2['accepted'] == {'a': 2}, r2
    check, _ = prefix_audit([ra, rc])
    check(w)
    assert w.read()['positions'] == {'a': 2, 'b': 0}
    print('  same id across partitions: final=%s'
          % json.dumps(w.read()['records']))


def verify_multi_rev_move_delete_resurrect_zero():
    """In-batch multiple revisions, site move, delete, resurrection, zero-sum
    non-empty bucket, different IDs sharing one bucket."""
    w = Warehouse(); a = ['a', 1, 'A']; b = ['b', 1, 'B']
    w.bind(a, 0); w.bind(b, 0)
    ra = [
        ['x', 1, 'east', 10, False],
        ['x', 2, 'east', 0, False],    # zero amount, non-empty bucket kept
        ['x', 3, 'west', 7, True],     # delete + move
        ['x', 4, 'west', 7, False],    # resurrection
        ['y', 1, 'west', -7, False],   # different id same bucket -> zero sum
    ]
    rb = [['z', 1, 'north', 3, False]]
    plan = w.prepare('p6', segment(a, ra) + segment(b, rb))
    w.bind(['b', 2, 'NEXT'], 0)       # b skipped; z must never appear
    result = w.publish_available(plan)
    assert result['accepted'] == {'a': 5}, result
    assert result['skipped'] == {'b': 'STALE_OWNER'}
    check, expected = prefix_audit([ra])
    check(w)
    # zero-sum non-empty group retained; north bucket absent (skipped).
    assert w.read()['totals'] == [['west', 2, 0]], w.read()['totals']
    assert [r[0] for r in w.read()['records']] == ['x', 'y']
    print('  move/delete/resurrect/zero-sum: totals=%s'
          % json.dumps(w.read()['totals']))


def verify_noncontiguous_atomicity():
    """NONCONTIGUOUS rejects before any write; nothing is half committed."""
    w = Warehouse(); a = ['a', 1, 'A']; w.bind(a, 0)
    deliveries = [[a, 0, [[2, ['x', 1, 'e', 1, False]]]]]  # gap: expects pos 1
    plan = w.prepare('p7', deliveries)
    before = w.read()
    try:
        w.publish_available(plan)
    except ValueError as exc:
        assert str(exc) == 'NONCONTIGUOUS'
    else:
        raise AssertionError('NONCONTIGUOUS expected')
    assert w.read() == before and w.receipt('p7') is None

    # Empty segment is contiguous and accepted, advancing by zero length.
    empty_plan = w.prepare('p7b', [[a, 0, []]])
    r = w.publish_available(empty_plan)
    assert r == {'request': 'p7b', 'accepted': {'a': 0}, 'skipped': {}}, r
    assert w.read()['positions'] == {'a': 0}
    print('  noncontiguous atomicity + empty segment OK')


def verify_callback_failure_atomicity():
    """If before_commit raises, no write transaction stays open and the
    plan can still be committed afterwards."""
    w = Warehouse(); a = ['a', 1, 'A']; w.bind(a, 0)
    plan = w.prepare('p8', segment(a, [['x', 1, 'e', 5, False]]))

    def boom():
        raise RuntimeError('callback exploded')

    try:
        w.publish_available(plan, before_commit=boom)
    except RuntimeError as exc:
        assert str(exc) == 'callback exploded'
    else:
        raise AssertionError('callback exception must propagate')
    assert w.receipt('p8') is None
    # Connection is still usable; subsequent commit succeeds.
    r = w.publish_available(plan)
    assert r['accepted'] == {'a': 1}
    assert w.receipt('p8') == r
    print('  callback failure atomicity OK')


def verify_lost_reply():
    """lose_reply raises after a real commit; retry returns the same receipt."""
    w = Warehouse(); a = ['a', 1, 'A']; w.bind(a, 0)
    plan = w.prepare('p9', segment(a, [['x', 1, 'e', 5, False]]))
    try:
        w.publish_available(plan, lose_reply=True)
    except RuntimeError as exc:
        assert str(exc) == 'PUBLISH_REPLY_LOST'
    else:
        raise AssertionError('PUBLISH_REPLY_LOST expected')
    again = w.publish_available(plan, lose_reply=True)  # existing receipt: no raise
    assert again == {'request': 'p9', 'accepted': {'a': 1}, 'skipped': {}}
    assert w.receipt('p9') == again
    print('  lost reply idempotent: %s' % json.dumps(again, sort_keys=True))


def verify_interleaving(tmpdir):
    """Two real connections on one temporary SQLite file. before_commit replays
    several ownership flaps and intersecting dependency changes, plus a fully
    disjoint external publish. The disjoint write must not cause a logical
    retry (no global generation); finite interference must still complete."""
    path = os.path.join(tmpdir, 'interleave.db')
    w = Warehouse(path); other = Warehouse(path)
    a = ['a', 1, 'A']; b = ['b', 1, 'B']
    c = ['c', 1, 'C']; d = ['d', 1, 'D']
    w.bind(a, 0); w.bind(b, 0); w.bind(c, 0); w.bind(d, 0)
    rows_a = [['x', 1, 'east', 10, False], ['x', 2, 'west', 7, False]]
    rows_b = [['y', 1, 'north', 3, False]]
    rows_c = [['k', 1, 'south', 5, False]]
    rows_d = [['d1', 1, 'deep', 2, False]]
    plan = w.prepare('p10', segment(a, rows_a) + segment(b, rows_b)
                     + segment(c, rows_c) + segment(d, rows_d))

    events = []

    def interfere():
        n = len(events)
        if n == 0:
            # Ownership flap on d: a candidate-accepted partition is re-owned.
            other.bind(['d', 2, 'TAKER'], 0)
            events.append('d-reowned')
        elif n == 1:
            # Intersecting logical dependency via another held partition:
            # another commit touches shared id x and the east/west buckets.
            # Local dependency checking must force a recomputation pass.
            dep_rows = [['x', 1, 'east', 10, False],
                        ['x', 3, 'east', 4, False]]
            dep_plan = other.prepare(
                'p10-dep', [[a, 0, [[i + 1, r] for i, r in enumerate(dep_rows)]]])
            r = other.publish_available(dep_plan)
            assert r['accepted'] == {'a': 2}, r
            events.append('dep-x-rev3')
        elif n == 2:
            # Second ownership flap: c is taken away as well.
            other.bind(['c', 2, 'TAKER'], 0)
            events.append('c-reowned')
        elif n == 3:
            # Fully disjoint publish: no shared ID and disjoint site buckets.
            # It must NOT invalidate this plan's candidate (no extra pass).
            other.bind(['z', 1, 'Z'], 0)
            disjoint = other.prepare(
                'p10-disjoint',
                segment(['z', 1, 'Z'], [['zzz', 1, 'remote', 99, False]]))
            r = other.publish_available(disjoint)
            assert r['accepted'] == {'z': 1}, r
            events.append('disjoint-z')

    result = w.publish_available(plan, before_commit=interfere)
    # Passes: candidate1(d re-owned) -> candidate2(dep x rev3 advances a) ->
    # candidate3(c re-owned) -> candidate4(disjoint z does not invalidate)
    # -> commit. Four candidates, four callback calls.
    assert events == ['d-reowned', 'dep-x-rev3', 'c-reowned', 'disjoint-z'], events
    assert result['request'] == 'p10'
    # Re-owned partitions (epochs moved forward) can never be re-accepted by
    # their stale tokens; a's position moved via the intersecting dependency
    # commit, so its stale segment is skipped with POSITION_MOVED; held b lands.
    assert result['accepted'] == {'b': 1}, result
    assert result['skipped'] == {
        'a': 'POSITION_MOVED', 'c': 'STALE_OWNER', 'd': 'STALE_OWNER'}, result
    # Independent prefix accounting over everything actually published.
    check, _ = prefix_audit([
        [['x', 3, 'east', 4, False]],                 # other connection, id x
        rows_b,                                        # b accepted final pass
        [['zzz', 1, 'remote', 99, False]],             # disjoint partition z
    ])
    check(w)
    assert w.read()['positions'] == {
        'a': 2, 'b': 1, 'c': 0, 'd': 0, 'z': 1}
    # Idempotent retry returns the original receipt and never double-posts.
    assert w.publish_available(plan) == result
    assert w.read()['positions'] == {
        'a': 2, 'b': 1, 'c': 0, 'd': 0, 'z': 1}
    print('  interleaving: candidates=%d events=%s' % (len(events), events))
    print('  interleaving receipt=%s' % json.dumps(result, sort_keys=True))
    print('  disjoint external write caused zero extra retry passes')


def verify_finite_interference(tmpdir):
    """Intersecting dependency changes repeat for several passes then stop.
    Completion must not be capped at one retry and must converge once the
    interference ends."""
    path = os.path.join(tmpdir, 'flap.db')
    w = Warehouse(path); other = Warehouse(path)
    a = ['a', 1, 'A']; b = ['b', 1, 'B']
    w.bind(a, 0); w.bind(b, 0)
    # Both partitions carry id x: b's candidate depends on the warehouse's
    # current x row, so external churn on x invalidates the local snapshot.
    plan = w.prepare('p11',
                     segment(a, [['x', 1, 'east', 10, False]])
                     + segment(b, [['x', 2, 'north', 2, False]]))
    calls = []

    def churn():
        calls.append(1)
        if len(calls) <= 5:
            # Each call publishes a fresh revision of x from the second
            # connection, invalidating the local dependency snapshot and
            # forcing a recomputation pass outside the write lock.
            rev = 10 + len(calls)
            nxt = other.prepare(
                'p11-churn-%d' % rev,
                [[a, len(calls) - 1,
                  [[len(calls), ['x', rev, 'east', 10 + rev, False]]]]])
            r = other.publish_available(nxt)
            assert r['accepted'] == {'a': len(calls)}, r

    result = w.publish_available(plan, before_commit=churn)
    assert len(calls) == 6, calls          # five disturbed passes + final one
    assert result['accepted'] == {'b': 1}, result
    assert result['skipped'] == {'a': 'POSITION_MOVED'}, result
    check, _ = prefix_audit([
        [['x', 15, 'east', 25, False]],                     # last churn write
        [['x', 2, 'north', 2, False]],                      # b loses to rev 15
    ])
    check(w)
    assert w.receipt('p11') == result
    print('  finite interference: %d candidates, converged after churn stopped'
          % len(calls))


def verify():
    normal()  # regression: original assertions kept intact
    with tempfile.TemporaryDirectory(prefix='wh-verify-') as tmpdir:
        verify_partial_reowner()
        verify_all_skipped()
        verify_position_moved(tmpdir)
        verify_reason_priority_and_unknown()
        verify_same_id_across_partitions()
        verify_multi_rev_move_delete_resurrect_zero()
        verify_noncontiguous_atomicity()
        verify_callback_failure_atomicity()
        verify_lost_reply()
        verify_interleaving(tmpdir)
        verify_finite_interference(tmpdir)
    print('PUBLISH_AVAILABLE_VERIFY_OK')


def demo():
    # Legacy all-or-nothing behavior is unchanged.
    w = Warehouse(); a = ['a', 1, 'A']; b = ['b', 1, 'B']
    w.bind(a, 0); w.bind(b, 0)
    p = w.prepare('batch', segment(a, [['x', 1, 'east', 10, False]])
                  + segment(b, [['x', 2, 'west', 7, False]]))
    w.bind(['b', 2, 'NEXT'], 0)
    try:
        w.publish(p)
    except RuntimeError as exc:
        print(json.dumps({'legacy_error': str(exc), 'actual': w.read()},
                         sort_keys=True))

    # New entrypoint publishes only partitions whose ownership and position are
    # still held at commit time, each skip explained, with a real saved receipt.
    w = Warehouse(); a = ['a', 1, 'A']; b = ['b', 1, 'B']
    w.bind(a, 0); w.bind(b, 0)
    plan = w.prepare('demo-batch',
                     segment(a, [['x', 1, 'east', 10, False]])
                     + segment(b, [['x', 2, 'west', 7, False]]))
    w.bind(['b', 2, 'NEXT'], 0)
    receipt = w.publish_available(plan)
    print(json.dumps({'publish_available_receipt': receipt,
                      'warehouse_after': w.read()}, sort_keys=True))
    # A retried request (e.g. response lost on the wire) returns the same
    # receipt and never double-posts.
    print(json.dumps({'retry_receipt': w.publish_available(plan),
                      'receipt_lookup': w.receipt('demo-batch')},
                     sort_keys=True))


if __name__ == '__main__':
    if sys.argv[1] == 'normal':
        normal()
    elif sys.argv[1] == 'verify':
        verify()
    elif sys.argv[1] == 'demo':
        demo()
    else:
        raise SystemExit('normal, verify or demo')
