import json
import sys
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
            'totals': [[k, *v] for k,v in sorted(totals.items())]}


def segment(token, rows):
    return [[token, 0, [[i+1,r] for i,r in enumerate(rows)]]]


def normal():
    # The existing publisher is correct even for stale precomputation.
    for rows_a, rows_b in [
        ([['x',1,'east',10,False]], [['x',2,'west',7,False]]),
        ([['x',1,'east',10,False]], [['y',1,'east',-10,False]])]:
        w=Warehouse();a=['a',1,'A'];b=['b',1,'B']
        w.bind(a,0);w.bind(b,0)
        pa=w.prepare('a',segment(a,rows_a));pb=w.prepare('b',segment(b,rows_b))
        rb=w.publish(pb);ra=w.publish(pa)
        assert w.publish(pb)==rb and w.publish(pa)==ra
        expected=oracle([rows_a,rows_b]);actual=w.read()
        assert actual['records']==expected['records']
        assert actual['totals']==expected['totals']
        assert actual['positions']=={'a':1,'b':1}
    w=Warehouse();a=['a',1,'A'];b=['b',1,'B']
    w.bind(a,0);w.bind(b,0)
    plan=w.prepare('both',segment(a,[['x',1,'e',2,False]])+segment(b,[['y',1,'w',3,False]]))
    w.bind(['a',2,'NEW'],0)
    before=w.read()
    try:w.publish(plan)
    except RuntimeError as exc:assert str(exc)=='STALE_PLAN'
    else:raise AssertionError('legacy publisher must reject whole stale plan')
    assert w.read()==before and w.receipt('both') is None
    w=Warehouse();a=['a',1,'A'];w.bind(a,0)
    rows=[['x',1,'a',10,False],['x',2,'b',7,False],['y',1,'b',-7,False]]
    p=w.prepare('reply',segment(a,rows))
    try:w.publish(p,lose_reply=True)
    except RuntimeError as exc:assert str(exc)=='PUBLISH_REPLY_LOST'
    else:raise AssertionError('expected lost reply')
    assert w.publish(p)=={'request':'reply','positions':{'a':3}}
    assert w.read()['totals']==[['b',2,0]]
    print('BASELINE_PUBLISH_OK')


def demo():
    w=Warehouse();a=['a',1,'A'];b=['b',1,'B']
    w.bind(a,0);w.bind(b,0)
    p=w.prepare('batch',segment(a,[['x',1,'east',10,False]])+segment(b,[['x',2,'west',7,False]]))
    w.bind(['b',2,'NEXT'],0)
    try:w.publish(p)
    except RuntimeError as exc:print(json.dumps({'legacy_error':str(exc),'actual':w.read()}))


if __name__=='__main__':
    if sys.argv[1]=='normal':normal()
    elif sys.argv[1]=='demo':demo()
    else:raise SystemExit('normal or demo')
