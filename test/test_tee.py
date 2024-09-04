# test stream source generators are not duplicated before tee
import stream

def test_tee():
    seen = []

    def source():
        nonlocal seen
        for i in range(20):
            seen.append(i)
            yield i

    ans = []

    foo = stream.filter(lambda x: x%3==0) >> stream.append(ans)

    bar = source() >> stream.tee(foo) >> stream.take(10)
    ans1 = bar >> list
    assert tuple(ans1) == tuple(range(10))
    assert tuple(ans) == tuple(range(0,20,3))
    assert tuple(seen) == tuple(range(20))
