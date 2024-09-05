from stream import item, Source

def checkitem(s):
    for N in [20, 44, 51]:
        a = list(range(N))[s]
        b = Source(range(N)) >> item[s]
        print(N, s)
        assert tuple(a) == tuple(b)

def test_item():
    stops = [None, -100, -50, -22, -21, -20, -19, -10, -3, -2, -1,
              0, 1, 2, 3, 10, 19, 20, 21, 22, 50, 100]
    for step in [-2, -1, None, 1, 2]:
        for a in stops:
            for b in stops:
                checkitem( slice(a, b, step) )
