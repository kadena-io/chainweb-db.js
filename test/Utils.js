const { toEqual } = require('jest');
const { iter, asyncIter, sleep } = require('../src/Utils');
const utils = require('../src/Utils');

/* ************************************************************************** */
/* Test Utils */

const t = [0,1,2,3,4,5,6,7,8,9];

const seqArray = (n,s) => Array.from({length: n}, (_e, i) => i + (s ?? 0));

// TODO also consider return value of yield
expect.extend({
    toYieldEqual(received, expected) {
        // return this.toEqual(Array.from(received), Array.from(expected));
        const rarr = Array.from(received);
        const earr = Array.from(expected);
        return {
            message: () => `expected ${rarr} to equals ${earr}`,
            pass: this.equals(rarr, earr),
        };
    }
});

expect.extend({
    toResultEqual(received, expected) {
        const result = iter.drain(received);
        return {
            message: () => `expected iterator result ${result} to equal ${expected}`,
            pass: this.equals(result, expected),
        };
    }
});

expect.extend({
    toYieldForall(received, pred) {
        const rarr = Array.from(received);
        return {
            message: () => `expected all elements of ${rarr} to satisfy predicate`,
            pass: rarr.every(pred),
        }
    }
})

/* ************************************************************************** */
/* Test tests */

describe("test functions", () => {
    describe("toYieldEqual", () => {
        test("toYieldEqual succeeds on equal arrays", () => {
            expect([1,2,3]).toYieldEqual([1,2,3]);
        });
        test.each([[[]],[[1]],[[1,2]],[[1,2,3,4]],[[3,2,1]]])("toYieldEqual([1,2,3] fails for %p", (a) => {
            expect(a).not.toYieldEqual([1,2,3]);
        });
    });

    describe("toResultEqual", () => {
        test("toResultEqual on undefined result", () => {
            expect([1,2,3].values()).toResultEqual(undefined);
        });
        test("toResultEqual on defined result", () => {
            const i = function * () { yield 0; yield 1; return 17; }
            expect(i()).toResultEqual(17);
        });
        test("toResultEqual fails on wrong result", () => {
            const i = function * () { yield 0; yield 1; return 17; }
            expect(i()).not.toResultEqual(1);
        });
        test("toResultEqual fails on wrong undefined result", () => {
            const i = function * () { yield 0; yield 1; return 17; }
            expect(i()).not.toResultEqual(undefined);
        });
        test("toResultEqual fails on wrong undefined result", () => {
            const i = function * () { yield 0; yield 1; }
            expect(i()).not.toResultEqual(0);
        });
    });

    describe("toYieldForall", () => {
        test("toYieldForall succeeds for trivial predicate", () => {
            expect([1,2,3].values()).toYieldForall(_x => true);
        });
        test("toYieldForall always succeeds on empty iteration", () => {
            expect([].values()).toYieldForall(_x => false);
        });
        test("toYieldForall succeeds for even numbers", () => {
            expect([0,2,4].values()).toYieldForall(x => x % 2 == 0);
        });
        test("toYieldForall fails for even numbers", () => {
            expect([0,1,4].values()).not.toYieldForall(x => x % 2 == 0);
        });
    });
});

/* ************************************************************************** */
/* Misc tests */

describe("sleep", () => {
    test ("sleep 10ms resolves", async () => {
        await expect(sleep(10).then(() => 1)).resolves.toEqual(1);
    });
    test ("sleep 10ms is async", async () => {
        await expect(Promise.any([sleep(10).then(() => 1), Promise.resolve(2)])).resolves.toEqual(2);
        await expect(Promise.all([sleep(10).then(() => 1), Promise.reject(2)])).rejects.toEqual(2);
    });
});

/* ************************************************************************** */
/* Iterator tests */

describe("iterators", () => {
    describe("sequences", () => {
        test.each(seqArray(4))("sequence of length %i", (n) => {
            expect(iter.seq(0,n-1)).toYieldEqual(seqArray(n));
        });
        test.each([[1,0],[1,1],[1,4]])("sequence from %i to %i", (s,e) => {
            expect(iter.seq(s,e)).toYieldEqual(seqArray(e - s + 1,s));
        });
        test("sequence of length 5 and step 2", () => {
            expect(iter.seq(0,4,2)).toYieldEqual([0,2,4]);
        });
        test("sequence of length 6 and step 2", () => {
            expect(iter.seq(0,5,2)).toYieldEqual([0,2,4]);
        });
        test("sequence of length 1 and step 2", () => {
            expect(iter.seq(0,0,2)).toYieldEqual([0]);
        });
        test("sequence of length 0 and step 2", () => {
            expect(iter.seq(0,-1,2)).toYieldEqual([]);
        });
    });
    
    describe("take", () => {
        test.each([0,1,2,3,4,5,6,7,8,9,10])("take %i", (n) => {
            expect(iter.take(t.values(), n)).toYieldEqual(seqArray(n));
        });
        test.each([0,1,2,3,4,5,6,7,8,9,10])("length of take %i", (n) => {
            expect(iter.length(iter.take(t.values(), n))).toBe(n);
        });
        test("iterator take 6 from array of length 5", () => {
            expect(iter.take(iter.seq(0,4), 6)).toYieldEqual(seqArray(5));
        });
        describe ("result", () => {
            test ("take 0", () => {
                const i = function * () { yield 0; yield 1; return 17; }
                expect(iter.take(i(), 0)).toResultEqual(undefined);
                expect(iter.take(i(), 0)).not.toResultEqual(17);
                expect(iter.take(i(), 0)).not.toResultEqual(0);
                expect(iter.take(i(), 0)).not.toResultEqual(1);
            });
            test ("take 1", () => {
                const i = function * () { yield 0; yield 1; return 17; }
                expect(iter.take(i(), 1)).toResultEqual(undefined);
                expect(iter.take(i(), 1)).not.toResultEqual(17);
                expect(iter.take(i(), 1)).not.toResultEqual(0);
                expect(iter.take(i(), 1)).not.toResultEqual(1);
            });
            test ("take 2", () => {
                const i = function * () { yield 0; yield 1; return 17; }
                expect(iter.take(i(), 2)).toResultEqual(undefined);
                expect(iter.take(i(), 2)).not.toResultEqual(17);
                expect(iter.take(i(), 2)).not.toResultEqual(0);
                expect(iter.take(i(), 2)).not.toResultEqual(1);
            });
            test ("take 3", () => {
                const i = function * () { yield 1; yield 1; return 17; }
                expect(iter.take(i(), 3)).toResultEqual(17);
            });
        });
    });

    describe("drop", () => {
        test.each([0,1,2,3,4,5,6,7,8,9,10])("drop %i", (n) => {
            expect(iter.drop(t.values(), n)).toYieldEqual(seqArray(10-n, n));
        });
        test.each([0,1,2,3,4,5,6,7,8,9,10])("length of drop %i", (n) => {
            expect(iter.length(iter.drop(t.values(), n))).toBe(10-n);
        });
    })
    
    describe("equals", () => {
        test("equals [0,1,2]", () => {
            expect(iter.equals([0,1,2].values(), [0,1,2].values())).toBeTruthy();
        });
        test("equals with map", () => {
            expect(iter.equals(iter.map([0,1,2].values(), x => x + 1), [1,2,3].values())).toBeTruthy();
        });
        test("equals with filter", () => {
            expect(iter.map(iter.filter([0,1,2].values(), x => x > 0), [1,2].values())).toBeTruthy();
        });
        test("equals with take", () => {
            expect(iter.map(iter.take([0,1,2].values(), 2), [0,1].values())).toBeTruthy();
        });
        test("equals with drop", () => {
            expect(iter.map(iter.drop([0,1,2].values(), 2), [2].values())).toBeTruthy();
        });
        test("equals []", () => {
            expect(iter.equals([].values(), [].values())).toBeTruthy();
        });
        test.each([[[]], [[1]], [[0,1]], [[1,2]], [[1,2,3]], [[0,1,2,3,4]], [[2,1,0]]])("iterator equals fails for [1,2,3] and %p", (b) => {
            expect(iter.equals([0,1,2].values(), b.values())).toBeFalsy();
        });
    });

    describe("length", () => {
        test.each([0,1,2,3])("length of iteration with %i elements", (n) => {
            expect(iter.length(iter.seq(0,n-1))).toBe(n);
        });
        test("length of filtered iteration", () => {
            expect(iter.length(iter.filter(iter.seq(0,4),x => x > 0))).toBe(4);
        });
        test("length of mapped iteration", () => {
            expect(iter.length(iter.map(iter.seq(0,4),x => x + 1))).toBe(5);
        });
    });

    describe("filter", () => {
        test("filter true", () => {
            expect(iter.filter(t.values(), x => true)).toYieldEqual(t);
        });
        test("filter false", () => {
            expect(iter.filter(t.values(), x => false)).toYieldEqual([]);
            expect(iter.filter(t.values(), x => false)).not.toYieldEqual(t);
        });
        test("filter even", () => {
            expect(iter.filter(t.values(), x => x % 2 == 0)).toYieldForall(x => x % 2 == 0);
        });
        describe("result", () => {
            test ("predicate false", () => {
                const i = function * () { yield 0; yield 1; return 17; }
                expect(iter.filter(i(), _x => false)).toResultEqual(17);
            });
            test ("predicate x >= 1", () => {
                const i = function * () { yield 0; yield 1; return 17; }
                expect(iter.filter(i(), x => x >= 1)).toResultEqual(17);
            });
        });
    });

    describe("map", () => {
        test("map identity", () => {
            expect(iter.map(t.values(), x => x)).toYieldEqual(t);
        });
        test("map +1", () => {
            expect(iter.map(t.values(), x => x+1)).toYieldEqual(t.map(x => x + 1));
        });
        test("map throw over []", () => {
            expect(iter.map([].values(), x => {throw "must not happen";})).toYieldEqual([]);
        });
        test ("map result", () => {
            const i = function * () { yield 0; yield 1; return 17; }
            expect(iter.map(i(), x => x + 1)).toResultEqual(17);
        });
    });

    describe("drain", () => {
        test("empty after drain", () => {
            const r = t.values();
            iter.drain(r);
            expect(r).toYieldEqual([]);
        });
        test ("drain result", () => {
            const i = function * () { yield; yield; return 17; }
            const result = iter.drain(i());
            expect(result).toBe(17);
        })
    });

    describe("result", () => {
        test("replace result", () => {
            const i = function * () { yield 0; yield 1; return 17; }
            expect(iter.result(i(), 18)).not.toResultEqual(17);
            expect(iter.result(i(), 18)).toResultEqual(18);
        });
        test("add result", () => {
            expect(iter.result(t.values(), 18)).toResultEqual(18);
        });
    });

    describe("fmap", () => {
        test("identity", () => {
            const i = function * () { yield 0; yield 1; return 17; }
            expect(iter.fmap(i(), x => x)).toResultEqual(17);
        });
        test("plus 1", () => {
            const i = function * () { yield 0; yield 1; return 17; }
            expect(iter.fmap(i(), x => x + 1)).toResultEqual(18);
            expect(iter.fmap(i(), x => x + 1)).not.toResultEqual(17);
        });
        test("undefined", () => {
            expect(iter.fmap(t.values(), x => x)).toResultEqual(undefined);
        });
    });

    describe("Iterator", () => {
        test("Iterator to array", () => {
            expect([1,2,3]).toEqual([1,2,3]);
            expect(Array.from(new iter.Iterator([1,2,3].values()))).toEqual([1,2,3]);
        });
        test("It", () => {
            const r = iter.It(t.values());
            expect(r).toYieldEqual(t);
        });
        test("take", () => {
            const r = iter.It(t.values()).take(4);
            expect(r).toYieldEqual([0,1,2,3]);
        });
        test("drain", () => {
            const r = iter.It(t.values());
            r.drain();
            expect(r).toYieldEqual([]);
        });
        test("iterator chain", () => {
            const r = iter.It(t.values()).drop(1).take(4).map(x => x + 1).filter(x => x % 2 == 0);
            expect(r).toYieldEqual([2,4]);
        });
        test("lenght of iterator chain", () => {
            const r = iter.It(t.values()).drop(1).take(4).map(x => x + 1).filter(x => x % 2 == 0);
            expect(r.length()).toBe(2);
        })
        test("equality of iterator chain", () => {
            const r = iter.It(t.values()).drop(1).take(4).map(x => x + 1).filter(x => x % 2 == 0);
            expect(r.equals([2,4].values())).toBeTruthy();
        })
        test("inequality of iterator chain", () => {
            const r = iter.It(t.values()).drop(1).take(4).map(x => x + 1).filter(x => x % 2 == 0);
            expect(r.equals([4,2].values())).toBeFalsy();
        })
    });
});

describe("async iterator", () => {
    const i = async function * () { 
        yield await sleep(10).then(x => 0); 
        yield await sleep(10).then(x => 1); 
        return await sleep(10).then(x => 2); 
    }
    test ("next", async () => {
        const i2 = i();
        await expect(await i2.next()).toEqual({ done: false, value: 0 });
        await expect(await i2.next()).toEqual({ done: false, value: 1 });
        await expect(await i2.next()).toEqual({ done: true, value: 2 });
    });

    describe("take", () => {
        test ("0/2", async () => {
            const i2 = asyncIter.take(i(),0);
            await expect(await i2.next()).toEqual({ done: true, value: undefined });
        });
        test ("1/2", async () => {
            const i2 = asyncIter.take(i(),1);
            await expect(await i2.next()).toEqual({ done: false, value: 0 });
            await expect(await i2.next()).toEqual({ done: true, value: undefined });
        });
        test ("2/2", async () => {
            const i2 = asyncIter.take(i(),2);
            await expect(await i2.next()).toEqual({ done: false, value: 0 });
            await expect(await i2.next()).toEqual({ done: false, value: 1 });
            await expect(await i2.next()).toEqual({ done: true, value: undefined });
        });
        test ("3/2", async () => {
            const i2 = asyncIter.take(i(),3);
            await expect(await i2.next()).toEqual({ done: false, value: 0 });
            await expect(await i2.next()).toEqual({ done: false, value: 1 });
            await expect(await i2.next()).toEqual({ done: true, value: 2 });
        });
    });

    describe("drop", () => {
        test ("0/2", async () => {
            const i2 = asyncIter.drop(i(), 0);
            await expect(await i2.next()).toEqual({ done: false, value: 0 });
            await expect(await i2.next()).toEqual({ done: false, value: 1 });
            await expect(await i2.next()).toEqual({ done: true, value: 2 });
        });
        test ("1/2", async () => {
            const i2 = asyncIter.drop(i(), 1);
            await expect(await i2.next()).toEqual({ done: false, value: 1 });
            await expect(await i2.next()).toEqual({ done: true, value: 2 });
        });
        test ("2/2", async () => {
            const i2 = asyncIter.drop(i(), 2);
            await expect(await i2.next()).toEqual({ done: true, value: 2 });
        });
        test ("3/2", async () => {
            const i2 = asyncIter.drop(i(), 3);
            await expect(await i2.next()).toEqual({ done: true, value: undefined });
        });
    });

    describe("equals", () => {
        test("self", async () => {
            await expect(asyncIter.equals(i(), i())).resolves.toBe(true);
        });
        test("not equals", async () => {
            const i2 = i();
            await expect(asyncIter.equals(i(), asyncIter.drop(i(),1))).resolves.toBe(false);
        });
    });

    describe("for async ... of", () => {
        test ("iterator", async () => {
            const i2 = i();
            let j = 0;
            for await (let x of i2) { expect(x).toBe(j); j++; }
        });
        test.each([0,1,2]) ("take %i", async (n) => {
            const i2 = asyncIter.take(i(), n);
            let j = 0;
            for await (let x of i2) { expect(x).toBe(j); j++; }
            expect(j).toBe(n);
        });
        test.each([0,1,2]) ("drop %i", async (n) => {
            const i2 = asyncIter.drop(i(), n);
            let j = n;
            for await (let x of i2) { expect(x).toBe(j); j++; }
            expect(j).toBe(2);
        });
    });
});
