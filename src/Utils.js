/* ************************************************************************** */
/* Utils */

/* Decode a base64Url string. Padding is optional.
 *
 * @param {string} str - base64Url encoded string
 * @returns {Buffer} decoded bytes
 */
function from64 (str) {
    return Buffer.from(str, 'base64url');
}

/* Encode a binary data as base64Url (without padding).
 *
 * @param {Buffer} buf - binary data
 * @returns {string} base64Url (without padding) encoded data
 */
function to64 (buf) {
    return buf.toString('base64url');
}

/* Decode a base64Url encoded JSON string
 *
 * @param {string} str - base64Url encoded JSON string
 * @returns {any} a javascript value
 */
function from64Json (str) {
    return JSON.parse(from64(str));
}

/* Encode a javascript object as base64Url (without padding) encoded JSON
 * string.
 *
 * @param {*} value - A javascript value that can be encoded as JSON
 * @returns {string} base64url (without padding) encoded data
 */
function to64Json (value) {
    return Buffer.from(JSON.stringify(value)).toString('base64url');
}

/* Pause execution for some time
 *
 * @param {number} ms - Number of milliseconds that the execution is paused
 */
async function sleep(ms) {
    return new Promise((resolve, _reject) => setTimeout(resolve,ms));
}

/* ************************************************************************** */
/* Iterator Utils */

class Iterator {
    constructor(iterator) {
        this.iterator = iterator;
    }

    [Symbol.iterator]() { return this; }

    next () {
        return this.iterator.next();
    }

    seek (key) {
        return trySeek(this.iterator, key);
    }

    /* Methods */

    // TODO: would it be better to always return this
    // and only update the wrapped iterator?

    take (n) {
        return new Iterator(take(this.iterator, n));
    }

    drop (n) {
        return new Iterator(drop(this.iterator, n));
    }

    filter (fn) {
        return new Iterator(filter(this.iterator, fn));
    }

    map (fn) {
        return new Iterator(map(this.iterator, fn));
    }

    result (r) {
        return new Iterator(result(this.iterator, r));
    }

    fmap (fn) {
        return new Iterator(fmap(this.iterator, fn));
    }

    drain () {
        return drain(this.iterator);
    }

    length () {
        return length(this.iterator);
    }

    equals (other) {
        return equals(this.iterator, other);
    }
}

function It (i) {
    return new Iterator(i);
}

class AsyncIterator {
    constructor(iterator) {
        this.iterator = iterator;
    }

    [Symbol.asyncIterator]() { return this; }

    async next () {
        return await this.iterator.next();
    }

    async seek (key) {
        return await trySeek(this.iterator, key);
    }

    /* Methods */

    // TODO: would it be better to always return this
    // and only update the wrapped iterator?

    take (n) {
        return new AsyncIterator(asyncTake(this.iterator, n));
    }

    drop (n) {
        return new AsyncIterator(asyncDrop(this.iterator, n));
    }

    filter (fn) {
        return new AsyncIterator(asyncFilter(this.iterator, fn));
    }

    map (fn) {
        return new AsyncIterator(asyncMap(this.iterator, fn));
    }

    result (r) {
        return new AsyncIterator(asyncResult(this.iterator, r));
    }

    fmap (fn) {
        return new AsyncIterator(asynFmap(this.iterator, fn));
    }

    async drain () {
        return await asyncDrain(this.iterator);
    }

    async length () {
        return await asyncLength(this.iterator);
    }

    async equals (other) {
        return await asyncEquals(this.iterator, other);
    }
}

function AsyncIt (i) {
    return new AsyncIterator(i);
}

function trySeek(it, key) {
    if (it.seek) {
        return it.seek(key);
    } else {
        throw "iterator doesn't implement seek"
    }
}

function tryEnd(it) {
    if (it.end) {
        return it.end();
    } else {
        throw "iterator doesn't implement end"
    }
}

/* Produces the first n elements of an iterator. Returns a result
 * only if the iterator has less than n elements.
 */
function take (iter, n) {
    return {
        i: 0,
        next: function (v) {
            if (this.i < n) {
                ++this.i;
                return iter.next(v);
            } else {
                return {done: true}
            }
        },
        seek: (key) => trySeek(it, key),
        [Symbol.iterator]: function() { return this; },
    }
}

function asyncTake (iter, n) {
    return {
        i: 0,
        next: async function (v) {
            if (this.i < n) {
                ++this.i;
                return await iter.next(v);
            } else {
                return {done: true}
            }
        },
        seek: (key) => trySeek(it, key),
        [Symbol.asyncIterator]: function() { return this; },
    }
}

function drop (iter, n) {
    return {
        i: 0,
        next: function (v) {
            while (this.i < n) {
                iter.next(); // in the async case this wouldn't be awaited
                ++this.i;
            }
            return iter.next(v)
        },
        seek: (key) => trySeek(it, key),
        [Symbol.iterator]: function() { return this; },
    }
}

function asyncDrop (iter, n) {
    return {
        i: 0,
        next: async function (v) {
            while (this.i < n) {
                await iter.next();
                ++this.i;
            }
            return await iter.next(v)
        },
        seek: (key) => trySeek(it, key),
        [Symbol.asyncIterator]: function() { return this; },
    }
}

function filter (it, fn) {
    return {
        next: function (v) {
            const {value, done} = it.next(v);
            if (done) {
                return { done: true, value: value };
            } else if (fn(value)) {
                return { done: false, value: value };
            } else {
                return this.next();
            }
        },
        seek: (key) => trySeek(it, key),
        [Symbol.iterator]: function() { return this; }
    }
}

function asyncFilter (it, fn) {
    return {
        next: async function (v) {
            const {value, done} = await it.next(v);
            if (done) {
                return { done: true, value: value };
            } else if (fn(value)) {
                return { done: false, value: value };
            } else {
                return await this.next();
            }
        },
        seek: (key) => trySeek(it, key),
        [Symbol.asyncIterator]: function() { return this; }
    }
}

/* Maps over the yielded values of the iterator. It does not map the result.
 */
function map (it, fn) {
    return {
        next: function (v) {
            const {value, done} = it.next(v);
            if (done) {
                return { done: true, value: value};
            } else {
                return { done: false, value: fn(value)};
            }
        },
        seek: (key) => trySeek(it, key),
        [Symbol.iterator]: function() { return this; }
    }
}

function asyncMap (it, fn) {
    return {
        next: async function (v) {
            const {value, done} = await it.next(v);
            if (done) {
                return { done: true, value: value};
            } else {
                return { done: false, value: fn(value)};
            }
        },
        seek: (key) => trySeek(it, key),
        [Symbol.asyncIterator]: function() { return this; }
    }
}

function drain(it) {
    while (true) {
        const {value, done} = it.next();
        if (done) {
            return value;
        }
    }
}

async function asyncDrain(it) {
    while (true) {
        const {value, done} = await it.next();
        if (done) {
            return value;
        }
    }
}

/* Add result to a iterator. Replaces existing result. */
function * result(it, result) {
    yield* it;
    return result;
}

async function * asyncResult(it, result) {
    await (yield* it);
    return result;
}

/* map over the result of an iterator */
function * fmap(it, fn) {
    const r = yield* it;
    return fn(r);
}

/* Generators */
async function * asyncFmap(it, fn) {
    const r = await (yield* it);
    return fn(r);
}

function * seq(start, end, step) {
    const x = step ?? 1;
    let cur = start;
    while (cur <= end) {
        yield cur;
        cur += x;
    }
}

/* Accumulators */

function count(it) {
    let i = 0;
    return {
        next: function (v) {
            const {value, done} = it.next(v);
            if (done) {
                return { done: true, value: { result: value, count: i } };
            } else {
                return { done: false, value: value };
            }
        },
        seek: (key) => trySeek(it, key),
        [Symbol.iterator]: function() { return this; }
    }
}

function asyncCount(it) {
    let i = 0;
    return {
        next: async function (v) {
            const {value, done} = await it.next(v);
            if (done) {
                return { done: true, value: { result: value, count: i } };
            } else {
                return { done: false, value: value };
            }
        },
        seek: (key) => trySeek(it, key),
        [Symbol.asyncIterator]: function() { return this; }
    }
}

/* Aggregators */

function length(it) {
    let i = 0;
    for (const value of it) { ++i; }
    return i;
}

async function asyncLength(it) {
    let i = 0;
    for await (const value of it) { ++i; }
    return i;
}

function equals(a, b) {
    while (true) {
        const av = a.next();
        const bv = b.next();
        if (av.value != bv.value || av.done != bv.done) {
            return false;
        }
        if (av.done) {
            return true;
        }
    }
}

async function asyncEquals(a, b) {
    while (true) {
        const av = await a.next();
        const bv = await b.next();
        if (av.value != bv.value || av.done != bv.done) {
            return false;
        }
        if (av.done) {
            return true;
        }
    }
}

/* ************************************************************************** */
/* Exports */

module.exports = {
    from64: from64,
    from64Json: from64Json,
    to64: to64,
    to64Json: to64Json,
    sleep: sleep,

    /* iterators */
    iter: {
        take: take,
        drop: drop,
        length: length,
        filter: filter,
        map: map,
        drain: drain,
        equals: equals,
        seq: seq,
        result: result,
        fmap: fmap,
        count: count,
        Iterator: Iterator,
        It: It,
    },
    asyncIter: {
        take: asyncTake,
        drop: asyncDrop,
        length: asyncLength,
        filter: asyncFilter,
        map: asyncMap,
        drain: asyncDrain,
        equals: asyncEquals,
        result: asyncResult,
        fmap: asyncFmap,
        count: asyncCount,
        Iterator: AsyncIterator,
        It: AsyncIt,
    },
}