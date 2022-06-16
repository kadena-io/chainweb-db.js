const { Db, IntKeyIntValueTable, StringKeyJsonValueTable, CompositeTable } = require('../src/Table');
const { mkTempDir, rmDir } = require('./test-utils');

/* ************************************************************************** */
/* Setup Test Database */

let dbDir;
let db;
let tbl;
let tbl2;

const IntTable = IntKeyIntValueTable;

beforeAll(async () => {
    dbDir = await mkTempDir();
    db = new Db(dbDir, "testDb")
    await db.open();
    tbl = new IntTable(db, "testTable");
    tbl2 = new IntTable(db, "noiseTable");
    console.log(`created test database at ${dbDir}`);
});

afterAll(async () => {
    await db.close();
    await rmDir(dbDir);
    console.log(`deleted test database at ${dbDir}`);
});

beforeEach(async () => {
    await tbl2.put(0, 10);
    await tbl2.put(1, 11);
    await tbl2.put(2, 12);
    await tbl2.put(-1, -11);
    await tbl2.put(-2, -12);
});

afterEach(async () => {
    await db.clear();
});

/* ************************************************************************** */
/* Test Setup Tests */

describe("test setup", () => {
    test("test db roundtrip", async () => {
        await tbl.put(1, 10);
        expect(tbl.get(1)).resolves.toEqual(10);
        expect(tbl.get(2)).rejects.toThrow('NotFound: ');
    });
    test("test db cleanup", async () => {
        expect(tbl.get(1)).rejects.toThrow('NotFound: ');
    });
});

/* ************************************************************************** */
/* StringKeyJsonValueTable */

describe("StringKeyJsonValueTable", () => {
    test("get put roundtrip", async () => {
        const jsonTbl = new StringKeyJsonValueTable(db, "StringKeysonValueTable");
        try {
            await jsonTbl.put("", null);
            await jsonTbl.put("a", 10);
            await jsonTbl.put("b", "c");
            await jsonTbl.put("c", []);
            await jsonTbl.put("cc", [1,2,3]);
            await jsonTbl.put("d", {});
            await jsonTbl.put("dd", {"a": [], "b": {}, "c": "x", "d": 1.1});
            expect(await jsonTbl.get("")).toEqual(null);
            expect(await jsonTbl.get("a")).toBe(10);
            expect(await jsonTbl.get("b")).toEqual("c");
            expect(await jsonTbl.get("c")).toEqual([]);
            expect(await jsonTbl.get("cc")).toEqual([1,2,3]);
            expect(await jsonTbl.get("d")).toEqual({});
            expect(await jsonTbl.get("dd")).toEqual({"a": [], "b": {}, "c": "x", "d": 1.1});
        } finally {
            jsonTbl.clear();
        }
    });
});

/* ************************************************************************** */
/* IntKeyIntValueTable */

describe ("IntTable", () => {

    test("get put roundtrip", async () => {
        await tbl.put(1, 10);
        expect(await tbl.get(1)).toEqual(10);
    });
    test("clear", async () => {
        await tbl.put(1, 10);
        await tbl.put(2, 11);
        expect(await tbl.get(1)).toEqual(10);
        expect(await tbl.get(2)).toEqual(11);
        expect(await tbl.clear()).toEqual();
        expect(tbl.get(1)).rejects.toThrow("NotFound: ");
        expect(tbl.get(2)).rejects.toThrow("NotFound: ");
        expect(await tbl2.get(0)).toEqual(10);
        expect(await tbl2.get(1)).toEqual(11);
        expect(await tbl2.get(2)).toEqual(12);
        expect(await tbl2.get(-1)).toEqual(-11);
        expect(await tbl2.get(-2)).toEqual(-12);
    });
    test("empty last", async () => {
        expect(tbl.last()).rejects.toEqual("last() called on empty iteration");
    });
    test("last", async () => {
        await tbl.put(1, 10);
        await tbl.put(2, 11);
        expect(await tbl.last()).toEqual({key: 2, value: 11});
    });
    test("empty first", async () => {
        expect(tbl.first()).rejects.toEqual("first() called on empty iteration");
    });
    test("first", async () => {
        await tbl.put(1, 10);
        await tbl.put(2, 11);
        expect(await tbl.first()).toEqual({key: 1, value: 10});
    });
    describe("iterator", () => {
        test("0 entries", async () => {
            const it = tbl.iterator();
            expect(await it.next()).toEqual({done: true});
            expect(await it.end()).toEqual();
        });
        test("0 entries reverse", async () => {
            const it = tbl.reverseIterator();
            expect(await it.next()).toEqual({done: true});
            expect(await it.end()).toEqual();
        });
        test("1 entry", async () => {
            await tbl.put(1, 10);
            const it = tbl.iterator();
            expect(await it.nextValue()).toEqual({done: false, value: 10});
            expect(await it.next()).toEqual({done: true});
            expect(await it.next()).not.toEqual({done: false});
            expect(await it.end()).toEqual();
        });
        test("1 entry reverse", async () => {
            await tbl.put(1, 10);
            const it = tbl.reverseIterator();
            expect(await it.nextValue()).toEqual({done: false, value: 10});
            expect(await it.next()).toEqual({done: true});
            expect(await it.next()).not.toEqual({done: false});
            expect(await it.end()).toEqual();
        });
        test("2 entries", async () => {
            await tbl.put(1, 10);
            await tbl.put(2, 11);
            const it = tbl.iterator();
            expect(await it.nextValue()).toEqual({done: false, value: 10});
            expect(await it.nextValue()).toEqual({done: false, value: 11});
            expect(await it.next()).toEqual({done: true});
            expect(await it.next()).not.toEqual({done: false});
            expect(await it.end()).toEqual();
        });
        test("2 entries reverse", async () => {
            await tbl.put(1, 10);
            await tbl.put(2, 11);
            const it = tbl.reverseIterator();
            expect(await it.nextValue()).toEqual({done: false, value: 11});
            expect(await it.nextValue()).toEqual({done: false, value: 10});
            expect(await it.next()).toEqual({done: true});
            expect(await it.next()).not.toEqual({done: false});
            expect(await it.end()).toEqual();
        });
        test("end", async () => {
            const it = tbl.iterator();
            expect(it.end()).resolves.toEqual();
            expect(it.end()).rejects.toThrow();
            expect(it.next()).rejects.toThrow();
        });
        test("seek", async () => {
            await tbl.put(1, 10);
            await tbl.put(2, 11);
            const it = tbl.iterator();
            await it.seek(2);
            expect(await it.nextValue()).toEqual({done: false, value: 11});
            expect(await it.next()).toEqual({done: true});
            expect(await it.end()).toEqual();
        });
        test("seek reverse", async () => {
            await tbl.put(1, 10);
            await tbl.put(2, 11);
            const it = tbl.reverseIterator();
            await it.seek(1);
            expect(await it.nextValue()).toEqual({done: false, value: 10});
            expect(await it.next()).toEqual({done: true});
            expect(await it.end()).toEqual();
        });
    });
});


/* ************************************************************************** */
/* CompositeTable */

/* view: { k0, v0, k1, v1 }
 * db schema:
 * - t0: { k, v, k1 }
 * - t1: { k, v }
 * 
 * t0.k1 is foreign key for t1.
 */
class NormalizedTable extends CompositeTable {

    constructor() { 
        const t0 = new StringKeyJsonValueTable(db, "t0");
        const t1 = new StringKeyJsonValueTable(db, "t1");
        super("tn", [t0, t1]); 
        this.t0 = t0;
        this.t1 = t1;
    }

    dbKey (key, values) {
        let r = {};
        r.t0 = key;
        r.t1 = values?.t0?.k1
        return r;
    }

    fromDbKey (keys) { return keys.t0.k; }

    // t0.k1 is a foreign key for t1
    dbValue (value) {
        return { 
            t0: { k: value.k0, v: value.v0, k1: value.k1 },
            t1: { k: value.k1, v: value.v1 },
        };
    }

    fromDbValue (values) {
        return {
            k0: values.t0.k,
            v0: values.t0.v,
            k1: values.t1.k,
            v1: values.t1.v,
        };
    }
}

describe ("CompositeTable", () => {

    test("get put roundtrip", async () => {
        const t0 = new StringKeyJsonValueTable(db, "t0");
        const t1 = new StringKeyJsonValueTable(db, "t1");
        const t = new CompositeTable("t", [t0, t1]);

        const k = {t0: "0", t1: "1"};
        const v = {t0: 0, t1: 1};
        await t.put(k, v);
        expect(await t.get(k)).toEqual(v);
        expect(await t0.get('0')).toEqual(0);
        expect(await t1.get('1')).toEqual(1);
    });

    test("clear", async () => {
        const t0 = new StringKeyJsonValueTable(db, "t0");
        const t1 = new StringKeyJsonValueTable(db, "t1");
        const t = new CompositeTable("t", [t0, t1]);

        const k = {t0: "0", t1: "1"};
        const v = {t0: 0, t1: 1};
        await t.put(k, v);
        expect(await t.get(k)).toEqual(v);
        expect(await t0.get('0')).toEqual(0);
        expect(await t1.get('1')).toEqual(1);
        await t.clear();
        expect(t.get(k)).rejects.toThrow("NotFound: ");
        expect(t0.get('0')).rejects.toThrow("NotFound: ");
        expect(t1.get('1')).rejects.toThrow("NotFound: ");
    });

    describe("NormalizedTable", () => {
        test("get put roundtrip", async () => {
            const t = new NormalizedTable();
            const k = "0";
            const v = { k0: "0", v0: "v0", k1: "1", v1: ["v1"] };
            await t.put(k, v);
            expect(await t.get(k)).toEqual(v);
            expect(await t.t0.get('0')).toEqual({ k: "0", v: "v0", k1: "1" });
            expect(await t.t1.get('1')).toEqual({ k: "1", v: ["v1"]});
        });
    });
});