const { TestWatcher } = require("jest");
const cdb = require("../src/ChainDb");
const { HeaderView, SHA256HashView } = require("../src/BlockHeader");
const { asyncIter } = require("../src/Utils");

/* ************************************************************************** */
/* Test Utils */

function compareDbKeys (table, key1, key2, comp) {
    if (comp) {
        return comp(table.dbKey(key1), table.dbKey(key2));
    } else {
        return table.dbKey(key1).compare(table.dbKey(key2));
    }
}

async function someKeys(tbl, n) {
    const it = tbl.iterator();
    try {
        const i = asyncIter.It(tbl.iterator()).map(x => x.key).drop(10).take(n);
        let a = [];
        for await (x of i) {
            a.push(x);
        }
        return a;
    } finally {
        it.end();
    }
}

/* ************************************************************************** */
/* Test Setup */

const dbPath = "./test/data/db/"

const db = new cdb.ChainDb(dbPath);
const tables = [
    { table: db.rankTable(0) },
    { table: db.headerTable(0) },
    { table: db.cutTable },
    { table: db.blockPayloadTable() },
    { table: db.blockTransactionsTable() },
    { table: db.blockOutputsTable() },
    { table: db.outputTreeTable() },
    { table: db.transactionTreeTable() },
    { table: db.payloadDataTable(), dbKeyComp: (key0, key1) => key0.BlockPayload.compare(key1.BlockPayload)},
    { table: db.payloadWithOutputsTable(), dbKeyComp: (key0, key1) => key0.PayloadData.compare(key1.PayloadData)},
];
const someKeyCount = 5;

beforeAll(async () => {
    await db.open({ readOnly: true });
    console.log('db opened')
    await Promise.all(tables.map(async t => {
        t.someKeys = await someKeys(t.table, someKeyCount);
    }));
});

afterAll(async () => {
    await db.close();
    console.log('db closed')
});

/* ************************************************************************** */
/* Test Setup */

describe("Test setup", () => {
    test.each(tables)("$table.name: some keys are available", (table) => {
        expect(table.someKeys.length).toBe(someKeyCount);
    });
});

/* ************************************************************************** */
/* Tests */

describe("get", () => {
    test.each(tables)("$table.name: get some keys", async (table) => {
        for (k of table.someKeys) {
            expect(await table.table.get(k)).toBeTruthy();
        }
    });
});

describe("first and last", () => {
    test.each(tables)("$table.name: first returns a value", async ({table}) => {
        expect(await table.first()).toBeTruthy();
    });
    test.each(tables)("$table.name: firstKey and firstValue same values as first", async ({table}) => {
        const x = await table.first()
        expect(await table.firstKey()).toEqual(x.key);
        expect(await table.firstValue()).toEqual(x.value);
    });
    test.each(tables)("$table.name: last returns a value", async ({table}) => {
        expect(await table.last()).toBeTruthy();
    });
    test.each(tables)("$table.name: lastKey and lastValue same values as first", async ({table}) => {
        const x = await table.last()
        expect(await table.lastKey()).toEqual(x.key);
        expect(await table.lastValue()).toEqual(x.value);
    });
    test.each(tables)("$table.name: key of last is larger than first", async (tbl) => {
        const table = tbl.table;
        const f = await table.first();
        const l = await table.last();
        expect(f.value).toBeTruthy();
        expect(l.value).toBeTruthy();
        expect(compareDbKeys(table, f.key, l.key, tbl.dbKeyComp)).toBe(-1);
    });
});

describe("seek", () => {
    test.each(tables)("$table.name: next after seek increases key", async (tbl) => {
        const table = tbl.table;
        const it = table.iterator();
        try {
            await it.seek();
            const {value, done} = await it.next();
            expect(done).toBe(false);
            expect(compareDbKeys(table, value.key, undefined, tbl.dbKeyComp)).toBe(1);
        } finally {
            it.end();
        }
    });
    test.each(tables)("$table.name: seek('') is first", async ({table}) => {
        const it = table.iterator();
        try {
            await it.seek();
            const {ekey, evalue, done} = await it.next();
            const {rkey, rvalue} = await table.first();
            expect(ekey).toEqual(rkey);
            expect(evalue).toEqual(rvalue);
        } finally {
            it.end();
        }
    });
});
