const cdb = require("../src/ChainDb");
const { asyncIter } = require('../src/Utils');

async function cuts (dbDir, depth) {

    const dbPath = dbDir ?? "/db/0/rocksDb";
    const db = new cdb.ChainDb(dbPath);

    await db.open({ readOnly: true });

    try {
        const tbl = db.cutTable;
        const cur = await tbl.last();
        const cut0 = await tbl.first();

	console.log(`first cut: ${cut0.key.height}, ${cut0.key.cutId}`);
	console.log(`current cut: ${cur.key.height}, ${cur.key.cutId}`);

        console.log(`first height of chain 0: ${cut0.value.hashes[0].height}`)
        console.log(`current height of chain 0: ${cur.value.hashes[0].height}`)

       const it = tbl.reverseIterator()
       try {
           const it2 = asyncIter.take(it, depth);
           for await (let x of it2) {
                console.dir(x.key);
           }
       } finally {
         it.end();
       }


    } finally {
        await db.close();
        console.log("db closed")
    }
}

const args = process.argv.slice(2);
const depth = Number(args[0]) ?? 1;
console.log(`depth: ${depth}`);
cuts(null, depth);
