const { formatBytes, rocksDbDirSize } = require("./test/test-utils");
const cdb = require("./src/ChainDb");
const { asyncIter } = require('./src/Utils');


/* ************************************************************************** */
/* Prune old cuts */

async function pruneOldCuts (dbDir, depth) {
    dbPath = dbDir ?? "/db/0/rocksDb";
    const db = new cdb.ChainDb(dbPath);

    const showSize = () => {
        console.log(`dir size: ${formatBytes(rocksDbDirSize(dbPath))}`);
    }

    showSize();
    await db.open();
    try {
        const tbl = db.cutTable;

        // size
        console.log(`approximate cut table size ${formatBytes(await tbl.approximateSize())}`);

        // prune cuts
        const cur = await tbl.last();
        const curHeight = cur.value.height;
        const curHeight0 = cur.value.hashes[0].height;
        const start = 0n;
        const end = BigInt(curHeight) - BigInt(depth) * 20n;

        console.log(`current height of chain 0: ${curHeight}`)
        console.log(`pruning ${start} to ${curHeight0 - depth} (cut height: ${end})`);
        await tbl.clear({height: start}, {height: end});
        console.log("finished prunning")

        // show size
        showSize();
        console.log(`approximate cut table size ${formatBytes(await tbl.approximateSize())}`);

        // compact (this only triggers compaction, but doesn't await it to be finished)
        console.log("triggering compaction of cut table")
        await tbl.compact();
        console.log("triggered compaction of cut table")

        // show size
        console.log(`approximate cut table size ${formatBytes(await tbl.approximateSize())}`);
    } finally {
        await db.close();
        console.log("db closed")
    }
    showSize();
}

/* ************************************************************************** */
/* module exports */

module.exports = pruneOldCuts;
