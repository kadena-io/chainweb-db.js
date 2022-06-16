const { formatBytes, rocksDbDirSize } = require("../test/test-utils");
const cdb = require("../src/ChainDb");

/* ************************************************************************** */

async function tableSizes (dbDir) {
    dbPath = dbDir ?? "../chainweb-master/tmp/mainnet/db/0/rocksDb";
    const db = new cdb.ChainDb(dbPath);

    console.log(`RocksDbDirSize ${formatBytes(rocksDbDirSize(dbPath))}`);

    await db.open()
    console.log("db opened")
    try {
        for await (t of db.baseTables) {
            const s = await t.approximateSize();
            console.log(`${t.name}: ${formatBytes(s)}`);
        }
        const total = await db.approximateSize();
        console.log(`total: ${formatBytes(total)}`);
    } finally {
        await db.close();
        console.log("db closed")
    }
}

/* ************************************************************************** */
/* exports */

module.exports = tableSizes;
