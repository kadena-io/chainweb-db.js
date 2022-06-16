const { tmpdir, endianness } = require('os');
const { mkdtemp, rmdir, fstat } = require('fs');
const fs = require('fs');
const path = require('path');

const cdb = require("../src/ChainDb");
const { asyncIter } = require('../src/Utils');


/* ************************************************************************** */
/* misc */

const formatBytesKB = (c) => `${(c / 1024).toFixed(2)} KiB`;
const formatBytesMB = (c) => `${(c / (1024*1024)).toFixed(2)} MiB`;
const formatBytesGB = (c) => `${(c / (1024*1024*1024)).toFixed(2)} GiB`;

function formatBytes (c) {
    if (c < 1024) {
        return `${c.toFixed(2)} bytes`
    } else if (c < 1024**1) {
        return `${(c / 1024**1).toFixed(2)} KiB`
    } else if (c < 1024**2) {
        return `${(c / 1024**2).toFixed(2)} MiB`
    } else if (c < 1024**3) {
        return `${(c / 1024**3).toFixed(2)} GiB`
    } else if (c < 1024**4) {
        return `${(c / 1024**4).toFixed(2)} TiB`
    } else {
        return `${(c / 1024**5).toFixed(2)} PiB`
    }
}

/* ************************************************************************** */
/* Temporary Directories */

const tmpDir = tmpdir();

function mkTempDir () {
    return new Promise((resolve,reject) => {
        mkdtemp(`${tmpDir}${path.sep}`, (err, directory) => {
            if (err) {
                reject(err); 
            } else {
                resolve(directory);
            }
        });
    });
};

function rmDir (path) {
    return new Promise((resolve, _reject) => {
        rmdir(path, IO => resolve());
    });
}

/* ************************************************************************** */
/* Directory sizes (non-recursive) */

function rocksDbDirSize (dir) {
    const dbFiles = fs.readdirSync(dir).map(x => path.join(dir, x));
    let totalSize = 0;
    dbFiles.forEach(f => {
        totalSize += fs.statSync(f).size
    });
    return totalSize;
}

/* ************************************************************************** */
/* Create Test database */

/* Copies data from a production database into a test database */

const someKeyCount = 50n;
const start = 1800000n;

async function createTestDb (mainnetDbPath) {
    mainnetDbPath = mainnetDbPath ?? "../chainweb-master/tmp/mainnet/db/0/rocksDb";
    const mainnetDb = new cdb.ChainDb(mainnetDbPath);

    const testDbPath = "./test/data/db";
    const testDb = new cdb.ChainDb(testDbPath);

    let curSize = 0;
    const showSizeDiff = () => {
        const newSize = rocksDbDirSize(testDbPath);
        console.log(`dir size change: ${formatBytes(newSize - curSize)} KB (total dir size: ${newSize} KB)`);
        curSize = newSize;
    }

    await Promise.all([
        mainnetDb.open(),
        testDb.open(),
    ]);
    
    try {
        // copy info table
        {
            console.log("copying info table")
            const it = mainnetDb.infoTable.iterator();
            try {
                for await (h of it) {
                    console.log(h);
                    await testDb.infoTable.put(h.key, h.value);
                }
            } finally {
                it.end();
            }
        }
        showSizeDiff()

        // Populate cut table
        {
            console.log("populating cut table")
            const it = mainnetDb.cutTable.iterator();
            await it.seek({height: 20n*start});
            try {
                let i = 0
                for await (h of it) {
                    if (h.key.height > 20n*start + someKeyCount*20n) {
                        break;
                    }
                    await testDb.cutTable.put(h.key, h.value);
                    ++i;
                }
                console.log(`Added ${i} cuts`)
            } finally {
                it.end();
            }
        }
        showSizeDiff();

        // Populate header and payload tables
        for (let cid=0; cid < 20; ++cid) {
            console.log(`chain ${cid}: populating header and payload tables on chain`)
            const sourceHeaderTable = mainnetDb.headerTable(cid);
            const sourcePayloadTable = mainnetDb.payloadWithOutputsTable(cid);
            const targetHeaderTable = testDb.headerTable(cid);
            const targetPayloadTable = testDb.payloadWithOutputsTable(cid);

            const it = sourceHeaderTable.iterator({limit: someKeyCount});
            await it.seek({height: start});
            try {
                let j = 0;
                for await (h of asyncIter.take(it, someKeyCount)) {
                    const payloadHash = h.value.payloadHash;
                    const payload = await sourcePayloadTable.get(payloadHash);
                    await targetPayloadTable.put(payloadHash, payload);
                    await targetHeaderTable.put(h.key, h.value);
                    j++;
                }
                console.log(`chain ${cid}: Added ${j} blocks`);
            } finally {
                it.end();
            }
            showSizeDiff();
        }
        showSizeDiff();

        // count entries in cuts table
        const cutCount = await asyncIter.length(t.iterator());
        console.log(`Number of entries in cuts table: ${cutCount}`);

    } finally {
        mainnetDb.close();
        testDb.close();
        console.log("closed dbs");
    }
}

/* ************************************************************************** */

module.exports = {
    mkTempDir: mkTempDir,
    rmDir: rmDir,
    rocksDbDirSize: rocksDbDirSize,
    createTestDb: createTestDb,
    formatBytes: formatBytes,
    formatBytesKB: formatBytesKB,
    formatBytesMB: formatBytesMB,
    formatBytesGB: formatBytesGB,
}