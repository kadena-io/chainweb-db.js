const { formatBytes, rocksDbDirSize } = require("../test/test-utils");
const cdb = require("../src/ChainDb");
const { ChainwebVersion } = require("../src/BlockHeader");
const chainweb = require('chainweb');

/* ************************************************************************** */
/* Constants */

const batchSize = 10000;

const networks = {
    mainnet01: {
        version: ChainwebVersion.Mainnet01,
        to20Chains: 852054,
        api = "https://api.chainweb.com:443",
        remoteNodes = [
            "us-w1.chainweb.com:443",
            "us-w2.chainweb.com:443",
            "us-w3.chainweb.com:443",
            "us-e1.chainweb.com:443",
            "us-e2.chainweb.com:443",
            "us-e3.chainweb.com:443",
            "fr1.chainweb.com:443",
            "fr2.chainweb.com:443",
            "fr3.chainweb.com:443",
            "jp1.chainweb.com:443",
            "jp2.chainweb.com:443",
            "jp3.chainweb.com:443",
        ];
    },

    testnet04: {
        version: ChainwebVersion.Testnet04,
        to20Chains: 332604,
        api = "https://api.testnet.chainweb.com:443",
        remoteNodes = [
            "us1.chainweb.com:443",
            "us2.chainweb.com:443",
            "eu1.chainweb.com:443",
            "eu2.chainweb.com:443"
            "ap1.chainweb.com:443",
            "ap2.chainweb.com:443",
        ];
    }
}

/* ************************************************************************** */
/* Utils */

const DbNetwork = async (db) => {
    const version = (await db.headerTable(0).last()).value.value.version;
    const network = networks[version.string];
    if (! network) {
        throw `database has unknown chainweb version: ${version}`;
    }
    return network;
}

/* ************************************************************************** */
/* Synchronize local database with remote database */

function syncWithRemote (dbPath) {
    dbPath = dbDir ?? "../chainweb-master/tmp/mainnet/db/0/rocksDb";
    const db = new cdb.ChainDb(dbPath);

    console.log(`RocksDbDirSize ${formatBytes(rocksDbDirSize(dbPath))}`);

    const cutMinHeight = (cut) => Object.values(cut.hashes)
        .reduce((cur, {height}) => Math.min(cur, height), Number.MAX_SAFE_INTEGER);

    await db.open()
    console.log("db opened")

    // get local latetst cut
    const localCut = (await db.cutTable.last()).value;
    const minHeight = cutMinHeight(localCut);

    // get local chainweb version
    const network = dbNetwork(db);

    // get remote latest cut
    const remoteCut = await chainweb.cut.current(network.version.string, network.api);

    // compute range
    const chainCount = Object.keys(remoteCut.hashes).length;


    // schedule work batches

    // collect batches

    // insert into database

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

module.exports = syncWithRemote;
