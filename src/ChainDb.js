const levelup = require("rocksdb");
const {from64, to64, asyncIter } = require("./Utils");
const {Db, Table, TableIterator, StringKeyJsonValueTable, JsonValueTable, CompositeTable} = require("./Table");
const { SHA256HashView, HeaderView, PoWHashView } = require("./BlockHeader");

/* TODO
 *
 * - Version
 * - Add virtual table to lookup headers by hash only
 * - Add virtual table for events with
 *     block height, reqKey, creationTime(?), event.name, event.module.hash, event.module.namespace, event.module.name, event.params
 *
 */

/* ************************************************************************** */
/* Table of JSON values that is indexed by SHA256 Hashes */

class SHA256HashViewKeyJsonValueTable extends JsonValueTable {
    dbKey (hash) {
        return (hash ?? SHA256HashView.NullHash).buffer;
    }
    fromDbKey (buf) {
        return new SHA256HashView(buf);
    }
}

/* ************************************************************************** */

/* Data is only decoded when there is a well-defined unique binary
 * representation. In particular:
 *
 * - Integral numbers are represented as big integers
 * - base64 encoded Hashes are decoded to binary
 * - JSON strings are parsed as Javascript values
 * - Base64 encoded JSON payload values are represented literally
 *
 */

class ChainDb extends Db {

    constructor (path, namespace) {
        super(path, namespace);
    }

    async chainDbVersion () {
        return await this.infoTable.getChainDbVersion();
    }

    /* Info Table (if it exists) */
    infoTable = new InfoTable(this);

    /* Cut Table */
    cutTable = new CutTable(this);

    /* Header Table */
    headerTable (cid) { return new BlockHeaderTable(this, cid); }

    /* Rank Table */
    rankTable (cid) { return new RankTable(this, cid); }

    /* Payload Tables */
    blockPayloadTable (cid) { return new BlockPayloadTable(this, cid); }
    blockTransactionsTable (cid) { return new BlockTransactionsTable(this, cid); }
    blockOutputsTable (cid) { return new BlockOutputsTable(this, cid); }
    transactionTreeTable (cid) { return new TransactionTreeTable(this, cid); }
    outputTreeTable (cid) { return new OutputTreeTable(this, cid); }

    /* Virtual Payload Tables */
    payloadWithOutputsTable (cid) { return new PayloadWithOutputsTable(this, cid); }
    payloadDataTable (cid) { return new PayloadDataTable(this, cid); }

    baseTables = [
        [
            this.infoTable,
            this.cutTable,
            this.blockPayloadTable(),
            this.blockTransactionsTable(),
            this.blockOutputsTable(),
            this.outputTreeTable(),
            this.transactionTreeTable(),
        ],
        new Array(20).fill().map((_, cid) => this.rankTable(cid)),
        new Array(20).fill().map((_, cid) => this.headerTable(cid)),
    ].flat(1);
}

/* ************************************************************************** */
/* Version table (if available) */

class InfoTable extends StringKeyJsonValueTable {
    constructor(db) { super(db, "Info"); }

    async getChainDbVersion () {
        try {
            return await this.get("chainDbVersion");
        } catch(e) {
            return 0;
        }
    }
}

/* ************************************************************************** */
/* Rank Table */

/* Schema:
 * - key: {BigInt}
 * - value: {SHA256HashView}
 */
class RankTable extends Table {
    constructor (db, cid) {
        super(db, `BlockHeader/${cid}/rank`);
        this.cid = cid;
    }

    fromDbValue (dbValue) {
        return dbValue.readBigUInt64LE(0);
    }

    dbValue (value) {
        let buf = Buffer.alloc(8);
        buf.writeBigUInt64LE(value);
        return buf;
    }

    dbKey (hash) {
        return (hash ?? SHA256HashView.NullHash).buffer;
    }

    fromDbKey (buf) {
        return new SHA256HashView(buf);
    }
}

/* ************************************************************************** */
/* Block Header Table (Binary) */

/* Schema:
 * - key: {{height: BigInt, hash: SHA256HashView }}
 * - value: {HeaderView}
 */
class BlockHeaderTable extends Table {

    constructor (db, cid) {
        super(db, `BlockHeader/${cid}/header`);
        this.cid = cid;
    }

    fromDbKey (dbkey) {
        return {
            height: dbkey.readBigUInt64BE(0),
            hash: new SHA256HashView(dbkey.slice(8)),
        };
    }

    /* hash can be null, which is usefull for iteration */
    dbKey (key) {
        const hash = key?.hash ?? SHA256HashView.NullHash;
        const height = key?.height ?? 0n;
        let buf = Buffer.alloc(40); // uint64 + 32 byte hash
        buf.writeBigUInt64BE(height);
        hash.buffer.copy(buf, 8);
        return buf;
    }

    dbValue (hdr) { return hdr.buffer; }
    fromDbValue (buf) { return new HeaderView(buf); }
}

/* ************************************************************************** */
/* Cut Hashes Table */

/* Schema:
 * - key: {{height: BigInt, weight: PoWHashView, cutId: SHA256HashView }}
 * - value: {Object} - JSON representation of CutHashes
 */
class CutTable extends JsonValueTable {
    constructor (db) { super(db, `CutHashes`); }

    dbKey (key) {
        const height = key?.height ?? 0n;
        const weight = key?.weight ?? PoWHashView.NullHash;
        const cutId = key?.cutId ?? SHA256HashView.NullHash;
        let buf = Buffer.alloc(72);
        buf.writeBigUInt64BE(height);
        weight.buffer.copy(buf, 8);
        cutId.buffer.copy(buf, 40);
        return buf;
    }

    fromDbKey (dbkey) {
        return {
            height: dbkey.readBigUInt64BE(0),
            weight: new PoWHashView(dbkey.slice(8, 40)),
            cutId: new SHA256HashView(dbkey.slice(40)),
        };
    }
}

/* ************************************************************************** */
/* Payload Tables */

class BlockPayloadTable extends SHA256HashViewKeyJsonValueTable {
    constructor (db, cid) { super(db, `BlockPayload`); this.cid = cid; }
}

class BlockTransactionsTable extends SHA256HashViewKeyJsonValueTable {
    constructor (db, cid) { super(db, `BlockTransactions`); this.cid = cid; }
}

class BlockOutputsTable extends SHA256HashViewKeyJsonValueTable {
    constructor (db, cid) { super(db, `BlockOutputs`); this.cid = cid; }
}

class TransactionTreeTable extends SHA256HashViewKeyJsonValueTable {
    constructor (db, cid) { super(db, `TransactionTree`); this.cid = cid; }
}

class OutputTreeTable extends SHA256HashViewKeyJsonValueTable {
    constructor (db, cid) { super(db, `OutputTree`); this.cid = cid; }
}

/* ************************************************************************** */
/* Composite Payload Tables */

/* Schema:
 * - key: @type {SHA256HashView}
 * - Value:
 *     @typedef PayloadData
 *     @type {object}
 *     @property payloadHash {SHA256HashView} - primary key,
 *     @property transactions {string[]} - Array of base64url encoded transactions JSON string,
 *     @property minerData {string} - base64url encoded miner data JSON string,
 *     @property transactionsHash {SHA256HashView},
 *     @property outputsHash {SHA256HashView},
 */
class PayloadDataTable extends CompositeTable {

    constructor(db, cid) {
        const pldTable = new BlockPayloadTable(db, cid);
        const txsTable = new BlockTransactionsTable(db, cid);
        super("PayloadData", [pldTable, txsTable]);
    }

    fromDbKey (dbKey) {
        return dbKey.BlockPayload;
    }

    dbKey (key, values) {
        let k = {}
        k.BlockPayload = key ?? SHA256HashView.NullHash;
        const th = values?.BlockPayload?.transactionsHash;
        if (th) {
            k.BlockTransactions = new SHA256HashView(from64(th));
        }
        return k;
    }

    fromDbValue (dbValue) {
        const pld = dbValue.BlockPayload
        const txs = dbValue.BlockTransactions;
        return {
            transactions: txs.transaction,
            minerData: txs.minerData,
            transactionsHash: new SHA256HashView(from64(pld.transactionsHash)),
            outputsHash: new SHA256HashView(from64(pld.outputsHash)),
            payloadHash: new SHA256HashView(from64(pld.payloadHash)),
        };
    }

    dbValue (value) {
        return {
            BlockPayload: {
                transactionsHash: to64(value.transactionsHash.buffer),
                outputsHash: to64(value.outputsHash.buffer),
                payloadHash: to64(value.payloadHash.buffer),
            },
            BlockTransactions: {
                transactionsHash: to64(value.transactionsHash.buffer),
                transaction: value.transactions,
                minerData: value.minerData,
            },
        };
    }

    iterator (opts) {
        const baseIt = this.tables.BlockPayload.iterator(opts);
        const it = super.iterator(asyncIter.map(baseIt, x => x.key));
        it.end = () => baseIt.end();
        it.seek = (key) => baseIt.seek(key);
        return it;
    }
}

/* ************************************************************************** */
/* Composite Payload With Outputs Table */


/* Schema:
 * - key: @type {SHA256HashView}
 * - Value:
 *     @typedef PayloadWithOutputs
 *     @type {object}
 *     @property payloadHash {SHA256HashView} - primary key,
 *     @property minerData {string} - base64url encoded miner data JSON string,
 *     @property transactionsHash {SHA256HashView},
 *     @property outputsHash {SHA256HashView},
 *     @property coinbase {string} - base64url encoded coinbase output JSON string
 *     @property transactions {{transaction: string, output: string}[]} - Array of base64url encoded transactions and outputs JSON string,
 */
class PayloadWithOutputsTable extends CompositeTable {
    constructor(db, cid) {
        const pldTable = new PayloadDataTable(db, cid);
        const outsTable = new BlockOutputsTable(db, cid);
        super("PayloadWithOutputs", [pldTable, outsTable]);
    }

    fromDbKey (dbKey) {
        return dbKey.PayloadData;
    }

    dbKey (key, values) {
        const k = {};
        k.PayloadData = key ?? SHA256HashView.NullHash;
        k.BlockOutputs = values?.PayloadData?.outputsHash;
        return k;
    }

    fromDbValue (dbValue) {
        const pd = dbValue.PayloadData;
        const outs = dbValue.BlockOutputs;
        return {
            minerData: pd.minerData,
            transactionsHash: pd.transactionsHash,
            outputsHash: pd.outputsHash,
            payloadHash: pd.payloadHash,
            coinbase: outs.coinbaseOutput,
            transactions: pd.transactions.map((t,i) => ({
                transaction: t,
                output: outs.outputs[i],
            })),
        };
    }

    dbValue (value) {
        return {
            PayloadData: {
                minerData: value.minerData,
                transactionsHash: value.transactionsHash,
                outputsHash: value.outputsHash,
                payloadHash: value.payloadHash,
                transactions: value.transactions.map(({transaction}) => transaction),
            },
            BlockOutputs: {
                outputsHash: to64(value.outputsHash.buffer),
                coinbaseOutput: value.coinbase,
                outputs: value.transactions.map(({output}) => output),
            },
        };
    }
    /*
    async getJson (hash) {
        const x = await this.get(hash);
        x.transactions = x.transactions.map((p) => { return { transaction: from64Json(p.transaction), output: from64Json(p.output)}; });
        x.minerData = from64Json(x.minerData);
        x.transactionsHash = from64(x.transactionsHash);
        x.outputsHash = from64(x.outputsHash);
        x.payloadHash = from64(x.payloadHash);
        x.coinbase = from64Json(x.coinbase);
        return x;
    }
    */

    iterator (opts) {
        // This is inefficient, doing to many redundant gets:
        // TODO: use BlockPayloadTable for iteration or fix double double fetching
        // of values.
        const baseIt = this.tables.PayloadData.iterator(opts);
        const it = super.iterator(asyncIter.map(baseIt, x => x.key));
        it.end = () => baseIt.end();
        it.seek = (key) => baseIt.seek(key);
        return it;
    }
}

/* ************************************************************************** */

class BlockHeaderWithPayloadWithOutputsTable {
    constructor(db, cid) {
        this.hdrTbl = new BlockHeaderTable(db, cid);
        this.powTbl = new PayloadWithOutputsTable(db, cid);
    }

    async get (hash) {
        const pld = await this.pldTbl.get(hash);
        let txhash = from64(pld.transactionsHash);
        let outhash = from64(pld.outputsHash);
        const [txs, outs, txTree, outTree] = await Promise.all([
            this.txsTbl.get(txhash),
            this.outsTbl.get(outhash),
            this.txTreeTbl.get(txhash),
            this.outTreeTbl.get(outhash)
        ]);
        return {
            transactions: [txs.transaction, outs.outputs],
            minerData: txs.minerData,
            transactionsHash: pld.transactionsHash,
            outputsHash: pld.outputsHash,
            payloadHash: pld.payloadHash,
            coinbase: outs.coinbaseOutput
        };
    };
}

/* ************************************************************************** */

module.exports = {
    ChainDb: ChainDb,
};
