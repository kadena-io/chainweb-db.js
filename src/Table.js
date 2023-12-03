const leveldown = require("rocksdb");
const {from64, from64Json} = require("./Utils");

/* ************************************************************************** */
/* RocksDb */

// TODO use levelup instead?
// should we provide Node.js streams for tables? It seems that ES iterators are
// better suited, but not sure.

class Db {

    constructor(path, namespace) {
        this.rocksDb = leveldown(path);
        this.namespace = namespace ?? "";

        this.gte = `${this.namespace}-`;
        this.lt = `${this.namespace}.`;
        this.gteBuf = Buffer.from(this.gte, 'ascii');
        this.ltBuf = Buffer.from(this.lt, 'ascii');
    }

    /* Internal Key Mapping */

    fromRocksDbKeyQualified (rkey) {
        const x = rkey.indexOf('-');
        return {
            namespace: rkey.slice(0,x).toString('ascii'),
            key: this.fromDbKey(rkey.slice(x+1)),
        };
    }

    fromRocksDbKey (tkey) {
        return this.fromRocksDbKeyQualified(tkey)?.key;
    }

    toRocksDbKey (key) {
        return Buffer.concat([this.gteBuf, key]);
    }

    /* Other */

    open (opts) {
        return new Promise((resolve, reject) => {
            this.rocksDb.open(opts, (err) => {
                if (err) { reject(err); } else { resolve(); }
            });
        });
    };

    close () {
        return new Promise((resolve, reject) => {
            this.rocksDb.close((err) => {
                if (err) { reject(err); } else { resolve(); }
            });
        });
    };

    clear(start, end) {
        return new Promise ((resolve, reject) => {
            const startKey = start ? this.toRocksDbKey(start) : this.gteBuf;
            const endKey = end ? this.toRocksDbKey(end) : this.ltBuf;
            this.rocksDb.clear({ gte: startKey, lt: endKey}, (err) => {
                if (err) { reject(err); } else { resolve(); }
            });
        });
    };

    approximateSize(start, end) {
        return new Promise ((resolve, reject) => {
            const startKey = start ? this.toRocksDbKey(start) : this.gteBuf;
            const endKey = end ? this.toRocksDbKey(end) : this.ltBuf;
            this.rocksDb.approximateSize(startKey, endKey, (err, s) => {
                if (err) { reject(err); } else { resolve(s); }
            });
        });
    }
}

/* ************************************************************************** */
/* Generic Table with Keys and Values as Buffers */

class AbstractTable {

    constructor (name) {
        this.name = name;
    }

    /* Iterator
     *
     * @param {object} opts - options accepted by rocks db iterators. This include 'reverse', and 'limit'
     * @return iterator for the table. The caller must call `end()` on the result when done using it.
     */
    iterator (opts) {
        throw "iterator not implemented in AbstractTable"
    }

    /* Iterator
     *
     * @param {object} opts - options accepted by rocks db iterators. This include 'reverse', and 'limit'
     * @return iterator for the table. The caller must call `end()` on the result when done using it.
     */
    reverseIterator (opts) {
        return this.iterator({... opts, reverse: true});
    }

    async get (key, opts) {
        throw "get not implemented in AbstractTable"
    }

    async put (key, value, opts) {
        throw "put not implemented in AbstractTable"
    }

    async clear() {
        throw "clear not implemented in AbstractTable"
    }

    async first () {
        const it = this.iterator()
        const r = await it.next().finally(it.end());
        if (r.done) {
            throw "first() called on empty iteration";
        } else {
            return { key: r.value.key, value: r.value.value };
        }
    }

    async firstKey () {
        return (await this.first())?.key;
    }

    async firstValue () {
        return (await this.first())?.value;
    }

    async last () {
        const it = this.reverseIterator();
        const r = await it.next().finally(it.end());
        if (r.done) {
            throw "last() called on empty iteration";
        } else {
            return { key: r.value.key, value: r.value.value };
        }
    }

    async lastKey () {
        return (await this.last())?.key;
    }

    async lastValue () {
        return (await this.last())?.value;
    }
}

class Table extends AbstractTable {

    constructor (db, name) {
        super(name);
        this.db = db;
        this.rocksDb = db.rocksDb;
        this.prefix = `${db.namespace}-${this.name}`

        this.gte = `${this.prefix}$`
        this.lt = `${this.prefix}%`

        this.gteBuf = Buffer.from(this.gte, 'ascii');
        this.ltBuf = Buffer.from(this.lt, 'ascii');
    }

    /* Internal Key Mapping */

    fromTableKeyQualified (rkey) {
        const x = rkey.indexOf('-');
        const y = rkey.indexOf('$');
        return {
            namespace: rkey.slice(0,x).toString('ascii'),
            table: rkey.slice(x+1,y).toString('ascii'),
            key: this.fromDbKey(rkey.slice(y+1)),
        };
    }

    fromTableKey (tkey) {
        return this.fromTableKeyQualified(tkey)?.key;
    }

    toTableKey (key) {
        return Buffer.concat([this.gteBuf, this.dbKey(key)]);
    }

    /* Key Mapping */

    fromDbKey (tkey) {
        return tkey;
    }

    dbKey (key) {
        return key ?? Buffer.alloc(0);
    }

    /* Value Mapping */

    fromDbValue (rvalue) {
        return rvalue;
    }

    /* only needed for tables that support writes */
    dbValue (value) {
        return value;
    }

    /* Iterator
     *
     * @param {object} opts - options accepted by rocks db iterators. This include 'reverse', and 'limit'.
     * @return {Promise<object>} A new TableIterator object.
     */
    iterator (opts) {
        return new TableIterator(this, opts);
    }

    /* Get
     *
     * @async
     * @param {Buffer} key - the lookup key.
     * @param {object} opts - Rocksdb Get options
     * @return {Promise}
     */
    get (key, opts) {
        return new Promise((resolve, reject) => {

            let tkey;
            try { tkey = this.toTableKey(key); } catch (e) { reject(e); }

            this.rocksDb.get(tkey, opts, (err, res) => {
                if (err) {
                    reject(err);
                } else {
                    try {
                        resolve(this.fromDbValue(res));
                    } catch (e) {
                        reject(e);
                    }
                }
            });
        });
    }

    put (key, value, opts) {
        return new Promise((resolve, reject) => {
            let rkey;
            try { rkey = this.toTableKey(key); } catch (e) { reject(e); }

            let rvalue;
            try { rvalue = this.dbValue(value); } catch (e) { reject (e); }

            this.rocksDb.put(rkey, rvalue, opts, (err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    clear (start, end) {
        return new Promise((resolve, reject) => {
            const startKey = start ? this.toTableKey(start) : this.gteBuf;
            const endKey = end ? this.toTableKey(end) : this.ltBuf;
            this.rocksDb.clear({ gte: startKey, lt: endKey}, (err) => {
                if (err) { reject(err); } else { resolve(); }
            });
        });
    }

    /* This triggers background compaction. It doesn't seem to await
     * finalization of compaction.
     */
    compact (start, end) {
        return new Promise((resolve, reject) => {
            const startKey = start ? this.toTableKey(start) : this.gteBuf;
            const endKey = end ? this.toTableKey(end) : this.ltBuf;
            this.rocksDb.compactRange(startKey, endKey, (err) => {
                if (err) { reject(err); } else { resolve(); }
            });
        });
    }

    approximateSize (start, end) {
        return new Promise((resolve, reject) => {
            const startKey = start ? this.toTableKey(start) : this.gteBuf;
            const endKey = end ? this.toTableKey(end) : this.ltBuf;
            this.rocksDb.approximateSize(startKey, endKey, (err, s) => {
                if (err) { reject(err); } else { resolve(s); }
            });
        });
    }
}

/* ************************************************************************** */
/* Table Iterator */

class AbstractTableIterator {
    constructor(tbl) {
        this.table = tbl;
    }

    async next () {
        throw "next not implemented in AbstractTableIterator"
    }

    async nextKey () {
        const x = await this.next();
        x.value = x.value?.key;
        return x;
    }

    async nextValue () {
        const x = await this.next();
        x.value = x.value?.value;
        return x;
    }

    async seek (key) {
        throw "seek not implemented in AbstractTableIterator"
    }

    async end () {
        return;
    }

    [Symbol.asyncIterator] () { return this; }
}

/* Rocksdb Table iterator */

class TableIterator extends AbstractTableIterator {
    constructor(tbl, opts) {

        super(tbl);
        this.db = tbl.db;
        this.rocksDb = tbl.rocksDb;

        /* Create RocksDb Iterator options */
        let o = opts || {};

        if (o.lt !== undefined) {
            o.lt = this.table.toTableKey(o.lt);
        } else if (o.lte !== undefined) {
            o.lte = this.table.toTableKey(o.lte);
        } else {
            o.lt = this.table.ltBuf;
        }

        if (o.gt !== undefined) {
            o.gt = this.table.toTableKey(o.gt);
        } else if (o.gte !== undefined) {
            o.gte = this.table.rocksDb(o.gte);
        } else {
            o.gte = this.table.gteBuf;
        }

        o.keyAsBuffer = true;
        o.valueAsBuffer = true;

        /* Create RocksDb Iterator */
        this.it = this.rocksDb.iterator(o);
    }

    next () {
        return new Promise((resolve, reject) => {
            this.it.next((e, k, v) => {
                if (e) {
                    reject(e);
                } else if (k) {
                    resolve({
                        done: false,
                        value: {
                            key: this.table.fromTableKey(k),
                            value: this.table.fromDbValue(v),
                        },
                    });
                } else {
                    resolve({ done: true });
                }
            });
        });
    };

    async seek (key) {
        return await this.it.seek(this.table.toTableKey(key));
    }

    end () {
        return new Promise((resolve, reject) => {
            this.it.end((err) => {
                if (err) { reject(err); } else { resolve(); }
            });
        });
    }
}

class ReverseTableIterator extends TableIterator {
    constructor(tbl, opts) {
        super(tbl, { ... opts, reverse: true });
    }
}

/* ************************************************************************** */
/* Typed Tables */

class JsonValueTable extends Table {
    fromDbValue(rval) { return JSON.parse(rval.toString()); }
    dbValue(val) { return Buffer.from(JSON.stringify(val)); }
}

class StringKeyJsonValueTable extends JsonValueTable {
    fromDbKey(rkey) { return rkey.toString(); }
    dbKey(key) { return Buffer.from(key ?? ""); }
}

class IntKeyIntValueTable extends Table {
    fromDbKey(rkey) { return rkey.readIntLE(0, 6); }
    dbKey(key) { const rkey = Buffer.alloc(6); rkey.writeIntLE(key ?? 0, 0, 6); return rkey; }
    fromDbValue(rval) { return rval.readIntLE(0, 6); }
    dbValue(val) { const rval = Buffer.alloc(6); rval.writeIntLE(val, 0, 6); return rval; }
}

/* ************************************************************************** */
/* Composite Tables */

/* Composite Tables represent a merged view of more than a single table.
 *
 * Implements the same interface as Table, though it doesn't extend it.
 */
class CompositeTable extends AbstractTable {

    /* @param db - database object
     * @param name - name of the table
     * @param tables - array of component tables
     */
    constructor(name, tables) {
        super(name);

        // associative map from tables names to tables
        this.tables = tables.reduce((o, v) => ({ ...o, [v.name]: v }), {});
        this.tableCount = tables.length;
    }

    /* Given the component keys, returns the composite key.
     *
     * The base implementation is the identity function that returns the
     * composite key.
     *
     * @param dbKeys - associative array that maps component tables names the respective keys
     * @returns associative array that maps component table names to the respective keys
     */
    fromDbKey (dbKeys) { return dbKeys; }

    /* Given the composite user key, create the component keys.
     *
     * Component keys can depend on values from other components. This allows
     * creating denormalized views of normalized database layouts with foreign
     * key constraints.
     *
     * To supports keys-value dependencies the implementation can return partial
     * results and has access to a partial collection of component values for
     * the given key. The collection is represented as an associative array that
     * maps component table names the respective values.
     *
     * If a key depends on a value, an implementations of this method must satisfy
     * the following requirements:
     *
     * 1. There are no cyclic dependencies betweeen keys and values.
     * 2. Each time the method is called at least one additional key is resolved until
     *    all keys are resolved.
     * 3. When all values are known, the method returns all keys.
     *
     * Failing to do so may result in infinite loops, or failing `put` operations.
     *
     * The base implementation is the identity functions and assumes that the
     * composite key is an associative map from component table names to the
     * respective component keys.
     *
     * @param keys - composite user key, an associative map from component tables names to the respective keys.
     * @param values - associative map of tables names to values for the respective keys. May contain null values.
     * @returns associative map from component tables names to the respective keys.
     */
    dbKey (keys, _values) { return keys; }

    /* Compute the composite value from the component values
     *
     * The base implementation is the identity function.
     *
     * @param dbValues - associative map of component tables names to values
     * @returns - composite value, an associative map of component table names to values
     */
    fromDbValue (dbValues) { return dbValues; }

    /* Compute the component values from the composite value.
     *
     * This is only supported for bidirectional table views that support the
     * `put` method.
     *
     * The base implementation is the identity function.
     *
     * @param dbValues - composite value, associative map of component tables names to values
     * @returns component values, an associative map of component table names to values
     */
    dbValue (values) { return values; }

    /* Get a value from the table
     *
     * @params key - the key
     * @params opts - RocksDb get options
     * @returns the value for the key
     */
    async get (key, opts) {

        // Initially, there are no values and all available keys are pending
        const values = {};
        let pendingKeys = Object.entries(this.dbKey(key, values)).filter(([_,v]) => v);
        let missingValueCount = this.tableCount - Object.keys(values).length;

        while (missingValueCount > 0) {
            await Promise.all(pendingKeys.map(async ([k,v]) => {
                values[k] = await this.tables[k].get(v);
            }));

            // get new set of dbKeys for the next round of collecting values
            const dbKeys = Object.entries(this.dbKey(key, values));
            // mark all dbKeys pending for which there's no value yet
            pendingKeys = dbKeys.filter(([k,v]) => v && ! values[k]);

            // detect infinit loops due to invalid implementations of `dbKey`.
            const newMissingValueCount = this.tableCount - Object.keys(values).length;
            if (missingValueCount > 0 && newMissingValueCount >= missingValueCount) {
                throw `detected infinit loop in rocksDbKey implementation of table ${this.name}`
            } else {
                missingValueCount = newMissingValueCount;
            }
        }

        return this.fromDbValue(values);
    };

    /* Put a new value
     *
     * This method is supported only for bi-directional views, where it is
     * possible to compute the component values from the composite value.
     */
    async put (key, value) {
        const dbValues = this.dbValue(value);
        const dbKeys = Object.entries(this.dbKey(key, dbValues));
        return await Promise.all(dbKeys.map(async ([k,v]) => {
            await this.tables[k].put(v, dbValues[k]);
        }));
    }

    async clear () {
        return await Promise.all(Object.values(this.tables).map(async v => v.clear() ));
    }

    /* this is somewhat inefficient, because the the primary key
     * value will mostly be obtain twice. However, RocksDb caches queries
     * by default, so it's probably fine for most use cases.
     *
     * Otherwise, we could provide a low more level approach that would
     * share some code with the implementation of get.
     *
     */
    iterator (keyIterator, opts) {
        return new CompositeTableIterator(this, keyIterator, opts)
    }

    approximateSize (start, end) {
      const tbls = Object.entries(this.tables);
      return Promise.all(
          tbls.map(async ([tblName, tbl]) => {
              const startKey = start ? start[tblName] : undefined;
              const endKey = end ? end[tblName] : undefined;
              const r = await tbl.approximateSize(startKey, endKey);
              return r
          })
      ).then(x => x.reduce((a, b) => a + b, 0));
    }
}

class CompositeTableIterator extends AbstractTableIterator {
    constructor(tbl, keyIterator, opts) {
        super(tbl);
        this.keyIterator = keyIterator;
    }

    async next () {
        const key = await this.keyIterator.next();
        if (key.done) {
            return key;
        } else {
            return {
                done: false,
                value: {
                    key: key.value,
                    value: await this.table.get(key.value)
                }
            };
        };
    }

    async seek (key) {
        await this.keyIterator.seek(key);
    }
}

/* ************************************************************************** */

module.exports = {
    Db: Db,

    // Tables
    AbstractTable: AbstractTable,
    Table: Table,
    JsonValueTable: JsonValueTable,
    StringKeyJsonValueTable: StringKeyJsonValueTable,
    IntKeyIntValueTable: IntKeyIntValueTable,
    CompositeTable: CompositeTable,

    // Iterators
    TableIterator: TableIterator,
    ReverseTableIterator: ReverseTableIterator,
}
