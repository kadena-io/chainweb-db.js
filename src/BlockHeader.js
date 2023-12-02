const {from64, from64Json, toHex} = require("./Utils");

/* ************************************************************************** */

/* Version:
 *
 * `version` identifies the chainweb version. It is a 32 bit value in little endian encoding.
 * Values up to 0x0000FFFF are reserved for production versions (which includes `Development` and testnets).
 *
 * | value      | version     |
 * | ---------- | ----------- |
 * | 0x00000005 | Mainnet01   |
 * | 0x00000001 | Development |
 * | 0x00000007 | Testnet04   |
 */
class ChainwebVersion {

    constructor (x) {
        if (x instanceof Buffer) {
            this.code = x.readUInt32LE();
        } else if (typeof x === 'number') {
            this.code = x;
        } else if (typeof x === 'string') {
            switch (x) {
                case "mainnet01": this.code = 0x00000005; break;
                case "Mainnet01": this.code = 0x00000005; break;
                case "testnet04": this.code = 0x00000007; break;
                case "Testnet04": this.code = 0x00000007; break;
                case "development": this.code = 0x00000001; break;
                case "Development": this.code = 0x00000001; break;
                default: throw `Unsupported chainweb version: ${x}`
            }
        }
    }

    get [Symbol.toStringTag] () {
        return this.string;
    }

    get string () {
        switch (this.code) {
            case 0x00000005: return "mainnet01";
            case 0x00000007: return "testnet04";
            case 0x00000001: return "development";
            default: throw `Unsupported chainweb version: ${this.code}`
        }
    }

    toJSON () {
        return this.toString();
    }

    static get Mainnet01 () { return new ChainwebVersion("mainnet01"); }
    static get Testnet04 () { return new ChainwebVersion("testnet04"); }
    static get Development () { return new ChainwebVersion("development"); }
};

/* ************************************************************************** */
/* 256 bit Hashes */

/* Keeps a reference to the original buffer.
*/
class SHA256HashView {
    constructor (buffer, off) {
        const bytes = buffer.slice((off ?? 0), (off ?? 0) + 32);
        if (! (bytes instanceof Buffer)) {
            throw `Wrong argument type: expected Buffer`;
        } else if (bytes.length !== 32) {
            throw `SHA256HashView as wrong length. Expected 32 bytes but got ${bytes.length} bytes`;
        } else {
            this.buf = bytes;
        }
    }

    get buffer () { return Buffer.from(this.buf); }

    static compare(a, b) { return Buffer.compare(a.buf, b.buf); }
    compare (b) { return this.buf.compare(b.buf); }

    static equals(a, b) { return a.equals(a.buf, b.buf); }
    equals (b) { return this.buf.equlas(b.buf); }

    get [Symbol.toStringTag] () { return this.buf.toString('base64url'); }

    static get NullHash () {
        return new SHA256HashView(Buffer.alloc(32, 0));
    }

    toJSON () {
        return this.buffer.toString('base64url');
    }
}

/* The constructor copies the underlying buffer
*/
class SHA256Hash extends SHA256HashView {
    constructor (buffer, off) {
        super(Buffer.from(buffer), off);
    }
}

/* ************************************************************************** */
/* POW Hashes */

/* Keeps a reference to the original buffer.
 *
 * Arithmetic operations and comparisons on `parent`, `target`, `weight`, and `hash`
 * interpret the value as unsigned 256 bit integral numbers in little endian encoding.
 * All operations are performed using rational arithmetic of unlimited precision and the final result is rounded.
 * Please consult the code for details of how the result is rounded.
 *
 */
class PoWHashView {

    constructor (buffer, off) {
        const bytes = buffer.slice(off ?? 0, (off ?? 0) + 32);
        if (! (bytes instanceof Buffer)) {
            throw `Wrong argument type: expected Buffer`;
        } else if (bytes.length !== 32) {
            throw `PoWHashView as wrong length. Expected 32 bytes but got ${bytes.length} bytes`;
        } else {
            this.buf = bytes;
        }
    }

    get buffer () { return Buffer.from(this.buf); }

    static compare(a, b) { return Buffer.compare(a.buf.reverse(), b.buf.reverse()); }
    compare (b) { return this.buf.reverse().compare(b.buf.reverse()); }

    static equals(a, b) { return a.equals(a.buf, b.buf); }
    equals (b) { return this.buffer.equlas(b.buf); }

    get [Symbol.toStringTag] () { return this.value.toString(16); }

    get value () {
        let a = 0n;
        let x = 2n ** 64n;
        a = this.buf.readBigUInt64LE(24);
        a = a * x + this.buf.readBigUInt64LE(16);
        a = a * x + this.buf.readBigUInt64LE(8);
        a = a * x + this.buf.readBigUInt64LE(0);
        return a;
    }

    static get NullHash () {
        return new SHA256HashView(Buffer.alloc(32, 0));
    }

    toJSON () {
      return toHex(this.value, 32);
    }
}

/* The constructor copies the underlying buffer
*/
class PoWHash extends PoWHashView {
    constructor (buffer, off) {
        super(Buffer.from(buffer.slice(buffer, off)));
    }
}

/* ************************************************************************** */

/*
# BlockHeader Binary Format For Chain Graphs of Degree Three without Hash

defined in `Chainweb.BlockHeader`

| Size | Bytes   | Value       |
| ---- | ------- | ----------- |
| 8    | 0-7     | flags       |
| 8    | 8-15    | time        |
| 32   | 16-47   | parent      |
| 110  | 48-157  | adjacents   |
| 32   | 158-189 | target      |
| 32   | 190-221 | payload     |
| 4    | 222-225 | chain       |
| 32   | 226-257 | weight      |
| 8    | 258-265 | height      |
| 4    | 266-269 | version     |
| 8    | 270-277 | epoch start |
| 8    | 278-285 | nonce       |
| 32   | 286-317 | hash        |

total: 318 bytes

Adjacent Parents Record (length 3):

| Size | Bytes | Value     |
| ---- | ----- | --------- |
| 2    | 0-1   | length    |
| 108  | 2-109 | adjacents |

total: 110 bytes

Adjacent Parent:

| Size | Bytes | Value |
| ---- | ----- | ----- |
| 4    | 0-3   | chain |
| 32   | 4-35  | hash  |

total: 36 bytes

## Fields

**Time Stamps**:

`time` and `epoch start` are a little endian twoth complement encoded integral numbers that count SI microseconds since POSIX epoch (leap seconds are ignored). These numbers are always positive (highest bit is 0).

**Numbers**:

- `height` is a little endian encoded unsigned integral 64 bit number.
- `length` is a little endian encoded unsigned integral 16 bit number.

**Other**:

- `nonce` is any sequence of 8 bytes that is only compared for equality.
- `chain` is any sequence of 4 bytes that identifies a chain and can be compared for equality.
- `payload` is any sequence of 32 bytes that is a cryptographic hash of the payload associated with the block and can be compared for equality.
- `flags` are eight bytes of value 0x0 that are reserved for future use.
*/
class HeaderView {

    // TODO: it is probably better to return a proper object with non-lazy properties
    constructor (buffer) {
        if (! (buffer instanceof Buffer)) {
            throw `Wrong argument type: expected Buffer`;
        } else if (buffer.length !== 318) {
            throw `Header as wrong length. Expected 318 bytes but got ${buffer.length} bytes`;
        } else {
            this.buffer = buffer;
        }
    }

    /* | 8    | 0-7     | flags       | */
    get flags () { return this.buffer.slice(0,8); }

    /* | 8    | 8-15    | time        | */
    get time () { return this.buffer.readBigInt64LE(8); }

    /* | 32   | 16-47   | parent      | */
    get parentHash () { return new SHA256HashView(this.buffer, 16); }

    /* | 110  | 48-157  | adjacents   | */
    get adjacents () {
        const r = {};
        const length = this.buffer.readUInt16LE(48);
        for (let i = 0; i < length; ++i) {
            const off = 50 + i * 36;
            const chain = this.buffer.readUInt32LE(off);
            r[chain] = new SHA256HashView(this.buffer, off + 4);
        }
        return r;
    }

    /* | 32   | 158-189 | target      | */
    get target () { return new PoWHashView(this.buffer, 158); }

    /* | 32   | 190-221 | payload     | */
    get payloadHash () { return new SHA256HashView(this.buffer, 190); }

    /* | 4    | 222-225 | chain       | */
    get chain () { return this.buffer.readUInt32LE(222); }

    /* | 32   | 226-257 | weight      | */
    get weight () { return new PoWHashView(this.buffer, 226); }

    /* | 8    | 258-265 | height      | */
    get height () { return this.buffer.readBigUInt64LE(258); }

    /* | 4    | 266-269 | version     | */
    get version () { return new ChainwebVersion(this.buffer.slice(266, 266 + 4)); }

    /* | 8    | 270-277 | epoch start | */
    get epochStart () { return this.buffer.readBigUInt64LE(270); }

    /* | 8    | 278-285 | nonce       | */
    get nonce () { return this.buffer.slice(278, 278 + 8); }

    /* | 32   | 286-317 | hash        | */
    get hash () { return new SHA256HashView(this.buffer, 286); }

    get value () {
        return {
            flags: toHex(this.flags, 16),
            time: Number(this.time),
            parentHash: this.parentHash,
            adjacents: this.adjacents,
            target: this.target,
            payloadHash: this.payloadHash,
            weight: this.weight,
            height: Number(this.height),
            version: this.version,
            epochStart: Number(this.epochStart),
            nonce: toHex(this.nonce, 16),
            hash: this.hash,
        };
    }

    static compare(a, b) {
        return Buffer.compare(a.height, b.height)
           || a.time.compare(b.time)
           || a.hash.compare(b.hash);
    }

    compare (b) {
        return HeaderView.compare(this, b);
    }

    static equals(a, b) {
        return a.equals(a.hash, b.hash);
    }

    equals (b) {
        return this.hash.equlas(b.hash);
    }

    get [Symbol.toStringTag] () {
        return this.value;
    }

    toJSON () {
      return this.value;
    }
}

/* ************************************************************************** */

module.exports = {
    ChainwebVersion: ChainwebVersion,
    HeaderView: HeaderView,
    SHA256HashView: SHA256HashView,
    SHA256Hash: SHA256Hash,
    PoWHashView: PoWHashView,
    PoWHash: PoWHash,
};
