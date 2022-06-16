const sqlite = require('better-sqlite3')

/* ************************************************************************** */
/* sync to sqlite transfers db */

function insertExample () {

    const sqlDb = new sqlite.Database('../chainweb-master/tmp/mainnet/chainweb.db');

    const insert = db.prepare(
        'INSERT INTO transfers (hash, cmd, sender, receiver, amount, srcChain, trgChain, height, creationTime VALUES (@hash, @cmd, @sender, @receiver, @amount, @srcChain, @trgChain, @height, @creationTime)'
    );

    const insertMany = db.transaction((transfers) => {
        for (const t of transfers) insert.run(t);
    });

    insertMany([
        {
            hash: 'oizZzOwO0kK74XXJyDI_h5Jx1MUFM0ehIQaCbrso9FA',
            cmd: 'coin.transfer-create',
            sender: '7590fd61819f98b3276eacf50b41525e9080bb0598d6212689e85b61d9981015',
            receiver: 'bca8a017c9edb8d32ac03bf3863798c2ab02bb67bc1983ae1c07f19930e85596',
            amount: 5.276751863679,
            srcChain: '3',
            trgChain: '3',
            height: 140950,
            creationTime: '2019-12-17T16:08:32Z'
        }
    ]);
}

/* ************************************************************************** */
/* module exports */

module.exports = TODO;

