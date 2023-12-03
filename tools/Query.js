// with-ctx docker compose --env-file=.env run --rm chainweb-db-js-tools
// apt-get install nodejs
// npm install
// node ./tools/Header.js /db/0_old_rocksdb 3 4294600 20

const { program, Command } = require('commander');
const fs = require('fs');

const cdb = require("../src/ChainDb");
const { asyncIter, length } = require('../src/Utils');
const { SHA256Hash } = require("../src/BlockHeader");

const { createLogger, transports } = require('winston');

/* ************************************************************************** */
/* Logger */

const logger = createLogger({
  level: 'warn',
  transports: [new transports.Console()],
});

/* ************************************************************************** */
/* Utils */

async function withReadOnlyDb (dbPath, fn) {
  const db = new cdb.ChainDb(dbPath);
  await db.open({ readOnly: true });
  logger.debug("db opened")
  try {
    return await fn(db);
  } finally {
      await db.close();
      logger.debug("db closed")
  }
}

function makeSizeCommand () {
  const sizeCmd = new Command('approximate-size');
  sizeCmd
    .alias('size')
    .description('approximate size in bytes of the table.')
    .action(async (_, cmd) => {
      const size = await cmd.tbl.approximateSize();
      console.log(size);
    });
  return sizeCmd;
}

function makeCountCommand () {
  const sizeCmd = new Command('count');
  sizeCmd
    .alias('size')
    .description('number of entries in the table')
    .action(async (_, cmd) => {
      const it = cmd.tbl.iterator()
      console.log(await asyncIter.length(it));
    });
  return sizeCmd;
}

/* By default Stringify BigInt values as strings
 */
BigInt.prototype.toJSON = function() {
    return this.toString()
}

/* ************************************************************************** */
/* Main Program Options */

program
  .name("query-chainweb-db")
  .description("Return items from the Chainweb RocksDb")
  .version("0.0.0")

  // provide database
  .option('-d, --database-directory <string>', "Chainweb RocksDb directory.", "/db/0/rocksDb")
  .hook('preAction', async (thisCmd, actionCmd) => {
    const dbDir = thisCmd.opts().databaseDirectory;
    if (!fs.existsSync(dbDir)) {
      logger.error(`failed to open database directory ${dbDir}`);
      process.exit(1);
    }
    const db = new cdb.ChainDb(dbDir);
    await db.open({ readOnly: true });
    logger.debug("db opened in readonly mode");
    actionCmd.db = db;
  })
  .hook('postAction', async (thisCmd, actionCmd) => {
    const db = actionCmd.db;
    await db.close();
    logger.debug("db closed");
  })

  // set loglevel
  .option('--debug', "enable debugging")
  .hook('preSubcommand', (thisCmd, subCmd) => {
    logger.level = thisCmd.opts().debug ? 'debug' : 'warn';
    logger.debug('Debugging enabled');
  });

/* ************************************************************************** */
/* Header */

const headerTable = program
  .command('header')
  .alias('hdr')
  .requiredOption('-c, --chain-id <int>', "Id of the chain from which headers are returned.")
  .hook('preAction', async (thisCmd, actionCmd) => {
    const chain = thisCmd.opts().chainId;
    actionCmd.tbl = actionCmd.db.headerTable(chain);
  });

headerTable.command('list')
  .description('list entries from the BlockHeader table')
  .option('-s, --start-height <int>', "Start block height.", 0)
  .option('-n, --count <int>', "Number of headers that are returned.", 1)
  .option('--deps', "Include dependencies from the rank and payload tables)")
  .option('--check', "Like --deps, but only checks whether dependencies exist.")
  .hook('preAction', async (thisCmd, actionCmd) => {
    const chain = thisCmd.optsWithGlobals().chainId;
    actionCmd.ranksTbl = actionCmd.db.rankTable(chain);
    actionCmd.payloadTbl = actionCmd.db.payloadWithOutputsTable(chain);
  })
  .action(async (_, cmd) => {
    logger.log('debug', { options: cmd.optsWithGlobals() });

    const options = cmd.opts();
    const start = BigInt(options.startHeight);
    const count = options.count;

    const it = cmd.tbl.iterator()
    try {
      it.seek({height: start, hash: null});
        const it2 = asyncIter.take(it, count);
        for await (let x of it2) {
          if (cmd.opts().deps) {
            const r = await cmd.ranksTbl.get(x.value.hash)
              .catch(e => e.toString());
            const p = await cmd.payloadTbl.get(x.value.payloadHash)
              .catch(e => e.toString());
            console.log(JSON.stringify({header: x, rank: r, payload: p}));
          } else if (cmd.opts().check) {
            const r = await cmd.ranksTbl.get(x.value.hash)
              .then(_ => true)
              .catch(_ => false);
            const p = await cmd.payloadTbl.get(x.value.payloadHash)
              .then(_ => true)
              .catch(_ => false);
            console.log(JSON.stringify({header: x, rank: r, payload: p}));
          } else {
            console.log(JSON.stringify(x));
          }
        }
    } finally {
      it.end();
    }
  });

headerTable.addCommand(makeSizeCommand());
headerTable.addCommand(makeCountCommand());

/* ************************************************************************** */
/* Payload */

const payloadTable = program
  .command('payload-with-outputs')
  .alias("pwo")
  .requiredOption('-c, --chain-id <int>', "Id of the chain from which payloads are returned.")
  .hook('preAction', async (thisCmd, actionCmd) => {
    const chain = thisCmd.opts().chainId;
    actionCmd.tbl = actionCmd.db.payloadWithOutputsTable(chain);
  });

payloadTable.command("get")
  .description('get block block payload')
  .requiredOption('-p, --payload-hash <string>', "The payload hash of the payload")
  .action(async (_, cmd) => {
    logger.log('debug', { options: cmd.optsWithGlobals() });
    const options = cmd.opts();
    const hash = new SHA256Hash(Buffer.from(options.payloadHash, 'base64url'));
    const v = await cmd.tbl.get(hash);
    console.log(JSON.stringify(v));
  });

payloadTable.addCommand(makeSizeCommand());
payloadTable.addCommand(makeCountCommand());

/* ************************************************************************** */
/* Rank */

const rankTable = program
  .command('block-rank')
  .alias('height')
  .alias("rank")
  .requiredOption('-c, --chain-id <int>', "Id of the chain from which payloads are returned.")
  .hook('preAction', async (thisCmd, actionCmd) => {
    const chain = thisCmd.opts().chainId;
    actionCmd.tbl = actionCmd.db.rankTable(chain);
  });

rankTable.command('get')
  .description('get block height for a block hash')
  .requiredOption('-b, --block-hash <string>', "The block hash for which the height is looked up.")
  .action(async (_, cmd) => {
    logger.log('debug', { options: cmd.optsWithGlobals() });
    const options = cmd.opts();
    const hash = new SHA256Hash(Buffer.from(options.blockHash, 'base64url'));
    const v = await cmd.tbl.get(hash);
    console.log(Number(v));
  });

rankTable.addCommand(makeSizeCommand());
rankTable.addCommand(makeCountCommand());

/* ************************************************************************** */
/* Cut */

const cutTable = program
  .command('cut')
  .hook('preAction', async (thisCmd, actionCmd) => {
    actionCmd.tbl = actionCmd.db.cutTable;
  });

// TODO add block height start option
cutTable.command('list')
  .description('list entries from the cut table')
  .option('-s, --start-height <int>', "Start cut height.", 0)
  .option('-n, --count <int>', "Number of cuts that are returned.", 1)
  // .option('--deps', "Include dependencies from the rank and payload tables)")
  // .option('--check', "Like --deps, but only checks whether dependencies exist.")
  // .hook('preAction', async (thisCmd, actionCmd) => {
  //   const chain = thisCmd.optsWithGlobals().chainId;
  //   actionCmd.ranksTbl = actionCmd.db.rankTable(chain);
  //   actionCmd.payloadTbl = actionCmd.db.payloadWithOutputsTable(chain);
  // })
  .action(async (_, cmd) => {
    logger.log('debug', { options: cmd.optsWithGlobals() });

    const options = cmd.opts();
    const start = BigInt(options.startHeight);
    const count = options.count;

    const it = cmd.tbl.iterator()
    try {
      it.seek({height: start, hash: null});
        const it2 = asyncIter.take(it, count);
        for await (let x of it2) {
          // if (cmd.opts().deps) {
          //   const r = await cmd.ranksTbl.get(x.value.hash)
          //     .catch(e => e.toString());
          //   const p = await cmd.payloadTbl.get(x.value.payloadHash)
          //     .catch(e => e.toString());
          //   console.log(JSON.stringify({header: x, rank: r, payload: p}));
          // } else if (cmd.opts().check) {
          //   const r = await cmd.ranksTbl.get(x.value.hash)
          //     .then(_ => true)
          //     .catch(_ => false);
          //   const p = await cmd.payloadTbl.get(x.value.payloadHash)
          //     .then(_ => true)
          //     .catch(_ => false);
          //   console.log(JSON.stringify({header: x, rank: r, payload: p}));
          // } else {
            console.log(JSON.stringify(x));
          // }
        }
    } finally {
      it.end();
    }
  });

cutTable.addCommand(makeSizeCommand());
cutTable.addCommand(makeCountCommand());

/* ************************************************************************** */
/* Main */

program.parse(process.argv);

