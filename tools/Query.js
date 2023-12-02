// with-ctx docker compose --env-file=.env run --rm chainweb-db-js-tools
// apt-get install nodejs
// npm install
// node ./tools/Header.js /db/0_old_rocksdb 3 4294600 20

const { program } = require('commander');
const fs = require('fs');

const cdb = require("../src/ChainDb");
const { asyncIter } = require('../src/Utils');
const { SHA256Hash } = require("../src/BlockHeader");

const { createLogger, transports } = require('winston');

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

/* ************************************************************************** */
/* Main Program Options */

program
  .name("query-chainweb-db")
  .description("Return items from the Chainweb RocksDb")
  .version("0.0.0")
  .option('-d, --database-directory <string>', "Chainweb RocksDb directory.", "/db/0/rocksDb")
  .option('--debug', "enable debugging");

/* ************************************************************************** */
/* Print Headers */

async function headers (dbPath, chain, start, count) {
  withReadOnlyDb(dbPath, async (db) => {
    const hdr_tbl = db.headerTable(chain);
    const it = hdr_tbl.iterator()
    try {
      it.seek({height: start, hash: null});
        const it2 = asyncIter.take(it, count);
        for await (let x of it2) {
          console.log(`${JSON.stringify(x.value)}`);
        }
    } finally {
      it.end();
    }
  });
}

program.command('headers')
  .description('print entries from the BlockHeader table')
  .option('-c, --chain-id <int>', "Id of the chain from which headers are returned.", 0)
  .option('-s, --start-height <int>', "Start block height.", 0)
  .option('-n, --count <int>', "Number of headers that are returned.", 1)
  .action((_, cmd) => {

    const options = cmd.optsWithGlobals();
    logger.level = options.debug ? 'debug' : 'warn';
    logger.log('debug', { options: options});

    const dbDir = options.databaseDirectory;
    const chain = options.chainId;
    const start = BigInt(options.startHeight);
    const count = options.count;

    if (!fs.existsSync(dbDir)) {
      logger.error(`failed to open database directory ${dbDir}`);
      process.exit(1);
    }

    headers(dbDir, chain, start, count);
  });

program.command('payload')
  .description('print a block payload')
  .option('-c, --chain-id <int>', "Id of the chain from which headers are returned.", 0)
  .requiredOption('-p, --payload-hash <string>', "The payload hash of the payload")
  .action((_, cmd) => {
    const options = cmd.optsWithGlobals();
    logger.level = options.debug ? 'debug' : 'warn';
    logger.log('debug', { options: options});

    const dbDir = options.databaseDirectory;
    const chain = options.chainId;
    const hash = new SHA256Hash(Buffer.from(options.payloadHash, 'base64url'));

    withReadOnlyDb(dbDir, async (db) => {
      const tbl = db.payloadWithOutputsTable(chain);
      const v = await tbl.get(hash);
      console.log(JSON.stringify(v));
    });
  });

program.command('rank')
  .description('find block height for a block hash')
  .option('-c, --chain-id <int>', "Id of the chain.", 0)
  .requiredOption('-b, --block-hash <string>', "The block hash for which the height is looked up.")
  .action((_, cmd) => {
    const options = cmd.optsWithGlobals();
    logger.level = options.debug ? 'debug' : 'warn';
    logger.log('debug', { options: options});

    const dbDir = options.databaseDirectory;
    const chain = options.chainId;
    const hash = new SHA256Hash(Buffer.from(options.blockHash, 'base64url'));

    withReadOnlyDb(dbDir, async (db) => {
      const tbl = db.rankTable(chain);
      const v = await tbl.get(hash);
      console.log(Number(v));
    });
  });

/* ************************************************************************** */
/* Main */

program.parse(process.argv);

