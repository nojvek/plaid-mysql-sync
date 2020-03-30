import fs from 'fs';
import plaid from 'plaid';
import moment from 'moment';

// trying to land this as part of proper typescript
// see https://github.com/microsoft/TypeScript/issues/37695
type Obj<T> = {[key: string]: T};

export interface PlaidConfig {
  clientId: string;
  secret: string;
  publicKey: string;
  env: string;
  institutionTokens: Obj<string>;
}

export interface DbConfig {
  host: string;
  user: string;
  password: string;
  database: string;
}

interface SyncConfig {
  plaid: PlaidConfig;
  db: DbConfig;
}

const {plaid: plaidConf, db: dbConf}: SyncConfig = JSON.parse(fs.readFileSync(`./config.json`, `utf-8`));

const plaidApi = new plaid.Client(
  plaidConf.clientId,
  plaidConf.secret,
  plaidConf.publicKey,
  plaid.environments[plaidConf.env],
  {version: '2019-05-29'},
);

function arrayToSql(tableName: string, rows: any[]): string {
  if (rows.length < 1) {
    return ``;
  }

  const esc = (str: string) => `\`${str}\``;
  const sqlParts: string[] = [];
  const columns = Object.keys(rows[0]);
  sqlParts.push(`INSERT INTO ${esc(tableName)} (${columns.map(esc).join(`, `)}) VALUES`);
  rows.forEach((row, idx) => {
    const comma = idx < rows.length - 1 ? `,` : ``;
    sqlParts.push(`(${columns.map((col) => JSON.stringify(row[col] ?? null)).join(`, `)})${comma}`);
  });
  const valueUpdates = columns
    .filter((col) => col !== `id`)
    .map((col) => `${esc(col)}=VALUES(${esc(col)})`)
    .join(`, `);
  sqlParts.push(`ON DUPLICATE KEY UPDATE ${valueUpdates}`);
  sqlParts.push(`;`);
  return sqlParts.join(`\n`);
}

function writeTable(tableName: string, rows: any[]) {
  fs.writeFileSync(`tables/${tableName}.sql`, arrayToSql(tableName, rows));
}

async function syncCategories() {
  const {categories} = await plaidApi.getCategories();
  const categoryRows = categories.map(({category_id, group, hierarchy}) => ({
    id: category_id,
    group,
    category: hierarchy[0],
    category1: hierarchy[1],
    category2: hierarchy[2],
  }));

  writeTable(`categories`, categoryRows);
}

async function syncAccounts(institutionTokens: Obj<string>, historyMonths = 1) {
  const accountRows = [];
  const institutionRows = [];
  const transactionRows = [];

  // refreshing transactions is not available in dev env :(
  // const accessTokens = Object.values(institutionTokens);
  // console.log(`refreshing transactions across ${accessTokens.length} accounts`);
  // await Promise.all(accessTokens.map((accessToken) => plaidClient.refreshTransactions(accessToken)));

  for (const [institutionName, accessToken] of Object.entries(institutionTokens)) {
    console.log(`downloading data for`, institutionName, accessToken);

    // accounts //
    const {accounts, item: institution} = await plaidApi.getAccounts(accessToken);
    institutionRows.push({
      id: institution.institution_id,
      name: institutionName,
    });

    for (const account of accounts) {
      let curBalance = account.balances.current;
      if (account.type === `credit` || account.type === `loan`) {
        curBalance *= -1;
      }

      accountRows.push({
        id: account.account_id,
        institution_id: institution.institution_id,
        balance_current: curBalance,
        mask: account.mask,
        name: account.official_name || account.name,
        type: account.type,
        subtype: account.subtype,
      });
    }

    // transactions //
    const startDate = moment().subtract(historyMonths, 'months').format('YYYY-MM-DD');
    const endDate = moment().format('YYYY-MM-DD');
    const {transactions} = await plaidApi.getAllTransactions(accessToken, startDate, endDate);

    for (const tr of transactions) {
      if (tr.pending) {
        continue; // ignore pending transactions
      }

      // remove unneccessary prefix
      const prefixMatch = tr.name.match(/^Ext Credit Card (Debit|Credit) /);
      if (prefixMatch) {
        tr.name = tr.name.substr(prefixMatch[0].length);
      }

      if (!tr.location.country && tr.iso_currency_code === `USD` && tr.location.region) {
        tr.location.country = `US`;
      }

      transactionRows.push({
        id: tr.transaction_id,
        account_id: tr.account_id,
        name: tr.name,
        amount: -tr.amount,
        date: tr.date,
        category_id: tr.category_id,
        currency_code: tr.iso_currency_code,
        location_city: tr.location.city,
        location_state: tr.location.region,
        location_country: tr.location.country,
        payment_channel: (tr as any).payment_channel, // plaid types not upto date, made a PR
      });
    }
  }

  writeTable(`accounts`, accountRows);
  writeTable(`institutions`, institutionRows);
  writeTable(`transactions`, transactionRows);
}

///// main /////

syncCategories().catch((err) => console.error(err));
syncAccounts(plaidConf.institutionTokens, 5 * 12).catch((err) => console.error(err));
