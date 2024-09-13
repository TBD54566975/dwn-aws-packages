# ⚠️ Warning: Experimental

This module is currently under active development and is suitable for *experimental* use only. This code base could fundamentally change at any time without warning.

It is highly discouraged to use this for any production workloads.

# DWN AWS  Store <!-- omit in toc -->

DynamoDB NoSQL backed implementations of DWN `MessageStore`, `DataStore`, `EventLog` and `ResumableTask`.

- [Supported DBs](#supported-dbs)
- [Installation](#installation)
- [Considerations](#considerations)
- [Usage](#usage)
  - [DynamoDB](#dynamodb)
- [Development](#development)
  - [Prerequisites](#prerequisites)
    - [`node` and `npm`](#node-and-npm)
    - [`serverless`](#serverless)
  - [Running Tests](#running-tests)
  - [`npm` scripts](#npm-scripts)
  - [Environment Variables](#environment-variables)
- [Developers Journal](#developers-journal)
- [Tips](#tips)

# Supported DBs
* AWS DynamoDB ✔️
* AWS DynamoDB Local ✔️

# Installation

```bash
npm install @tbd54566975/dwn-datastore-aws
```

# Considerations

- Since DynamoDB is an AWS hosted service, factor latency into your application requirements. Use the `AWS_REGION` environment variable to use an AWS Region closest to where your application runs.
- If you wish to run tests against a AWS hosted DynamoDB tables, the stores will create the tables in real time, however, the tables may not be ready for use for several minutes. You may need to run the test to create the tables, allow the first pass to fail, then attempt to run the test again after 10 minutes when the tables are ready for use.

# Usage

## DynamoDB

```typescript
import Database from 'better-sqlite3';

import { Dwn } from '@tbd54566975/dwn-sdk-js'
import { MessageStoreNoSql, DataStoreNoSql, EventLogNoSql, ResumableTaskStoreNoSql } from '@tbd54566975/dwn-sql-store';

const messageStore = new MessageStoreNoSql();
const dataStore = new DataStoreNoSql();
const eventLog = new EventLogNoSql();
const resumableTaskStore = new ResumableTaskStoreNoSql();

const dwn = await Dwn.create({ messageStore, dataStore, eventLog, resumableTaskStore });
```

# Development

## Prerequisites
### `node` and `npm`
This project is developed and tested with [Node.js](https://nodejs.org/en/about/previous-releases)
`v18` and `v20` and NPM `v9`. You can verify your `node` and `npm` installation via the terminal:

```
$ node --version
v20.3.0
$ npm --version
9.6.7
```

### `serverless`
This project uses serverless to run a local instance of dynamodb for testing purposes only.
```
npm i serverless -g
```

If you don't have `node` installed. Feel free to choose whichever approach you feel the most comfortable with. If you don't have a preferred installation method, i'd recommend using `nvm` (aka node version manager). `nvm` allows you to install and use different versions of node. It can be installed by running `brew install nvm` (assuming that you have homebrew)

Once you have installed `nvm`, install the desired node version with `nvm install vX.Y.Z`.

## Running Tests
> 💡 Make sure you have all the [prerequisites](#prerequisites)

0. clone the repo and `cd` into the project directory
1. Install all project dependencies by running `npm install`
2. Start the test databases using `./scripts/start-databases`
3. Open a second terminal and run `export IS_OFFLINE=true` to run the tests against the offline DynamoDB database
4. Run tests using `npm run test` (serverless struggles with daemons so it requires a dedicated terminal)

`Ctrl+C` in the terminal where you started the database when tests have completed.

## `npm` scripts

| Script                  | Description                                 |
| ----------------------- | ------------------------------------------- |
| `npm run build:cjs`     | compiles typescript into CommonJS           |
| `npm run build:esm`     | compiles typescript into ESM JS             |
| `npm run build`         | compiles typescript into ESM JS & CommonJS  |
| `npm run clean`         | deletes compiled JS                         |
| `npm run test`          | runs tests.                                 |
| `npm run test-coverage` | runs tests and includes coverage            |
| `npm run lint`          | runs linter                                 |
| `npm run lint:fix`      | runs linter and fixes auto-fixable problems |

## Environment Variables

| Environment Variable    | Value          | Description                                 |
| ----------------------- | -------------- | ------------------------------------------- |
| `IS_OFFLINE`            | true|false     | Uses a local DynamoDB instance for testing  |
| `AWS_REGION`            | ap-southeast-2 | The region where the DynamoDB tables should be created (https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html) |


# Developers Journal

- 20240711: Added `--timeout 10000` to the mocha command to stop tests from timing out after 2000ms. This was causing issues with the DynamoDB tests when running against a live database rather than a local instance. When connecting to regions with high latency, tests can take some time. Recommend using regions as close to home as possible.
- 20240715: Web5 requires splitting a tiebreak (where the sort key is equal) based on messageCid value. This is problematic in DynamoDB since you can only truly sort on one value which is your sort key. To get around this, I've added extra GSIs and populated with <attribute>+messageCid to allow sorting on that instead. So it'll order on the first section, and on tiebreaks it'll automatically sort using the messageCid tacked onto the end.

# Tips

When running tests and coming across errors, it's a bit difficult to narrow down the problem when the tests are external to this project.

Edit the below file to comment out whole sections of tests. This can reduce a tonne of noise when trying to run individual tests. E.g. Comment out everything except for testMessageStore(); then run tests again.
[text](node_modules/@tbd54566975/dwn-sdk-js/dist/esm/tests/test-suite.js)

You can go even more granular and drop into those methods to add debug logging to individual tests and comment out tests within those modules that aren't necessary.
