import { ResumableTaskStoreDynamoDB } from '../src/resumable-task-store-dynamodb.js';
import { DataStoreDynamoDB } from '../src/data-store-dynamodb.js';
import { EventLogDynamoDB } from '../src/event-log-dynamodb.js';
import { MessageStoreDynamoDB } from '../src/message-store-dynamodb.js';
import { TestSuite } from '@tbd54566975/dwn-sdk-js/tests';

// Remove when we Node.js v18 is no longer supported by this project.
// Node.js v18 maintenance begins 2023-10-18 and is EoL 2025-04-30: https://github.com/nodejs/release#release-schedule
import { webcrypto } from 'node:crypto';
// @ts-expect-error ignore type mismatch
if (!globalThis.crypto) globalThis.crypto = webcrypto;

describe('NoSQL Store Test Suite', () => {

  describe('DynamoDB Support', () => {
    TestSuite.runStoreDependentTests({
      messageStore       : new MessageStoreDynamoDB(),
      dataStore          : new DataStoreDynamoDB(),
      eventLog           : new EventLogDynamoDB(),
      resumableTaskStore : new ResumableTaskStoreDynamoDB(),
    });
  });

});