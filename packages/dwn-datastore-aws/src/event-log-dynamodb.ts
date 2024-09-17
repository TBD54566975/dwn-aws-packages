import type { EventLog, Filter, PaginationCursor } from '@tbd54566975/dwn-sdk-js';
import { extractTagsAndSanitizeIndexes } from './utils/sanitize-events.js';
import { replaceReservedWords } from './utils/sanitize.js';
import {
  marshall
} from '@aws-sdk/util-dynamodb';
import {
  DynamoDBClient,
  ListTablesCommand,
  CreateTableCommand,
  AttributeDefinition,
  KeySchemaElement,
  BillingMode,
  TableClass,
  PutItemCommand,
  ScanCommand,
  DeleteItemCommand,
  ScanCommandInput,
  GlobalSecondaryIndex,
  BatchWriteItemCommand,
  BatchWriteItemCommandInput,
  UpdateItemCommand,
  ReturnValue,
  QueryCommandInput,
  QueryCommand
} from '@aws-sdk/client-dynamodb';

export class EventLogDynamoDB implements EventLog {
  #tableName = 'eventLog';
  #client: DynamoDBClient;

  constructor() {
    if ( process.env.IS_OFFLINE == 'true' ) {
      this.#client = new DynamoDBClient({
        region: 'localhost',
        endpoint: 'http://0.0.0.0:8006',
        credentials: {
          accessKeyId: 'MockAccessKeyId',
          secretAccessKey: 'MockSecretAccessKey'
        },
      });
    } else {
      this.#client = new DynamoDBClient({
        region: process.env.AWS_REGION ? process.env.AWS_REGION : 'ap-southeast-2'
      });
    }
  }

  async open(): Promise<void> {

    const input = {};
    const command = new ListTablesCommand(input);
    const response = await this.#client.send(command);

    // Does table already exist?
    if ( response.TableNames ) {
      const tableExists = response.TableNames?.length > 0 && response.TableNames?.indexOf(this.#tableName) !== -1;
      if ( !tableExists ) {
        const createTableInput = {
          AttributeDefinitions: [
            {
              AttributeName: 'tenant',
              AttributeType: 'S',
            } as AttributeDefinition,
            {
              AttributeName: 'watermark',
              AttributeType: 'N',
            } as AttributeDefinition,
            {
              AttributeName: 'messageCid',
              AttributeType: 'S',
            } as AttributeDefinition,
          ],
          TableName: this.#tableName,
          KeySchema: [
            {
              AttributeName: 'tenant',
              KeyType: 'HASH',
            } as KeySchemaElement,
            {
              AttributeName: 'messageCid',
              KeyType : 'RANGE',
            } as KeySchemaElement,
          ],
          GlobalSecondaryIndexes: [
            {
              IndexName: 'watermark',
              KeySchema: [
                { AttributeName: 'tenant', KeyType: 'HASH' } as KeySchemaElement,
                { AttributeName: 'watermark', KeyType: 'RANGE' } as KeySchemaElement 
              ],
              Projection: {
                ProjectionType: 'ALL'
              }
            } as GlobalSecondaryIndex
          ],
          BillingMode: 'PAY_PER_REQUEST' as BillingMode,
          TableClass: 'STANDARD' as TableClass,
        };

        const createTableCommand = new CreateTableCommand(createTableInput);

        try {
          await this.#client.send(createTableCommand);
        } catch ( error ) {
          console.error(error);
        }
      }
    }
  }

  async close(): Promise<void> {
    this.#client.destroy();
  }

  async append(
    tenant: string,
    messageCid: string,
    indexes: Record<string, string | boolean | number>
  ): Promise<void> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `append`.'
      );
    }

    try {
      // This is the DynamoDB version of an atomic, auto incrementing index in an SQL database
      const counterParams = {
        TableName: this.#tableName,
        Key: { 'tenant': { S: tenant + '_counter'}, 'messageCid': { S: 'counter' } },
        UpdateExpression: 'SET #count = if_not_exists(#count, :start) + :incr',
        ExpressionAttributeNames: { '#count': 'count' },
        ExpressionAttributeValues: {
          ':incr'  : { N: '1' }, // Increment value
          ':start' : { N: '0' } // Initial value if 'count' does not exist
        },
        ReturnValues: 'UPDATED_NEW' as ReturnValue
      };

      const updateCommand = new UpdateItemCommand(counterParams);
      const updateResult = await this.#client.send(updateCommand);
      const incNumber: string = updateResult.Attributes?.['count']?.N ?? '';
      const incrementedCounter = parseInt(incNumber, 10);
      const { indexes: putIndexes, tags } = extractTagsAndSanitizeIndexes(indexes);
      const fixIndexes = replaceReservedWords(putIndexes);
      const input = {
        'Item': {
          'tenant': {
            'S': tenant
          },
          'messageCid': {
            'S': messageCid
          },
          ...marshall(tags),
          ...marshall(fixIndexes),
          'watermark': { N: incrementedCounter.toString() }
        },
        'TableName': this.#tableName
      };


      const command = new PutItemCommand(input);
      await this.#client.send(command);
    } catch ( error ) {
      console.error(error);
    }
  }

  async getEvents(
    tenant: string,
    cursor?: PaginationCursor
  ): Promise<{events: string[], cursor?: PaginationCursor }> {

    // get events is simply a query without any filters. gets all events beyond the cursor.
    return this.queryEvents(tenant, [], cursor);
  }

  async queryEvents(
    tenant: string,
    filters: Filter[],
    cursor?: PaginationCursor
  ): Promise<{events: string[], cursor?: PaginationCursor }> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `queryEvents`.'
      );
    }

    try {
      const filterDynamoDB: any = [];
      const expressionAttributeValues = {};

      // Build a filter dynamically, this will get executed server side, however this may 
      // have a performance impact as not all fields are indexes
      for (const filter of filters) {
        const constructFilter = {
          FilterExpression: '',
        };
        const conditions: string[] = [];
        for ( const keyRaw in filter ) {
          // `schema` and `method` are reserved keywords so we replace them here
          const key = keyRaw == 'schema' ? 'xschema' : keyRaw == 'method' ? 'xmethod' : keyRaw;
          constructFilter.FilterExpression += key;
          const value = filter[key];
          if (typeof value === 'object') {
            if (value['gt']) {
              conditions.push(key + ' > :x' + key + 'GT');
              expressionAttributeValues[':x' + key + 'GT'] = value['gt'];
            }
            if (value['gte']) {
              conditions.push(key + ' >= :x' + key + 'GTE');
              expressionAttributeValues[':x' + key + 'GTE'] = value['gte'];
            }
            if (value['lt']) {
              conditions.push(key + ' < :x' + key + 'LT');
              expressionAttributeValues[':x' + key + 'LT'] = value['lt'];
            }
            if (value['lte']) {
              conditions.push(key + ' <= :x' + key + 'LTE');
              expressionAttributeValues[':x' + key + 'LTE'] = value['lte'];
            }
          } else {
            conditions.push(key + ' = :x' + key + 'EQ');
            expressionAttributeValues[':x' + key + 'EQ'] = filter[keyRaw].toString();
          }
        }
        filterDynamoDB.push('(' + conditions.join(' AND ') + ')');
      }

      expressionAttributeValues[':tenant'] = tenant;


      const filterExp = filterDynamoDB.join(' OR ');

      const params: QueryCommandInput = {
        TableName: this.#tableName,
        IndexName: 'watermark',
        KeyConditionExpression: '#tenant = :tenant',
        ExpressionAttributeNames: {
          '#tenant': 'tenant'
        },
        ExpressionAttributeValues : marshall(expressionAttributeValues),
        ScanIndexForward          : true,
      };

      if ( filterExp ) {
        params.FilterExpression = filterExp;
      }

      if ( cursor ) {
        params['ExclusiveStartKey'] = JSON.parse(cursor.messageCid);
      }


      const command = new QueryCommand(params);
      const data = await this.#client.send(command);

      if( data.Items ){
        const events: string[] = [];
        const lastMessage: any = data.Items.at(-1);
        const cursorValue = {};
        if ( lastMessage !== undefined ) {
          cursorValue['tenant'] = lastMessage['tenant'];
          cursorValue['messageCid'] = lastMessage['messageCid'];
          cursorValue['watermark'] = lastMessage['watermark'];
          cursor = { messageCid: JSON.stringify(cursorValue), value: JSON.stringify(cursorValue) };
        }


        for (let { messageCid } of data.Items) {
          // Send empty string if messageCid.S is undefined
          events.push(messageCid?.S ?? '');
        }

        return { events, cursor };
      }

    } catch (error) {
      console.error(error);
    }

    return { events: [], cursor: {messageCid: "", value: -1} };
  }

  async deleteEventsByCid(
    tenant: string,
    messageCids: Array<string>
  ): Promise<void> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `delete`.'
      );
    }

    if (messageCids.length === 0) {
      return;
    }

    const keysToDelete: any = [
    ];

    for ( const messageCid of messageCids ) {
      keysToDelete.push( { 'tenant': { S: tenant}, 'messageCid': { S: messageCid} } );
    }

    await this.deleteItems(keysToDelete);

  }

  async deleteItems(keysToDelete: { [key: string]: any }[]) {

    // Prepare requests in batches of 25 (DynamoDB batchWriteItem limit)
    const batchSize = 25;
    const batches: BatchWriteItemCommandInput[] = [];

    for (let i = 0; i < keysToDelete.length; i += batchSize) {
      const batchKeys = keysToDelete.slice(i, i + batchSize);

      const deleteRequests = batchKeys.map(key => ({
        DeleteRequest: {
          Key: key
        }
      }));

      batches.push({
        RequestItems: {
          [this.#tableName]: deleteRequests
        }
      });
    }

    for (const batch of batches) {
      const command = new BatchWriteItemCommand(batch);
      try {
        await this.#client.send(command);
      } catch (error) {
        console.error('Error deleting batch:', error);
      }
    }
  }

  async clear(): Promise<void> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `clear`.'
      );
    }

    try {
      let scanParams: ScanCommandInput = {
        TableName: this.#tableName
      };

      let scanCommand = new ScanCommand(scanParams);
      let scanResult;

      do {
        scanResult = await this.#client.send(scanCommand);

        // Delete each item
        for (let item of scanResult.Items) {
          let deleteParams = {
            TableName: this.#tableName,
            Key: {
              'tenant': { S: item.tenant.S.toString() },
              'messageCid': { S: item.messageCid.S.toString() }
            }
          };
          let deleteCommand = new DeleteItemCommand(deleteParams);
          await this.#client.send(deleteCommand);
        }

        scanParams.ExclusiveStartKey = scanResult.LastEvaluatedKey;

      } while (scanResult.LastEvaluatedKey);

    } catch (err) {
      console.error('Unable to clear table:', err);
    }
  }
}