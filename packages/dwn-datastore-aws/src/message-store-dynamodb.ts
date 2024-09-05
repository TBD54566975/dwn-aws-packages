import {
  DwnInterfaceName,
  DwnMethodName,
  executeUnlessAborted,
  Filter,
  GenericMessage,
  MessageStore,
  MessageStoreOptions,
  MessageSort,
  Pagination,
  SortDirection,
  PaginationCursor,
} from '@tbd54566975/dwn-sdk-js';

import { KeyValues } from './types.js';
import * as block from 'multiformats/block';
import * as cbor from '@ipld/dag-cbor';
import {
  DynamoDBClient,
  ListTablesCommand,
  CreateTableCommand,
  AttributeDefinition,
  KeySchemaElement,
  BillingMode,
  TableClass,
  GetItemCommand,
  PutItemCommand,
  ScanCommand,
  DeleteItemCommand,
  ScanCommandInput,
  QueryCommand,
  GlobalSecondaryIndex,
  QueryCommandInput
} from '@aws-sdk/client-dynamodb';
import {
  marshall
} from '@aws-sdk/util-dynamodb';
import { extractTagsAndSanitizeIndexes, replaceReservedWords } from './utils/sanitize.js';
import { sha256 } from 'multiformats/hashes/sha2';



export class MessageStoreDynamoDB implements MessageStore {
  #tableName = 'messageStoreMessages';
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
              AttributeName: 'messageCid',
              AttributeType: 'S',
            } as AttributeDefinition,
            {
              AttributeName: 'dateCreatedSort',
              AttributeType: 'S',
            } as AttributeDefinition,
            {
              AttributeName: 'datePublishedSort',
              AttributeType: 'S',
            } as AttributeDefinition,
            {
              AttributeName: 'messageTimestampSort',
              AttributeType: 'S',
            } as AttributeDefinition
          ],
          TableName : this.#tableName,
          KeySchema : [
            {
              AttributeName: 'tenant',
              KeyType: 'HASH',
            } as KeySchemaElement,
            {
              AttributeName: 'messageCid',
              KeyType: 'RANGE',
            } as KeySchemaElement,
          ],
          GlobalSecondaryIndexes: [
            {
              IndexName: 'dateCreated',
              KeySchema: [
                { AttributeName: 'tenant', KeyType: 'HASH' } as KeySchemaElement,
                { AttributeName: 'dateCreatedSort', KeyType: 'RANGE' } as KeySchemaElement
              ],
              Projection: {
                ProjectionType: 'ALL'
              }
            } as GlobalSecondaryIndex,
            {
              IndexName: 'datePublished',
              KeySchema: [
                  { AttributeName: 'tenant', KeyType: 'HASH' } as KeySchemaElement,
                  { AttributeName: 'datePublishedSort', KeyType: 'RANGE' } as KeySchemaElement
              ],
              Projection: {
                ProjectionType: 'ALL'
              }
            } as GlobalSecondaryIndex,
            {
              IndexName: 'messageTimestamp',
              KeySchema: [
                  { AttributeName: 'tenant', KeyType: 'HASH' } as KeySchemaElement,
                  { AttributeName: 'messageTimestampSort', KeyType: 'RANGE' } as KeySchemaElement
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

  async put(
    tenant: string,
    message: GenericMessage,
    indexes: KeyValues,
    options?: MessageStoreOptions
  ): Promise<void> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `put`.'
      );
    }

    options?.signal?.throwIfAborted();

    const getEncodedData = (message: GenericMessage): { message: GenericMessage, encodedData: string|null} => {
      let encodedData: string|null = null;
      if (message.descriptor.interface === DwnInterfaceName.Records && message.descriptor.method === DwnMethodName.Write) {
        const data = (message as any).encodedData as string|undefined;
        if(data) {
          delete (message as any).encodedData;
          encodedData = data;
        }
      }
      return { message, encodedData };
    };

    const { message: messageToProcess, encodedData} = getEncodedData(message);

    const encodedMessageBlock = await executeUnlessAborted(
      block.encode({ value: messageToProcess, codec: cbor, hasher: sha256}),
      options?.signal
    );

    const messageCid = encodedMessageBlock.cid.toString();
    const encodedMessageBytes = Buffer.from(encodedMessageBlock.bytes);

    // In SQL this is split into an insert into a tags table and the message table.
    // Since we're working with docs here, there should be no reason why we can't
    // put it in one write.
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
        'encodedMessageBytes': {
          'B': encodedMessageBytes
        },
        ...marshall(tags),
        ...marshall(fixIndexes)
      },
      'TableName': this.#tableName
    };

    // Adding special elements with messageCid concatenated, we use this for sorting where messageCid breaks tiebreaks
    if ( input.Item['dateCreated'] ) {
      input.Item['dateCreatedSort'] = { S: input.Item['dateCreated'].S + input.Item['messageCid'].S };
    }
    if ( input.Item['datePublished'] ) {
      input.Item['datePublishedSort'] = { S: input.Item['datePublished'].S + input.Item['messageCid'].S };
    }
    if ( input.Item['messageTimestamp'] ) {
      input.Item['messageTimestampSort'] = { S: input.Item['messageTimestamp'].S + input.Item['messageCid'].S };
    }

    if ( encodedData !== null ) {
      input.Item['encodedData'] = {
        'S': encodedData
      };
    }

    const command = new PutItemCommand(input);
    try {
      await this.#client.send(command);
    } catch ( error ) {
      console.error(error);
    }

  }

  async get(
    tenant: string,
    cid: string,
    options?: MessageStoreOptions
  ): Promise<GenericMessage | undefined> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `get`.'
      );
    }
    try {
      options?.signal?.throwIfAborted();

      const input = {
        TableName: this.#tableName, 
        Key : { 
          'tenant': {
            S: tenant,
          },
          'messageCid': {
            S: cid
          }
        },
        AttributesToGet: [
          'tenant', 'messageCid', 'encodedMessageBytes', 'encodedData'
        ]
      };
      const command = new GetItemCommand(input);
      const response = await executeUnlessAborted(
        this.#client.send(command),
        options?.signal
      );

      if ( !response.Item ) {
        return undefined;
      }

      const result = {
        tenant: response.Item.tenant.S?.toString(),
        messageCid: response.Item.messageCid.S?.toString(),
        encodedMessageBytes: response.Item.encodedMessageBytes.B,
        encodedData: response.Item.encodedData?.S?.toString()
      };

      const responseData = await this.parseEncodedMessage(
        result.encodedMessageBytes ? result.encodedMessageBytes: Buffer.from(''), 
        result.encodedData, options
      );
      return responseData;
    } catch ( error ) {
      console.error(error);
    }
  }

  async query(
    tenant: string,
    filters: Filter[],
    messageSort?: MessageSort,
    pagination?: Pagination,
    options?: MessageStoreOptions
  ): Promise<{ messages: GenericMessage[], cursor?: PaginationCursor}> {

    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `query`.'
      );
    }

    options?.signal?.throwIfAborted();

    try {
      const { property: sortProperty, direction: sortDirection } = this.extractSortProperties(messageSort);

      const filterDynamoDB: any = [];
      const expressionAttributeValues = {};

      // Dynamically generate a filter that will run server side in DynamoDB
      for (const [index, filter] of filters.entries()) {
        const constructFilter = {
          FilterExpression: '',
        };
        const conditions: string[] = [];
        for ( const keyRaw in filter ) {
          // `schema` and `method` are reserved keywords so we replace them here
          const key = (keyRaw == 'schema' ? 'xschema' : keyRaw == 'method' ? 'xmethod' : keyRaw).replace('.', '');

          constructFilter.FilterExpression += key;
          const value = filter[keyRaw];
          if (typeof value === 'object') {
            if (value['gt']) {
              conditions.push(key + ' > :x' + key + index + 'GT');
              expressionAttributeValues[':x' + key +  index + 'GT'] = value['gt'];
            }
            if (value['gte']) {
              conditions.push(key + ' >= :x' + key + index + 'GTE');
              expressionAttributeValues[':x' + key + index + 'GTE'] = value['gte'];
            }
            if (value['lt']) {
              conditions.push(key + ' < :x' + key + index + 'LT');
              expressionAttributeValues[':x' + key + index + 'LT'] = value['lt'];
            }
            if (value['lte']) {
              conditions.push(key + ' <= :x' + key + index + 'LTE');
              expressionAttributeValues[':x' + key + index + 'LTE'] = value['lte'];
            }
          } else {
            conditions.push(key + ' = :x' + key + index + 'EQ');
            // we store booleans as a string in dynamodb, so check the value type and convert to string if required
            expressionAttributeValues[':x' + key + index + 'EQ'] = typeof filter[keyRaw] === 'boolean' ? filter[keyRaw].toString() : filter[keyRaw];
          }
        }

        // handle empty filters
        if ( conditions.length > 0 ) {
          filterDynamoDB.push('(' + conditions.join(' AND ') + ')');
        }

      }

      let params: any = this.cursorInputSort(tenant, pagination, sortProperty, sortDirection, filters);
      expressionAttributeValues[':tenant'] = tenant;
      params['ExpressionAttributeValues'] = marshall(expressionAttributeValues);
      const filterExp = filterDynamoDB.join(' OR ');
      if ( filterExp ) {
        params.FilterExpression = filterExp;
      }
      const command = new QueryCommand(params);
      const data = await executeUnlessAborted(
        this.#client.send(command),
        options?.signal
      );

      delete params['Limit'];

      if ( data.ScannedCount !== undefined && data.Items !== undefined && data.ScannedCount > 0 && data.LastEvaluatedKey ) {
        let matches = true;
        for ( const key in data.LastEvaluatedKey ){
          if ( data.LastEvaluatedKey[key] !== data.Items[data.ScannedCount - 1].S ) {
            matches = false;
          }
        }
        if ( matches ) {
          delete data['LastEvaluatedKey'];
        }
      }

      // Extract and return the items from the response
      if (data.Items) {
        const results = await this.processPaginationResults(data.Items, sortProperty, data.LastEvaluatedKey, pagination?.limit, options);
        return results;
      } else {
        return { messages: []};
      }
    } catch (err) {
      console.error('Error retrieving items:', err);
      throw err;
    }
  }

  async delete(
    tenant: string,
    cid: string,
    options?: MessageStoreOptions
  ): Promise<void> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `delete`.'
      );
    }

    options?.signal?.throwIfAborted();

    let deleteParams = {
      TableName : this.#tableName,
      Key: marshall({
        'tenant': tenant,
        'messageCid': cid
      })
    };
    let deleteCommand = new DeleteItemCommand(deleteParams);
    await executeUnlessAborted(
      this.#client.send(deleteCommand),
      options?.signal
    );

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
            Key: marshall({
              'tenant': item.tenant.S.toString(),
              'messageCid': item.messageCid.S.toString()
            })
          };

          let deleteCommand = new DeleteItemCommand(deleteParams);
          await this.#client.send(deleteCommand);
        }

        // Continue scanning if we have more items
        scanParams.ExclusiveStartKey = scanResult.LastEvaluatedKey;

      } while (scanResult.LastEvaluatedKey);

    } catch (err) {
      console.error('Unable to clear table:', err);
    }
  }

  sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private async parseEncodedMessage(
    encodedMessageBytes: Uint8Array,
    encodedData: string | null | undefined,
    options?: MessageStoreOptions
  ): Promise<GenericMessage> {
    options?.signal?.throwIfAborted();

    const decodedBlock = await block.decode({
      bytes  : encodedMessageBytes,
      codec  : cbor,
      hasher : sha256
    });

    const message = decodedBlock.value as GenericMessage;
    // If encodedData is stored within the MessageStore we include it in the response.
    // We store encodedData when the data is below a certain threshold.
    // https://github.com/TBD54566975/dwn-sdk-js/pull/456
    if (message !== undefined && encodedData !== undefined && encodedData !== null) {
      (message as any).encodedData = encodedData;
    }
    return message;
  }

  /**
   * Processes the paginated query results.
   * Builds a pagination cursor if there are additional messages to paginate.
   * Accepts more messages than the limit, as we query for additional records to check if we should paginate.
   *
   * @param messages a list of messages, potentially larger than the provided limit.
   * @param limit the maximum number of messages to be returned
   *
   * @returns the pruned message results and an optional pagination cursor
   */
  private async processPaginationResults(
    results: any[],
    sortProperty: string,
    lastEvaluatedKey: any,
    limit?: number,
    options?: MessageStoreOptions,
  ): Promise<{ messages: GenericMessage[], cursor?: PaginationCursor}> {
    // we queried for one additional message to determine if there are any additional messages beyond the limit
    // we now check if the returned results are greater than the limit, if so we pluck the last item out of the result set
    // the cursor is always the last item in the *returned* result so we use the last item in the remaining result set to build a cursor
    let cursor: PaginationCursor | undefined;
    if (limit !== undefined && results.length > limit) {
      results = results.slice(0, limit);
      const lastMessage = results.at(-1);
      const cursorValue = {};
      cursorValue['tenant'] = lastMessage['tenant'];
      cursorValue[sortProperty + 'Sort'] = lastMessage[sortProperty + 'Sort'];
      cursorValue['messageCid'] = lastMessage['messageCid'];
      cursor = { messageCid: JSON.stringify(cursorValue), value: JSON.stringify(cursorValue) };
    }

    // extracts the full encoded message from the stored blob for each result item.
    const messages: Promise<GenericMessage>[] = results.map(r => this.parseEncodedMessage(new Uint8Array(r.encodedMessageBytes.B), r.encodedData?.S, options));
    return { messages: await Promise.all(messages), cursor };
  }

  /**
   * Extracts the appropriate sort property and direction given a MessageSort object.
   */
  private extractSortProperties(
    messageSort?: MessageSort
  ):{ property: 'dateCreated' | 'datePublished' | 'messageTimestamp', direction: SortDirection } {
    if(messageSort?.dateCreated !== undefined)  {
      return  { property: 'dateCreated', direction: messageSort.dateCreated };
    } else if(messageSort?.datePublished !== undefined) {
      return  { property: 'datePublished', direction: messageSort.datePublished };
    } else if (messageSort?.messageTimestamp !== undefined) {
      return  { property: 'messageTimestamp', direction: messageSort.messageTimestamp };
    } else {
      return  { property: 'messageTimestamp', direction: SortDirection.Ascending };
    }
  }

  /**
   * Extracts the appropriate sort property and direction given a MessageSort object.
   */
  private cursorInputSort(
    tenant: string,
    pagination: Pagination|undefined,
    sortAttribute: string,
    sortDirection: SortDirection,
    filters: Filter[]
  ): any {
    const direction = sortDirection == SortDirection.Ascending ? true : false;
    const params: QueryCommandInput = {
      TableName: this.#tableName,
      KeyConditionExpression: '#tenant = :tenant',
      ExpressionAttributeNames: {
        '#tenant': 'tenant'
      },
      ExpressionAttributeValues: marshall({
        ':tenant': tenant
      }),
      ScanIndexForward: direction,

    };

    if ( sortAttribute ) {
      params['IndexName'] = sortAttribute;
      if ( direction ) {
        params['ScanIndexForward'] = direction;
      }
    }

    if ( pagination?.limit ) {
      params['Limit'] = (pagination.limit * filters.length) + 1;
    }
    if ( pagination?.cursor ) {
      params['ExclusiveStartKey'] = JSON.parse(pagination.cursor.messageCid);
    }
    return params;
  }
}