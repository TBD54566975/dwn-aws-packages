import { DataStore, DataStream, DataStoreGetResult, DataStorePutResult } from '@tbd54566975/dwn-sdk-js';
import { Readable } from 'readable-stream';
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
  ScanCommandInput
} from '@aws-sdk/client-dynamodb';
import {
  marshall
} from '@aws-sdk/util-dynamodb';

export class DataStoreDynamoDB implements DataStore {

  #client: DynamoDBClient;
  #tableName = 'dataStore';

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
      if ( tableExists ) {
        return;
      }
    }


    const createTableInput = {
      AttributeDefinitions: [
        {
          AttributeName: 'tenant',
          AttributeType: 'S',
        } as AttributeDefinition,
        {
          AttributeName: 'recordIdDataCid',
          AttributeType: 'S',
        } as AttributeDefinition
      ],
      TableName: this.#tableName,
      KeySchema: [ 
        {
          AttributeName: 'tenant',
          KeyType: 'HASH',
        } as KeySchemaElement,
        {
          AttributeName: 'recordIdDataCid',
          KeyType: 'RANGE',
        } as KeySchemaElement,
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

  async close(): Promise<void> {
    this.#client.destroy();
  }

  async get(
    tenant: string,
    recordId: string,
    dataCid: string
  ): Promise<DataStoreGetResult | undefined> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `get`.'
      );
    }

    const input = {
      TableName : this.#tableName,
      Key: {
        'tenant': {
          S: tenant,
        },
        'recordIdDataCid': {
          S: recordId + '|' + dataCid
        }
      },
      AttributesToGet: [
        'tenant', 'recordId', 'dataCid', 'data'
      ]
    };
    const command = new GetItemCommand(input);
    const response = await this.#client.send(command);
    response.Item;

    if ( !response.Item ) {
      return undefined;
    }

    const result = {
      recordId: response.Item.recordId.S?.toString(),
      tenant: response.Item.tenant.S?.toString(),
      dataCid: response.Item.dataCid.S?.toString(),
      data: response.Item.data.B
    };

    return {
      dataSize: result.data ? result.data.length : 0,
      dataStream: new Readable({
        read() {
          this.push(result.data ? Buffer.from(result.data) : null);
          this.push(null);
        }
      }),
    };
  }

  async put(
    tenant: string,
    recordId: string,
    dataCid: string,
    dataStream: Readable
  ): Promise<DataStorePutResult> {

    const bytes = await DataStream.toBytes(dataStream);
    const data = Buffer.from(bytes);

    const input = {
      'Item': {
        'tenant': {
          'S': tenant
        },
        'recordId': {
          'S': recordId
        },
        'dataCid': {
          'S': dataCid
        },
        'recordIdDataCid': {
          'S': recordId + '|' + dataCid
        },
        'data': {
          'B': data
        }
      },
      'TableName': this.#tableName
    };
    const command = new PutItemCommand(input);
    await this.#client.send(command);

    return {
      dataSize: bytes.length
    };
  }

  async delete(
    tenant: string,
    recordId: string,
    dataCid: string
  ): Promise<void> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `delete`.'
      );
    }

    let deleteParams = {
      TableName: this.#tableName,
      Key: marshall({
        'tenant'          : tenant,
        'recordIdDataCid' : recordId + '|' + dataCid
      })
    };

    let deleteCommand = new DeleteItemCommand(deleteParams);
    await this.#client.send(deleteCommand);
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
              'recordIdDataCid': item.recordIdDataCid.S.toString()
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

}