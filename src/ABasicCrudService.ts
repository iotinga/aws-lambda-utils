import { logger } from 'aws-log'
import { DynamoDB } from 'aws-sdk'
import { DocumentClient, ExpressionAttributeNameMap, ExpressionAttributeValueMap } from 'aws-sdk/clients/dynamodb'
import { ICrudService } from '.'

export abstract class ABasicCrudService<K extends object, D extends object> implements ICrudService<K, D> {
  private log = logger().reloadContext()

  constructor(private client: DynamoDB.DocumentClient, private tableName: string) {}

  async createOrUpdate(data: D): Promise<D | undefined> {
    const dbInput: DocumentClient.PutItemInput = { TableName: this.tableName, Item: data, ReturnValues: 'ALL_OLD' }
    try {
      await this.client.put(dbInput).promise()
      this.log.info(`${this.constructor.name}.createOrUpdate`, dbInput)
      return data
    } catch (error) {
      this.log.warn(`${this.constructor.name}.createOrUpdate`, { dbInput, error })
      return undefined
    }
  }
  async read(key: K): Promise<D | undefined> {
    const dbInput: DocumentClient.GetItemInput = { TableName: this.tableName, Key: key }
    try {
      const result = await this.client.get(dbInput).promise()
      this.log.info(`${this.constructor.name}.read`, dbInput)
      return <D | undefined>result.Item
    } catch (error) {
      this.log.warn(`${this.constructor.name}.read`, { dbInput, error })
      return undefined
    }
  }
  async update(
    key: K,
    exp: string,
    expValues: ExpressionAttributeValueMap,
    expNames?: ExpressionAttributeNameMap,
    condition?: string
  ): Promise<D | undefined> {
    const dbInput: DocumentClient.UpdateItemInput = {
      TableName: this.tableName,
      Key: key,
      UpdateExpression: exp,
      ExpressionAttributeValues: expValues,
      ExpressionAttributeNames: expNames,
      ReturnValues: 'ALL_NEW',
      ConditionExpression: condition,
    }
    try {
      const result = await this.client.update(dbInput).promise()
      this.log.info(`${this.constructor.name}.update`, dbInput)
      return <D>result.Attributes
    } catch (error) {
      this.log.warn(`${this.constructor.name}.update`, { dbInput, error })
      return undefined
    }
  }
  async delete(key: K): Promise<D | undefined> {
    const dbInput: DocumentClient.DeleteItemInput = { TableName: this.tableName, Key: key, ReturnValues: 'ALL_OLD' }
    try {
      const result = await this.client.delete(dbInput).promise()
      this.log.info(`${this.constructor.name}.delete`, dbInput)
      return <D>result.Attributes
    } catch (error) {
      this.log.warn(`${this.constructor.name}.delete`, { dbInput, error })
      return undefined
    }
  }

  async query(exp: string, attrValues: ExpressionAttributeValueMap, index?: string): Promise<D[]> {
    const dbInput: DocumentClient.QueryInput = {
      TableName: this.tableName,
      IndexName: index,
      KeyConditionExpression: exp,
      ExpressionAttributeValues: attrValues,
    }
    try {
      const result = await this.client.query(dbInput).promise()
      this.log.info(`${this.constructor.name}.query`, dbInput)
      if (result.Items) {
        return <D[]>result.Items
      }
    } catch (error) {
      this.log.warn(`${this.constructor.name}.query`, { dbInput, error })
    }
    return []
  }
}
