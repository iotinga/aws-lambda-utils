import { ExpressionAttributeNameMap, ExpressionAttributeValueMap } from 'aws-sdk/clients/dynamodb'
import * as AWS from 'aws-sdk'

export class DynamoStreamRecord implements ILambdaEvent {
  eventID: string
  eventName: string
  eventVersion: string
  eventSource: string
  awsRegion: string
  eventSourceARN: string
  dynamodb: {
    ApproximateCreationDateTime: number
    SequenceNumber: string
    SizeBytes: number
    StreamViewType: string
    Keys: any
    NewImage: any
    OldImage: any
  }

  constructor(rawData: {[x:string]: any} | string) {
    var data: {[x:string]: any}
    if (typeof rawData === 'string') {
      data = JSON.parse(rawData)
    } else {
      data = rawData
    }
    this.eventID = data?.eventID ?? Const.NULL_STR
    this.eventName = data?.eventName ?? Const.NULL_STR
    this.eventVersion = data?.eventVersion ?? Const.NULL_STR
    this.eventSource = data?.eventSource ?? Const.NULL_STR
    this.awsRegion = data?.awsRegion ?? Const.NULL_STR
    this.eventSourceARN = data?.eventSourceARN ?? Const.NULL_STR

    this.dynamodb = {
      ApproximateCreationDateTime: data?.dynamodb?.ApproximateCreationDateTime ?? 0,
      SequenceNumber: data?.dynamodb?.SequenceNumber ?? '0',
      SizeBytes: data?.dynamodb?.SizeBytes ?? 0,
      StreamViewType: data?.dynamodb?.StreamViewType ?? Const.NULL_STR,
      OldImage: data?.dynamodb?.OldImage,
      NewImage: data?.dynamodb?.NewImage,
      Keys: data?.dynamodb?.Keys,
    }

    if (typeof rawData != 'string') {
      if (typeof data?.dynamodb?.OldImage != Const.NULL_STR) {
        this.dynamodb.OldImage = AWS.DynamoDB.Converter.unmarshall(data?.dynamodb?.OldImage)
      }
      this.dynamodb.Keys = AWS.DynamoDB.Converter.unmarshall(data?.dynamodb?.Keys)
      this.dynamodb.NewImage = AWS.DynamoDB.Converter.unmarshall(data?.dynamodb?.NewImage)
    }
  }
}

export type ShadowKey = {
  id: string
}
export type ShadowPayload = {
  version: number
  timestamp?: number
  clientId?: string
  sk?: string
  metadata?: {
    desired?: { [x: string]: number | undefined }
    reported?: { [x: string]: number | undefined }
  }
  state?: {
    desired?: object
    reported?: object
  }
}
export type Shadow = ShadowKey & ShadowPayload

export interface ICrudService<K extends object, D extends object> {
  createOrUpdate(data: D): Promise<D | undefined>
  read(key: K): Promise<D | undefined>
  update(
    key: K,
    exp: string,
    expValues: ExpressionAttributeValueMap,
    expNames?: ExpressionAttributeNameMap,
    condition?: string
  ): Promise<D | undefined>
  delete(key: K): Promise<D | undefined>
}

export interface IShadowService extends ICrudService<ShadowKey, Shadow> {
  applyDelta: (delta:Shadow,writeConflic)
}
