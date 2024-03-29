
import dotenv from 'dotenv'
dotenv.config()
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const REGION = process.env.REGION || ""; 

const ddbClient = new DynamoDBClient({ region: REGION });
export { ddbClient };