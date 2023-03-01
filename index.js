
import dotenv from 'dotenv'
dotenv.config()
import { ScanCommand } from "@aws-sdk/client-dynamodb";
import { ddbClient } from "./DynamoDb.js";



const params = {
    ProjectionExpression: "id, email",

    TableName: process.env.TABLE_NAME || "",
};



(async () => {
    const data = await ddbClient.send(new ScanCommand(params));
    data.Items.forEach(item => {
        console.log(item.id.S, item.email.S)
    })
})()

