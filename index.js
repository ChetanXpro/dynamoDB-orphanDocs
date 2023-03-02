
import dotenv from 'dotenv'
dotenv.config()
import { ScanCommand, TransactGetItemsCommand } from "@aws-sdk/client-dynamodb";
import { ddbClient } from "./DynamoDb.js";



const params = {
    ProjectionExpression: "id, email",
    TableName: process.env.TABLE_NAME || "",
};

const params2 = {
    ReturnConsumedCapacity: 'INDEXES',
    TransactItems: [
        {
            Get: {
                TableName: 'User-apobio35frg77c7rjko75fo4ii-dev',
                Key: { id: '8d280b94-921b-5c9d-a4ad-e9456eac142b' }
            }
        }

    ]
}



// (async () => {
//     const data = await ddbClient.send(new ScanCommand(params));
//     data.Items.forEach(item => {
//         console.log(item.id.S, item.email.S)
//     })
// })()
const fet = async () => {
    console.log(`Run`)
    const data = await ddbClient.send(new TransactGetItemsCommand(params2));
    console.log(data)
}


fet()
    // (async () => {
    //     console.log(`Run`)
    //     const data = await ddbClient.send(new TransactGetItemsCommand(params));
    //     console.log(data.Item)

    // })()

