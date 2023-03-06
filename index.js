
import dotenv from 'dotenv'
dotenv.config()
import { ScanCommand, TransactGetItemsCommand, TransactWriteItemsCommand } from "@aws-sdk/client-dynamodb";
import { ddbClient } from "./DynamoDb.js";


const tables = [{
    "Chapter-apobio35frg77c7rjko75fo4ii-dev": ["createdByUserId", "lastModifiedByUserId"]
},
{
    "Comment-apobio35frg77c7rjko75fo4ii-dev": ["commentedByUserId"]
}
    ,
{
    "Doc-apobio35frg77c7rjko75fo4ii-dev": ["createdByUserId", "lastModifiedByUserId"]
},
{
    "DocItem-apobio35frg77c7rjko75fo4ii-dev": ["createdByUserId", "lastModifiedByUserId"]
}
    ,
{
    "GroupTask-apobio35frg77c7rjko75fo4ii-dev": ["createdByUserId", "updatedByUserId"]
},
{
    "Lead-apobio35frg77c7rjko75fo4ii-dev": ["approvedByUserId", "createdByUserId", "disqualifiedByUserId", "lastModifiedByUserId", "qualifiedByUserId", "rejectedByUserId"]
},
{
    "ProductDoc-apobio35frg77c7rjko75fo4ii-dev": ["createdByUserId", "lastModifiedByUserId"]
},
{
    "Task-apobio35frg77c7rjko75fo4ii-dev": ["requestedByUserId"]
},
{
    "UserGroup-apobio35frg77c7rjko75fo4ii-dev": ["userId"]
},
{
    "UserTask-apobio35frg77c7rjko75fo4ii-dev": ["createdByUserId", "updatedByUserId", "userId"]
},
{
    "WatcherTask-apobio35frg77c7rjko75fo4ii-dev": ["userId"]
}
    ,
{
    "User-apobio35frg77c7rjko75fo4ii-dev": ["createdBy"]
}
]





// fet()









const tabless = [{
    "ProductDoc-apobio35frg77c7rjko75fo4ii-dev": ["createdByUserId", "lastModifiedByUserId"]
}
]
let final = []

const scan = async (params, userId, ret, tableName) => {
    const scanCommand = new ScanCommand(params);
    const res = await ddbClient.send(scanCommand)
    let txn = {}
    let gg = []
    res.Items.forEach(i => {
        ret.split(',').forEach(e => {
            // console.log(i[e]?.S)
            if (userId !== i[e]?.S) return

            const khi = {}
            const currVal = i[e].S

            let id = i['id'].S
            let bye = {}
            bye[e] = currVal
            khi[id] = bye
            gg.push(khi)
        })
    })

    txn = { [tableName]: [...gg] }


    final.push(txn)




}



const massScan = async (userId) => {
    let final = []

    let allRequest = []
    const scanParams = async (item) => {


        const okkk = item[Object.keys(item)[0]].map((i, x) => {


            if (item[Object.keys(item)[0]].length === x + 1) {
                return `${i} = :${i}`
            }

            if (x === 0) {
                return `${i} = :${i} OR`
            }
            return `${i} = :${i} OR`
        })


        let filter = ''
        okkk.forEach(h => {
            filter = filter + h + " "
        })


        const val = item[Object.keys(item)[0]].map((i, x) => {

            return { [":" + i]: { S: userId } }

        })

        let ret = ''

        item[Object.keys(item)[0]].forEach((e, i) => {

            if (i === 0) {
                return ret = ret + e
            }
            ret = ret = ret + "," + e

        });

        const tableName = Object.keys(item)[0]

        const params = {
            FilterExpression: filter,
            ExpressionAttributeValues: val.reduce((accumulator, currentValue) => {
                const key = Object.keys(currentValue)[0];
                const value = currentValue[key];
                accumulator[`${key}`] = value;
                return accumulator;
            }, {}),
            ProjectionExpression: `id, ${ret}`,
            TableName: tableName,
        };


        allRequest.push(scan(params, userId, ret, tableName))



    }

    for (const item of tables) {
        await scanParams(item);
    }

    await Promise.all(allRequest)

}


const txn = async (data) => {
    let track = {}

    data.forEach((e) => {
        const currKey = Object.keys(e)


    })



    let params = {
        'TransactItems': [
            {
                "Update": {

                    "TableName": 'User-apobio35frg77c7rjko75fo4ii-dev',

                    "Key": {
                        "id": { "S": "8d280b94-921b-5c9d-a4ad-e9456eac142b" }
                    },
                    "UpdateExpression": "set lastName = :lastName, firstName = :firstName",
                    "ExpressionAttributeValues": {
                        ":lastName": {
                            "S": 'baliyannn'
                        },
                        ":firstName": {
                            "S": 'chetannnn'
                        }
                    }
                }
            }

        ]
    }

    // const data = await ddbClient.send(new TransactWriteItemsCommand(params));
    // console.log(data)
}



const runScan = async () => {

    await massScan("8d280b94-921b-5c9d-a4ad-e9456eac142b")
    txn(final)

}

runScan()





// 8d280b94-921b-5c9d-a4ad-e9456eac142b









