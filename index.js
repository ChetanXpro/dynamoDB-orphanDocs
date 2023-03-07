
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

]


const tabless = [{
    "ProductDoc-apobio35frg77c7rjko75fo4ii-dev": ["createdByUserId", "lastModifiedByUserId"]
}
]
let final = []
let versionFinal = []
let sortKeyMap = {}
const sortKey = ['Chapter-apobio35frg77c7rjko75fo4ii-dev', 'Doc-apobio35frg77c7rjko75fo4ii-dev', 'DocItem-apobio35frg77c7rjko75fo4ii-dev', 'ProductDoc-apobio35frg77c7rjko75fo4ii-dev']

const scan = async (params, userId, ret, tableName) => {
    const scanCommand = new ScanCommand(params);
    const res = await ddbClient.send(scanCommand)
    let txn = {}
    // console.log(res.Items)
    let gg = []
    let sortKeyArr = []
    let captureVersion = []

    res.Items.forEach(i => {
        // console.log(i)


        ret.split(',').forEach(e => {

            if (userId !== i[e]?.S) return

            if (sortKey.includes(tableName)) {
                // console.log(i.version)
                // sortKeyMap[tableName]



                let version = i['version'].S
                // captureVersion.push({ 'V': version })



                const khi = {}
                const currVal = i[e].S

                let id = i['id'].S
                let bye = {}
                bye[e] = currVal
                khi[id] = bye
                // console.log(khi)
                // console.log('push')
                sortKeyArr.push(khi, { 'V': version })
                // console.log('version', tableName)
                return
            }
            // console.log('not version', tableName)


            const khi = {}
            const currVal = i[e].S

            let id = i['id'].S
            let bye = {}
            bye[e] = currVal
            khi[id] = bye

            gg.push(khi)
        })
    })
    if (sortKey.includes(tableName)) {


        sortKeyMap = { [tableName]: [...sortKeyArr] }
        // console.log(sortKeyMap)
        versionFinal.push(sortKeyMap)
    }
    txn = { [tableName]: [...gg] }
    if (!sortKey.includes(tableName)) {
        final.push(txn)

    }

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
            ProjectionExpression: `id,version, ${ret}`,
            TableName: tableName,
        };


        allRequest.push(scan(params, userId, ret, tableName))



    }

    for (const item of tables) {
        await scanParams(item);
    }

    await Promise.all(allRequest)

}

let newKey = '83485ed8-4fd2-5852-9ce5-ec3703cf79a5'

const txn = async (data) => {
    let track = {}
    console.log(data)
    let pram = []
    let pram2 = []

   

    data.forEach((e) => {
        const currKey = Object.keys(e)[0]

        // console.log(currKey)
        console.log(e[currKey])



        if (e[currKey].length == 0) return

        // if (sortKey.includes(currKey)) return



        e[currKey].forEach(i => {
            // console.log(i)


            // console.log(`Without version`, currKey)
            let innerObjKey = Object.keys(i[Object.keys(i)])[0]


            let innerObjValue = i[Object.keys(i)][innerObjKey]




            let expValue = `:${innerObjKey}`

            let obj = {
                "Update": {

                    "TableName": currKey.toString(),

                    "Key": {
                        "id": { "S": Object.keys(i).toString() }
                    },
                    "UpdateExpression": `set ${innerObjKey} = :${innerObjKey}`,
                    "ExpressionAttributeValues": {
                        [expValue]: {
                            "S": `${newKey}`
                        }
                    }
                }
            }
            pram.push(obj)
        })
    })
    // pram.forEach(e=>{
    //     console.log(e.Update.TableName)
    //     console.log(e.Update.Key)
    //     console.log(e.Update.ExpressionAttributeValues)
    // })
    // console.log(pram.length)



    let paramss = {
        'TransactItems': [
            {
                "Update": {

                    "TableName": 'Chapter-apobio35frg77c7rjko75fo4ii-dev',

                    "Key": {
                        "id": { "S": "b86ca222-3efe-4259-be9c-e14c78063cff" },
                        "version": { "S": "1" }
                    },
                    "UpdateExpression": "set description = :description",
                    "ExpressionAttributeValues": {
                        ":description": {
                            "S": 'test desc'
                        }
                    }
                }
            }

        ]
    }
    // console.log(paramss.TransactItems[0])
    let params = {
        'TransactItems': [
            ...pram
        ]
    }
    // console.log(params.TransactItems[0])

    // const txnReq = await ddbClient.send(new TransactWriteItemsCommand(paramss));
    // // 

    // console.log(txnReq)
}



const runScan = async () => {

    await massScan("8d280b94-921b-5c9d-a4ad-e9456eac142b")
    // console.log(final)
    txn([...final, ...versionFinal])
    // console.log(versionFinal)
}

runScan()





// 8d280b94-921b-5c9d-a4ad-e9456eac142b

//new
// 83485ed8-4fd2-5852-9ce5-ec3703cf79a5









// 8d280b94-921b-5c9d-a4ad-e9456eac142b