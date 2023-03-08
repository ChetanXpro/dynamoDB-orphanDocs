
import dotenv from 'dotenv'
dotenv.config()
import { ScanCommand, TransactGetItemsCommand, TransactWriteItemsCommand } from "@aws-sdk/client-dynamodb";
import { ddbClient } from "./DynamoDb.js";


// List of all tables which we want to target and in value we have to put all fiels which contains user's id
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






// List of tables which have two keys (primarykey + sortKey)
const sortKey = ['Chapter-apobio35frg77c7rjko75fo4ii-dev', 'Doc-apobio35frg77c7rjko75fo4ii-dev', 'DocItem-apobio35frg77c7rjko75fo4ii-dev', 'ProductDoc-apobio35frg77c7rjko75fo4ii-dev']



let final = []
let versionFinal = []
let sortKeyMap = {}



const scan = async (params, userId, ret, tableName) => {
    const scanCommand = new ScanCommand(params);
    const res = await ddbClient.send(scanCommand)
    let txn = {}
    let gg = []
    let sortKeyArr = []


    res.Items.forEach(i => {
       


        ret.split(',').forEach(e => {

            if (userId !== i[e]?.S) return

            if (sortKey.includes(tableName)) {




                let version = i['version'].S




                const khi = {}
                const currVal = i[e].S

                let id = i['id'].S
                let bye = {}
                bye[e] = currVal
                const conObj = Object.assign({}, bye, { 'V': version })
                khi[id] = conObj

                sortKeyArr.push(khi)

                return
            }



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

        // Create ProjectionExpression values
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


        // Construct FilterExpression value
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

        // Params for scan
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

        // Pushiing all promise to an array
        allRequest.push(scan(params, userId, ret, tableName))
    }

    for (const item of tables) {
        await scanParams(item);
    }

    await Promise.all(allRequest)

}





const txn = async (data, newKey) => {
    let track = {}
    // console.log(data)
    let pram = []
    let pram2 = []


    // Create params for Transaction

    data.forEach((e) => {
        const currKey = Object.keys(e)[0]

        if (e[currKey].length == 0) return


        // Merge two objects which have same keys
        const map = new Map();
        const mergeSameKeyObjects = e[currKey].forEach(i => {

            const key = Object.keys(i)[0];
            const value = i[key];

            if (map.has(key)) {
                map.set(key, { ...map.get(key), ...value });
            } else {
                map.set(key, value);
            }
        })


        const mergedObjects = Array.from(map, ([key, value]) => ({ [key]: value }));



        mergedObjects.forEach(i => {

            const innerArr = Object.keys(Object.values(i)[0])



            //  Construct values for UpdateExpression key in txn
            let finish = ''
            const constructUpdateExpression = innerArr.map((i, x) => {

                if (i === 'V') return

                const cc = innerArr.filter(i => {
                    return i !== 'V'
                })


                if (innerArr.length === 1 || cc.length === 1) {

                    return finish = `set ${i} = :${i}`

                }


                if (innerArr.length === x + 1) {
                    return finish = finish + `${i} = :${i}`
                }

                if (x === 0) {
                    return finish = finish + `set ${i} = :${i},`
                }
                return finish + ` ${i} = :${i},`
            })





            let innerObjKey = Object.keys(i[Object.keys(i)])[0]


            // Construct ExpressionAttributeValues values
            let expObj = {}

            Object.keys(Object.values(i)[0]).forEach(i => {
                if (i === 'V') return

                // let nn = {}

                expObj[":" + i] = {
                    "S": newKey.toString()
                }


            })


            // Transaction obj for tables which have 2 ids
            const isVersionKey = Object.keys(i[Object.keys(i)]).filter(i => {
                return i === 'V'
            })

            if (isVersionKey.length !== 0) {


                let innerObjValue = i[Object.keys(i)]['V']

                let expValue = `:${innerObjKey}`

                let obj = {
                    "Update": {

                        "TableName": currKey.toString(),

                        "Key": {
                            "id": { "S": Object.keys(i).toString() },
                            "version": { "S": innerObjValue }

                        },
                        "UpdateExpression": finish,
                        "ExpressionAttributeValues": expObj
                    }
                }

                pram2.push(obj)

                return
            }



            //Transaction Obj for single id tables 

            let obj = {
                "Update": {

                    "TableName": currKey.toString(),

                    "Key": {
                        "id": { "S": Object.keys(i).toString() }
                    },
                    "UpdateExpression": finish,
                    "ExpressionAttributeValues": expObj
                }
            }
            pram.push(obj)


        })
    })





    let params = {
        'TransactItems': [
            ...pram,
            ...pram2
        ]
    }


    const txnReq = await ddbClient.send(new TransactWriteItemsCommand(params));
    console.log(txnReq)

}



const runScan = async () => {
    // enter old key to scan
    let oldKey = '83485ed8-4fd2-5852-9ce5-ec3703cf79a5'
    // Call table scan method
    await massScan(oldKey)




    // Enter new key 
    let newKey = '8d280b94-921b-5c9d-a4ad-e9456eac142b'

    // txn function
    txn([...final, ...versionFinal], newKey)

}

runScan()

