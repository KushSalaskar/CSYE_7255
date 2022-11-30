import { createClient } from "redis"
import { OAuth2Client } from "google-auth-library"
import crypto from "crypto"
import dotenv from "dotenv"

dotenv.config()

const CLIENT_ID = process.env.CLIENT_ID
const REDIS_PORT = process.env.REDIS_PORT || 6379

const googleClient = new OAuth2Client(CLIENT_ID)
const client = createClient(REDIS_PORT)
await client.connect()

export const googleIDPVerify = async (bearerToken) => {

    const token = bearerToken.split(" ")[1]
    async function verify() {
        const ticket = await googleClient.verifyIdToken({
            idToken: token,
            audience: CLIENT_ID
        });
        const payload = ticket.getPayload();
        return payload
    }
    return verify()

} 

export const verifyAuthorization = async (headers) => {
    try {
        if (headers['authorization'] === undefined || headers['authorization'] === "") {
            return false
        }

        const authorized = await googleIDPVerify(headers['authorization'])
        if (authorized) {
            return true
        }
        return false
    } catch (error) {
    
    }
}

export const createEtag = (plan) => {
    try {
        const hash = crypto.createHash('sha256').update(plan).digest('base64')
        return hash
    } catch (error) {
        console.log(error)
    }
}

export const checkIfPlanExistsService = async(objectId) => {
    try {
        const exists =  await client.exists(objectId)
        if (!exists) {
            return false
        }
        return true
    } catch (error) {
        console.log(error)
    }
}

export const getPlanEtag = async(objectId) => {
    try {
        const exists = await checkIfPlanExistsService(objectId + "_etag")
        if (!exists) {
            return false
        }
        const etag = await client.get(objectId + "_etag")
        return etag 
    } catch (error) {
        console.log(error)
    }
}

export const getPlanService = async (objectId) => {
    try {
        const exists =  await checkIfPlanExistsService(objectId)
        if (!exists) {
            return false
        }
        const plan = await client.get(objectId)
        const etag = await client.get(objectId + "_etag")
        return [plan, etag]
    } catch (error) {
        console.log(error)
    }
}

export const deletePlanService = async (objectId) => {
    try {
        const exists =  await checkIfPlanExistsService(objectId)
        if (!exists) {
            return false
        }
        const deleted = await client.del(objectId)
        const deleted_etag = await client.del(objectId + "_etag")
        return deleted && deleted_etag
    } catch (error) {
        console.log(error)
    }
}

export const savePlanService = async (objectId, plan, etag) => {
    try {
        await client.set(objectId, plan)
        await client.set(objectId + "_etag", etag)
        return objectId
    } catch (error) {
        console.log(error)        
    }
}

export const patchList = (mainObject, reqBody, k) => {
    try {
        const hmap = new Map()
        for (let i = 0; i < reqBody.length; i++) {
            if (reqBody[i]["objectId"] === undefined) {
                return "Bad Request"
            }
            hmap.set(reqBody[i]["objectId"], reqBody[i])
        }
        for (let i = 0; i < mainObject[k].length; i++) {
            if (hmap.has(mainObject[k][i]["objectId"])) {
                const tempObj = hmap.get(mainObject[k][i]["objectId"])
                hmap.delete(mainObject[k][i]["objectId"])
                for (let key in tempObj) {
                    if (typeof tempObj[key] !== "string") {
                        if (tempObj[key] instanceof Array) {
                            mainObject[k][i] = patchList(mainObject[k][i], tempObj[key], key)
                        } else {
                            mainObject[k][i] = patchObject(mainObject[k][i], tempObj[key], key)
                        }
                    } else {
                        mainObject[k][i][key] = tempObj[key]
                    }
                }
            }
        }
        for (let [key, value] of hmap) {
            mainObject[k].push(value)
        }
        return mainObject
    } catch (error) {
        console.log(error) 
    }
}

export const patchObject = (mainObject, reqBody, k) => {
    try {
        for (let key in reqBody) {
            mainObject[k][key] = reqBody[key]
        }
        return mainObject
    } catch (error) {
        console.log(error)
    }
}

export {client}