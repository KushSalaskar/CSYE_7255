import { createClient } from "redis"
import * as esUtils from "./elasticUtils.js"

const REDIS_PORT = process.env.REDIS_PORT || 6379

const client = createClient(REDIS_PORT)
await client.connect()

export const appendToPrimaryQueue = async (msg) => {
    try {
        const appended = await client.LPUSH("primaryQueue", msg)
        if (appended) {
            esUtils.listening()
        }
        return appended
    } catch (error) {
        console.log(error)
    }
}

export const popFromPrimaryQueue = async () => {
    try {
        const moveToSecondaryQueue = await client.BRPOPLPUSH("primaryQueue","secondaryQueue",0);
        return moveToSecondaryQueue
    } catch (error) {
        console.log(error)
    }
}

export const popFromSecondaryQueue = async () => {
    try {
        const secondaryQueuePop = await client.RPOP("secondaryQueue");
        return secondaryQueuePop;
    } catch (error) {
        console.log(error)
    }
}

export {client}