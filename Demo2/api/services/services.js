import { createClient } from "redis"

const REDIS_PORT = process.env.REDIS_PORT || 6379;

const client = createClient(REDIS_PORT)
await client.connect();

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

export const getPlanService = async (objectId) => {
    try {
        const exists =  await checkIfPlanExistsService(objectId)
        if (!exists) {
            return false
        }
        const plan = await client.get(objectId)
        return plan
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
        return deleted
    } catch (error) {
        console.log(error)
    }
}

export const savePlanService = async (objectId, plan) => {
    try {
        client.set(objectId, JSON.stringify(plan))
        return objectId
    } catch (error) {
        console.log(error)        
    }
}