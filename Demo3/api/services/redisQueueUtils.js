import {client} from "./services"

export const appendToPrimaryQueue = async (msg) => {
    try {
        const appended = await client.LPUSH("primaryQueue", msg)
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