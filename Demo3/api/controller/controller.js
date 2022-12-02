import * as planService from "../services/services.js"
import * as queueUtils from "../services/redisQueueUtils.js"
import Ajv from "ajv"
import { planSchema } from "../schema.js"
import dotenv from "dotenv"

dotenv.config()

const ajv = new Ajv()

//Method to handle errors
const errorHandler = (message, res, errCode=400) => {
    res.status(errCode);
    res.json({error:message});
}

//method to execute when exec is successfull
const setSuccessResponse = (data, res, etag, successCode=200) => {
    res.status(successCode);
    res.set({"Etag": etag})
    res.json(data);
}

//GET Controller
export const getPlan = async (req, resp) => {
    try {
        
        const authorized = await planService.verifyAuthorization(req.headers)

        if (!authorized) {
            errorHandler("Unauthorized", resp, 401)
            return
        }

        const id = `${req.params.id}`
        const doesPlanExist = await planService.checkIfPlanExistsService(id)
        if (!doesPlanExist) {
            errorHandler("No plans found with the corresponding ObjectId", resp, 404)
            return 
        }
        const [plan, etag] = await planService.getPlanService(id)
       
        if (req.headers['if-none-match'] !== undefined && req.headers['if-none-match'] === etag) {
            setSuccessResponse("Not Modified", resp, etag, 304)
            return
        }
        if (!plan) {
            errorHandler("No plans found with the corresponding ObjectId", resp, 404)
            return
        }
        setSuccessResponse(JSON.parse(plan), resp, etag) 
    } catch (error) {
        errorHandler(error.message, resp)
    }
}

//DELETE Controller
export const deletePlan = async (req, resp) => {
    try {
        const authorized = await planService.verifyAuthorization(req.headers)
        if (!authorized) {
            errorHandler("Unauthorized", resp, 401)
            return
        }
        const id = `${req.params.id}`
        const doesPlanExist = await planService.checkIfPlanExistsService(id)
        if (!doesPlanExist) {
            errorHandler("No plans found with the corresponding ObjectId to delete", resp, 404)
            return 
        }
        const etag = await planService.getPlanEtag(id)
        const [plan, etag_nouse] = await planService.getPlanService(id)
        let isDeleted = false
        if (req.headers['if-match'] === undefined) {
            errorHandler("Precondition required. Try using \"If-Match\"", resp, 428)
            return
        }
        if (req.headers['if-match'] !== etag) {
            errorHandler("A requested precondition failed", resp, 412)
            return
        } 
        if (req.headers['if-match'] === etag) {
            isDeleted = await planService.deletePlanService(id)
        }
        if (!isDeleted) {
            errorHandler("Something went wrong", resp, 500)
            return
        }
        
        await queueUtils.appendToPrimaryDeleteQueue(plan)

        setSuccessResponse(`Plan ${id} successfully deleted`, resp, etag, 204) 
    } catch (error) {
        console.log(error)
        errorHandler(error.message, resp)
    }
}

//PATCH Controller
export const patchPlan = async (req, resp) => {
    try {
        const authorized = await planService.verifyAuthorization(req.headers)
        if (!authorized) {
            errorHandler("Unauthorized", resp, 401)
            return
        }
        const id = `${req.params.id}`
        const doesPlanExist = await planService.checkIfPlanExistsService(id)
        if (!doesPlanExist) {
            errorHandler("No plans found with the corresponding ObjectId to delete", resp, 404)
            return 
        }
        let [plan, etag] = await planService.getPlanService(id)
        const planToDelete = plan
        if (req.headers['if-match'] === undefined) {
            errorHandler("Precondition required. Try using \"If-Match\"", resp, 428)
            return
        }
        if (req.headers['if-match'] !== etag) {
            errorHandler("A requested precondition failed", resp, 412)
            return
        } 
        if (req.headers['if-match'] === etag) {
            plan = JSON.parse(plan)
            for (let key in req.body) {
                if (typeof req.body[key] !== "string") {
                    if (req.body[key] instanceof Array) {
                        plan = planService.patchList(plan, req.body[key], key)
                        if (plan === "Bad Request") {
                            errorHandler("List objects must contain object ID", resp)
                            return 
                        }
                    } else {
                        plan = planService.patchObject(plan, req.body[key], key)
                    }
                } else {
                    plan[key] = req.body[key]
                }
            }
        }
        const valid = ajv.validate(planSchema, plan)
        if (!valid) {
            errorHandler(ajv.errors, resp)
            return
        }
        const { objectId } = plan
        const doesPlanExistNew = await planService.checkIfPlanExistsService(objectId)
        if (objectId !== id && doesPlanExistNew) {
            errorHandler(`The Plan with objectId ${objectId} already exists`, resp, 409)
            return
        }
        plan = JSON.stringify(plan)
        etag = planService.createEtag(plan)
        if (id !== objectId) {
            await planService.deletePlanService(id) 
        }
        await queueUtils.appendToPrimaryDeleteQueue(planToDelete)
        const respObjectId = await planService.savePlanService(objectId, plan, etag)
        await queueUtils.appendToPrimaryQueue(plan)
        if (respObjectId !== null){
            setSuccessResponse(JSON.parse(plan), resp, etag) 
        } else {
            errorHandler("Something went wrong", resp)
        } 
    } catch (error) {
        errorHandler(error.message, resp) 
    }
}

//POST Controller
export const savePlan = async (req, resp) => {
    try{
        const authorized = await planService.verifyAuthorization(req.headers)
        if (!authorized) {
            errorHandler("Unauthorized", resp, 401)
            return
        }
        const plan = JSON.stringify(req.body)
        if (req.body === "{}" || plan === "{}") {
            errorHandler("Request body cannot be empty", resp)
            return
        }
        const valid = ajv.validate(planSchema, req.body)
        if (!valid) {
            errorHandler(ajv.errors, resp)
            return
        }
        const { objectId } = req.body
        const doesPlanExist = await planService.checkIfPlanExistsService(objectId)
        if (doesPlanExist) {
            errorHandler(`The Plan with objectId ${objectId} already exists`, resp, 409)
            return
        }
        const etag = planService.createEtag(plan)
        const respObjectId = await planService.savePlanService(objectId, plan, etag)
        await queueUtils.appendToPrimaryQueue(plan)
        if (respObjectId !== null){
            setSuccessResponse(`Plan with ObjectId - ${respObjectId} successfully added`, resp, etag, 201)
        } else {
            errorHandler("Something went wrong", resp)
        }
    } catch(error) {
        errorHandler(error.message, resp)
    }
}