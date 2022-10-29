import * as planService from "../services/services.js"
import Ajv from "ajv"
import fetch from "node-fetch"
import { planSchema } from "../schema.js"

const ajv = new Ajv()

//Method to handle errors
const errorHandler = (message, res, errCode=400) => {
    res.status(errCode);
    res.json({error:message});
}

//method to execute when exec is successfull
const setSuccessResponse = (data, res, successCode=200) => {
    res.status(successCode);
    res.json(data);
}

export const getPlan = async (req, resp) => {
    try {
       const id = req.params.id
       const plan = await planService.getPlanService(id)
       if (!plan) {
            errorHandler("No plans found with the corresponding ObjectId", resp, 404)
            return
       }
       setSuccessResponse(JSON.parse(plan), resp) 
    } catch (error) {
        errorHandler(error.message, resp)
    }
}

export const deletePlan = async (req, resp) => {
    try {
        const id = req.params.id
        const getResp = await fetch(`http://localhost:5001/getPlan/${id}`)
        const etag = getResp.headers.get('etag')
        
        let isDeleted = false
        if (req.headers['if-match'] === etag) {
            isDeleted = await planService.deletePlanService(id)
        }
        if (!isDeleted) {
            errorHandler("No plans found with the corresponding ObjectId to delete", resp, 404)
            return
        }
        setSuccessResponse(`Plan ${id} successfully deleted`, resp, 204) 
    } catch (error) {
        errorHandler(error.message, resp)
    }
}

export const savePlan = async (req, resp) => {
    try{
        if (req.body === "{}" || JSON.stringify(req.body) === "{}") {
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
        const respObjectId = await planService.savePlanService(objectId, req.body) 
        if (respObjectId !== null){
            setSuccessResponse(`Plan with ObjectId - ${respObjectId} successfully added`, resp, 201)
        } else {
            errorHandler("Something went wrong", resp)
        }
    } catch(error) {
        errorHandler(error.message, resp)
    }
}