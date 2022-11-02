import express from "express"
import * as apiController from "../controller/controller.js"

const router = express.Router()

router.route("/addPlan")
    .post(apiController.savePlan)

router.route("/getPlan/:id")
    .get(apiController.getPlan)

router.route("/deletePlan/:id")
    .delete(apiController.deletePlan)

router.route("/patchPlan/:id")
    .patch(apiController.patchPlan)

export default router