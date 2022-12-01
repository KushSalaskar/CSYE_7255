import * as queueUtils from "./redisQueueUtils.js"
import fs from "fs"
import { Client } from "@elastic/elasticsearch"
import dotenv from "dotenv"
import { createClient } from "redis"

dotenv.config()

const REDIS_PORT = process.env.REDIS_PORT || 6379

const client = createClient(REDIS_PORT)
await client.connect()

const ES_PW = process.env.ES_PW || "jHgl*weBUCMJ05sTvOa4"
const CERT_PATH = process.env.CERT_PATH || "/Users/kushsalaskar/Desktop/CSYE_7255/elasticsearch-8.5.2/config/certs"
const ES_BASE_URL = process.env.ES_BASE_URL || "https://localhost:9200"

const es_client = new Client({
    node: ES_BASE_URL,
    auth: {
        username: 'elastic',
        password: ES_PW
    },
    tls: {
        ca: fs.readFileSync(CERT_PATH + '/http_ca.crt'),
        rejectUnauthorized: false
    }
})

const parentChildSplit = (parentId, data, parentChildMappingDict, keyVal) => {
    if (data === undefined) {
        return parentChildMappingDict
    }
    let objectId = ""
    if (data["objectId"] !== undefined || data["objectId"] !== null) {
        objectId = data["objectId"]
        parentChildMappingDict[objectId] = {}
        for (let key in data) {
            if (typeof data[key] !== "object") {
                parentChildMappingDict[objectId][key] = data[key]
                delete data[key]
            }
        }
        parentChildMappingDict[objectId]["__parent__"] = parentId
        parentChildMappingDict[objectId]["__mappingKey__"] = keyVal
    }
    for (let key in data) {
        if (typeof data[key] === "object" && !(data[key] instanceof Array)) {
            parentChildSplit(objectId, data[key], parentChildMappingDict, key)
            delete data[key]
        } else {
            for (let obj in data[key]) {
                parentChildSplit(objectId, data[key][obj], parentChildMappingDict, key)
            }
        }
    }
    return parentChildMappingDict
}

const createIndexMapping = async () => {
    try {
        const deleted = await es_client.indices.delete({
            index: "plan"
        })
    } catch (error) {
        console.log("Plan mapping did not exist:", error)
    }

    try {
        const createMapping = await es_client.indices.create({
            index: 'plan',
            settings: {
              index: {
                number_of_shards: 1,
                number_of_replicas: 1
              }
            },
            body: {
              mappings: {
                properties: {
                  mapping: {
                    type: 'join',
                    relations: {
                        plan: ["planCostShares", "linkedPlanServices"],
                        linkedPlanServices: ["linkedService", "planserviceCostShares"]
                      }
                    }
                  }
                }
            }
        })
    } catch (error) {
       console.log("Error creating mapping:", error) 
    }
}

export const listening = async () => {
    let queue_size = 0
    try{
      const size = await client.LLEN("primaryQueue")
      queue_size = size;
    } catch(err){
        console.log("Initializing")
    }
    if(queue_size > 0){
      const data = await queueUtils.popFromPrimaryQueue()
      const parsed_data = parentChildSplit("root", JSON.parse(data), {}, "plan")
      try{
        const elastic_result = await es_client.index({
            index: 'plan',
            id: parsed_data["objectId"],
            body: parsed_data
          })
          console.log(elastic_result)
      } catch(err){

      }
    }
  }

  await createIndexMapping()
  while(true){
    await listening()
  }