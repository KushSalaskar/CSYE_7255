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
    
    let q_size = 0

    try {
      q_size = await client.LLEN("primaryQueue")
    } catch (error) {
      console.log("Queue Empty")
    }

    if(q_size > 0) {
      try {
        const data = await queueUtils.popFromPrimaryQueue()
        const mappedJson = parentChildSplit("root", JSON.parse(data), {}, "plan")
        for (let objectId in mappedJson) {
          const mappingKey = mappedJson[objectId]["__mappingKey__"]
          const parentId = mappedJson[objectId]["__parent__"] === "root" ? "" : mappedJson[objectId]["__parent__"] 
          const es_fragment = mappedJson[objectId]
          delete es_fragment["__mappingKey__"]
          delete es_fragment["__parent__"]

          if (mappingKey === "plan") {
            es_fragment["mapping"] = "plan"
          } else if (mappingKey === "planserviceCostShares") {
            es_fragment["mapping"] = {
              name: "planserviceCostShares",
              parent: parentId
            } 
          } else if(mappingKey == "linkedService"){
            es_fragment["mapping"] = {
                name: "linkedService",
                parent: parentId 
              }
          } else if (mappingKey == "planCostShares"){
              es_fragment["mapping"] = {
                name: "planCostShares",
                parent: parentId
              }
          }else if (mappingKey == "linkedPlanServices"){
            es_fragment["mapping"] = {
                name: "linkedPlanServices",
                parent: parentId
              }
          }else if (mappingKey == "planCostShares"){
            es_fragment["mapping"] = {
                name: "planCostShares",
                parent: parentId
              }
          } else if(mappingKey == null) {
            es_fragment["mapping"] = "plan"
          }

          const es_result = await es_client.index({
            index: 'plan',
            id: objectId,
            routing: parentId,
            document: es_fragment
          })
          console.log(es_result)
        }

        await queueUtils.popFromSecondaryQueue()

      } catch (error) {
        console.log(error)
      }
  
    }
  }

  export const deleteListener = async () => {
    let q_size = 0

    try {
      q_size = await client.LLEN("primaryDeleteQueue")
    } catch (error) {
      console.log("Queue Empty")
    }

    if (q_size > 0) {
      try {
        const data = await queueUtils.popFromPrimaryDeleteQueue()
        const mappedJson = parentChildSplit("root", JSON.parse(data), {}, "plan")
        for (let objectId in mappedJson) {
          const es_delete = await es_client.delete({
            index: 'plan',
            id: objectId 
          })
          console.log(es_delete)
        }

        await queueUtils.popFromSecondaryDeleteQueue()

      } catch (error) {
        console.log(error)
      }
    }
  }

  await createIndexMapping()
  while(true){
    await listening()
    await deleteListener()
  }