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

console.log(ES_PW, CERT_PATH, ES_BASE_URL)

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

export const listening = async() => {
    let queue_size = 0
    try{
      const size = await client.LLEN("primaryQueue");
      queue_size = size;
    } catch(err){
        console.log("Initializing");
    }
    if(queue_size > 0){
      console.log("Queue size: ",queue_size);
      const data = await queueUtils.popFromPrimaryQueue();
      const parsed_data = JSON.parse(data);
      const elastic_result = await es_client.index({
        index: 'plan',
        id: parsed_data["objectId"],
        body: parsed_data
      })
      console.log(elastic_result)
    }
  }

  while(true){
    await listening()
  }