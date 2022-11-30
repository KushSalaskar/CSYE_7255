import * as queueUtils from "./redisQueueUtils.js"
import fs from "fs"
import { Client } from "@elastic/elasticsearch"
import dotenv from "dotenv"

dotenv.config()

const ES_PW = process.env.ES_PW
const CERT_PATH = process.env.CERT_PATH
const ES_BASE_URL = process.env.ES_BASE_URL

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
    console.log("HERREEEE")
    try{
      const size = await queueUtils.client.LLEN("primaryQueue");
      queue_size = size;
    } catch(err){
        console.log("Error getting queue size",err);
    }
    if(queue_size > 0){
      console.log("Queue size: ",queue_size);
      const data = await queueUtils.popFromPrimaryQueue();
      const parsed_data = JSON.parse(data);
      const elastic_result = await es_client.index({
        index: 'plan',
        body: parsed_data
      })
      console.log(elastic_result)
    }
  }