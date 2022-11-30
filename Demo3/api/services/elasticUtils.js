import * as queueUtils from "./redisQueueUtils"
import fs from "fs"
import { Client } from "@elastic/elasticsearch"
import {client} from "./services"
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
    },
    headers:{
        "Content-type": "application/json"
    }
})

//Testing es

const listening = async() => {
    let queue_size = 0
    try{
      const size = await client.LLEN("primaryQueue");
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
  
  while (true) {
    await listening();
  }

//testing es