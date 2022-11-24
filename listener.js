const request = require('request');
const {commentBulk} = require('./handlers/comments');
const {voteBulk} = require('./handlers/votes');
const {customJsonBulk} = require('./handlers/custom_jsons');
const {accountUpdateBulk} = require('./handlers/account_updates');
const hivejs = require('@hiveio/hive-js')

const {handleMuting} = require('./handle_mutings');
const {getOsClient, HIVE_NODES, MAX_BLOCKS_TO_FETCH, START_BLOCK_NUMBER, STOP_BLOCK_NUMBER, DISABLE_UPDATE_SETTINGS, BLOCK_NUMBER_DB_ID} = require('./config');

// Random API Node
hivejs.api.setOptions({ url: HIVE_NODES[Math.floor(Math.random() * HIVE_NODES.length)] });
hivejs.config.set('alternative_api_endpoints', HIVE_NODES);

let CURRENT_BLOCK_NUMBER = START_BLOCK_NUMBER;
async function getStartBlockNumber(){
    // TODO: Get the current block number from the storage
    if(CURRENT_BLOCK_NUMBER <= 0) {
        // We need to fetch it from settings-index
        const osClient = getOsClient();
        const result = await osClient.get({index : "settings", id : BLOCK_NUMBER_DB_ID});
        if(!result || !result.body || !result.body.found) {
            console.error("Could not fetch the start block number from the DB. Restarting...");
            return exit(-1);
        }

        // Parse doc and set block-num
        CURRENT_BLOCK_NUMBER = result.body._source.block_num;
        console.log("Start block number fetched from DB: ", CURRENT_BLOCK_NUMBER);
    }
}

async function updateBlockNumer(newNumber){
    CURRENT_BLOCK_NUMBER = newNumber;
    if(DISABLE_UPDATE_SETTINGS)
        return;

    // Update value in DB
    const osClient = getOsClient();
    const obj = {block_num : newNumber};
    await osClient.update({index : "settings", id : BLOCK_NUMBER_DB_ID, body : {doc : obj, upsert : obj}});
}

async function sendHeartbeat(s = 0, ms = null){
    const time_in_ms = ms ? ms : s * 1000;

    await new Promise((resolve, reject) => {
        request(process.env.CHAIN_LISTENER_HEARBEAT_URL + time_in_ms.toString(), {method : "GET"}, (err, res, body) => {
        if(err) 
            console.error(err); 
    
        // Resolve in any way (even if error because this error is not important)
        resolve();
        });
    });
}

async function* streamBlocks(){
    const start_time = Date.now();
    // Prepare the block stream (Get block_nums, the rpc-data and finally start the stream)
    const block_nums = Array.from({length: MAX_BLOCKS_TO_FETCH}, (v, k) => k + CURRENT_BLOCK_NUMBER);
    const rpc_data = block_nums.map((value, k) => {return {jsonrpc : "2.0", method : "condenser_api.get_ops_in_block", params: [value, false], id : k}});
    
    
    const {res : rpcResponse, body : rpcBody, err : rpcError, node_url} = await new Promise((resolve, reject) => {
        const req_data = {
            url : HIVE_NODES[Math.floor(Math.random() * HIVE_NODES.length)], 
            body : JSON.stringify(rpc_data), 
            method : "POST", 
            headers : {"Content-Type" : "application/json"}
        };
        
        request(req_data, (err, res, body) => {
            resolve({body, res, err, node_url : req_data.url});
        });
    });

    if(rpcResponse?.statusCode !== 200 || rpcError || !rpcBody){
        console.error("Error fetching RpcData: ", rpcResponse?.statusCode);
        console.error(rpcError, rpcBody);

        if(HIVE_NODES.length === 0)
        {
            console.error("No more nodes left to try. Restarting...");
            return exit(-1);
        }

        // Remove the node from the list and try again
        HIVE_NODES.splice(HIVE_NODES.indexOf(node_url), 1);
        console.log("Removed node from list: ", node_url);
        return yield* streamBlocks();
    }

    

    // Parse the response
    const parsedBody = JSON.parse(rpcBody);
    if(!parsedBody || !Array.isArray(parsedBody)){
        console.error("Error on parsing the RPC-Body: Not an array");

        if(HIVE_NODES.length === 0)
        {
            console.error("No more nodes left to try. Restarting...");
            return exit(-1);
        }

        // Remove the node from the list and try again
        console.error("Removing node from list: ", node_url);
        HIVE_NODES.splice(HIVE_NODES.indexOf(node_url), 1);
        return yield* streamBlocks();
    }

    // Prepare the stream
    const blocks = parsedBody.map(data => data.result).filter(ops => ops.length > 0);       
    if(!blocks){
        console.error("Error parsing Rpc Data: ");
        console.error(rpcBody, rpc_response);
        console.error("Restarting...")
        return exit(-1);
    }

    // Check if blocks available
    if(blocks.length === 0){
        return;
    }

    // Check for failings ==> There is no block with not trxs
    const failed_blocks = blocks.filter(value => !value || value.length === 0).length;
    if(failed_blocks > 0){
        console.error(`Failed to fetch ${failed_blocks} block(s) between ${CURRENT_BLOCK_NUMBER} and ${blocks.length}. Cause unknown. Restarting....`);
        return exit(-1);
    }

    // Yield all blocks
    for(const block of blocks){
        yield block;
    }

    // If all blocks were proceed successfully, update the current block number
    const elapsed_time = Math.floor((Date.now() - start_time) / 1000);
    console.log(`Fetched ${blocks.length} block(s) between ${CURRENT_BLOCK_NUMBER} and ${CURRENT_BLOCK_NUMBER + blocks.length} in ${elapsed_time}s`);
    await updateBlockNumer(CURRENT_BLOCK_NUMBER + blocks.length);

    // Send Heartbeat
    sendHeartbeat(elapsed_time);
}

function filterOperation(ops){
    return {
        comments : ops.filter(item => item[0] === "comment").map(item => item[1]),
        votes : ops.filter(item => item[0] === "vote").map(item => item[1]),
        custom_jsons : ops.filter(item => item[0] === "custom_json").map(item => item[1]),
        account_updates : ops.filter(item => item[0] === "account_update").map(item => item[1]),
    }
}

async function handleBlocks(){
    const start_time = Date.now();
    const osClient = getOsClient();

    // Stream and Process blocks
    for await(const block of streamBlocks()){
        // Add timestamps and filter Ops
        const ops = block.map(item => {item.op[1].timestamp = item.timestamp; return item;}).map(item => item.op)
        const {comments, votes, custom_jsons, account_updates} = filterOperation(ops);

        // Process all operations
        const task_results = await Promise.all([
            commentBulk(comments),
            voteBulk(votes),
            customJsonBulk(custom_jsons),
            accountUpdateBulk(account_updates)
        ]);

        // Combine results into one bulk and make changes in OpenSearch
        const bulk = task_results.flat().filter(item => item);
        if(bulk.length > 0){
            const result = await osClient.bulk({body : bulk});
            if(result.statusCode !== 200 && result.body.errors === false){
                // Exit when OpenSearch fails to ensure we don't have any data loss
                console.error("Error on entering Data in OpenSearch: ", result.statusCode, result.body);
                exit(-1);
            }
        }
    }

    if(STOP_BLOCK_NUMBER > -1 && CURRENT_BLOCK_NUMBER >= STOP_BLOCK_NUMBER){
        console.log("Reached the stop block number. Exiting with Code 0...");
        exit(0);
    }

    // Wait some time (max. 3sec)
    const wait_time = (3 - (Date.now() - start_time)) / 1000 ;
    setTimeout(handleBlocks, wait_time > 0 ? wait_time : 25);
}

async function mutingIntervals() {
    // Start the Interval to handle Mutes. 
    // wait initially for 5min and then execute it with a delay of 1h
    await new Promise(resolve => setTimeout(resolve, 1000 * 60 * 5));
    
    while(1){
        await handleMuting(); 
        await new Promise(resolve => setTimeout(resolve, 1000 * 60 * 60));
    }
}


async function startChainListener(){
    await getStartBlockNumber();
    
    handleBlocks();
    mutingIntervals();
}

startChainListener();