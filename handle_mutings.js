const request = require('request');

const {getOsClient} = require('./config');

async function getBatchCommunityRols(last_account = "") {
    var options = {
        url: 'https://api.hive.blog',
        method: 'POST',
        body: '{"jsonrpc":"2.0", "method":"bridge.list_community_roles", "params":{"community":"hive-118554", "last":"'+last_account+'","limit":100}, "id":1}'
    };

    const result = await new Promise((resolve, reject) => {
        request(options, (error, response, body) =>{
            if (!error && response.statusCode == 200 && JSON.parse(body).result) 
                resolve(JSON.parse(body));
            else if(error)
                reject(error);
            else
                reject(JSON.parse(body));
        });
    }).catch(err => {console.error("Cannot get batch of muted accounts: ", err); return null;});

    return result;
}

module.exports.handleMuting = async () => {
    const os_client = getOsClient();
    return;
    
    let last_account = '';
    while(true){
        const batch = await getBatchCommunityRols(last_account);
        if(batch.result.length === 0)
            break; // We finished

        for(let i = 0; i < batch.result.length; i++){
            [last_account, role, empty] = batch.result[i];
            if(role !== "muted")
                continue;

            // Set muted_in_community to true with update_query
            await os_client.update_by_query({
                body : {query: {
                    "bool": {
                        "must": [
                            { "term": { "author": last_account } },
                            { "term" : {parent_permlink : "hive-118554"}}
                        ]
                    }
                },
                script : {
                    source : "ctx._source.muted_in_community = true",
                    lang : "painless"
                },},
                index: "hive-post-data",
            });
        }
    }
}