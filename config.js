// Hive Vars
module.exports.HIVE_NODES = [
    "https://api.hive.blog",
    "https://api.deathwing.me",
    "https://hive-api.arcange.eu",
    "https://hived.emre.sh",
    "https://api.openhive.network"
]

// Database Vars
const opensearch = require('@opensearch-project/opensearch')
module.exports.getOsClient = () => {
    const nodes = JSON.parse(process.env.OPENSEARCH_NODES || "[]");
    const [username, password] = process.env.OPENSEARCH_AUTH.split(':')

    return new opensearch.Client({
        nodes: nodes,
        auth : {username, password},
        ssl: {}
    });
};


// CMD Args
const cmd_args = require('minimist')(process.argv.slice(2))
module.exports.START_BLOCK_NUMBER = cmd_args.start_block_number || -1;
console.log("START_BLOCK_NUMBER: ", module.exports.START_BLOCK_NUMBER);
module.exports.STOP_BLOCK_NUMBER = cmd_args.stop_block_number || -1;
console.log("STOP_BLOCK_NUMBER: ", module.exports.STOP_BLOCK_NUMBER);
module.exports.MAX_BLOCKS_TO_FETCH = cmd_args.max_blocks_to_fetch || 30;
console.log("MAX_BLOCKS_TO_FETCH: ", module.exports.MAX_BLOCKS_TO_FETCH);
module.exports.DISABLE_UPDATE_SETTINGS = cmd_args.disable_update_settings || false;
console.log("DISABLE_UPDATE_SETTINGS: ", module.exports.DISABLE_UPDATE_SETTINGS);
module.exports.BLOCK_NUMBER_DB_ID = cmd_args.block_number_db_id || "chain-sync-block-number";
console.log("BLOCK_NUMBER_DB_ID: ", module.exports.BLOCK_NUMBER_DB_ID);