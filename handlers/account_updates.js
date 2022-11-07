
async function processOne({json_metadata, account, timestamp}){
    let profile = {};
    try {
        profile = JSON.parse(json_metadata).profile || {};
    } catch{/* do nothing, because then it is setted to be empty */}	

    return [
        {update : {_index : "hive-accounts", _id : account}},
        {doc : {profile : profile, last_active : timestamp}, upsert : {name : account, profile : profile, last_active : timestamp}}
    ];
}

module.exports.accountUpdateBulk = async (accountUpdates) => {
    // Process all accountUpdates and return the results flatted
    const results = await Promise.all(accountUpdates.map(processOne));
    return results.flat();
}