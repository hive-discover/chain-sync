const XXHash = require('xxhash');

const {getOsClient} = require('../config');
const osClient = getOsClient();

function getCommentID({author, permlink}){
    const buffer = Buffer.from(`${author}/${permlink}`);
    return XXHash.hash64(buffer, 0xCAFEBABE).toString("base64");
}

async function processOne({voter, timestamp, post_id, post_index, weight}){
    // Add vote to correct set (up- or downvotes)
    const update_script = {
        source : "if(!ctx._source.containsKey(params.target)){ctx._source[params.target]=[];} if(ctx._source[params.target].contains(params.voter)){ctx.op = \"none\";} else {ctx._source[params.target].add(params.voter);}", 
        params : {
            voter : voter,
            target : weight > 0 ? "upvotes" : "downvotes",
        }
    }

    return [
        // Add vote to post
        {update : {_index : post_index, _id : post_id}},
        {"script" : update_script},
        // Add user to hive-accounts
        {update : {_index : "hive-accounts", _id : voter}},
        {doc : {last_active : timestamp}, upsert : {name : voter, last_active : timestamp}}
    ];
}

async function filterForExistingPosts(votes){
    const ids = votes.map(vote => getCommentID(vote));
    const body = {query : {ids : {values : ids}}, size : 10000, _source : {includes : ["nothing"]}};
    const response = await osClient.search({index : "hive-posts", body});
    
    // Filter out posts that don't exist and return the ids with the index
    const existingPosts = response.body.hits.hits.map(hit => ({post_id : hit._id, post_index : hit._index}));
    
    // Filter out posts that don't exist
    return votes.map(vote => {
        const id = getCommentID(vote);
        const existingPost = existingPosts.find(post => post.post_id == id);
        if(!existingPost)return null;

        return {...vote, post_id : existingPost.post_id, post_index : existingPost.post_index};
    }).filter(vote => vote?.post_id);
}

module.exports.voteBulk = async (votes) => {
    if(!votes || votes.length == 0)
        return [];

    // Filter out posts that don't exist
    votes = await filterForExistingPosts(votes);

    // Process all votes and return the results flatted
    const results = await Promise.all(votes.map(processOne));
    return results.flat();
}