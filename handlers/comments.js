const HTMLParser = require('node-html-parser');
const MarkdownIt = require('markdown-it')
const md = new MarkdownIt();
const XXHash = require('xxhash');
const hivejs = require('@hiveio/hive-js')

const {getOsClient, HIVE_NODES} = require('../config');
const osClient = getOsClient();

hivejs.api.setOptions({ url: HIVE_NODES[0] });
hivejs.config.set('alternative_api_endpoints', HIVE_NODES);


const getCommentID = ({author, permlink}) => {
    const buffer = Buffer.from(`${author}/${permlink}`);
    return XXHash.hash64(buffer, 0xCAFEBABE).toString("base64");
}
module.exports.getCommentID = getCommentID;

function parsePost(comment){
    
    try{
        // Parse Metadata
        comment.json_metadata = JSON.parse(comment.json_metadata)
        comment.json_metadata.tags = comment.json_metadata.tags || []
        comment.json_metadata.image = comment.json_metadata.image || []

        // Check variable types
        if(!comment.json_metadata.tags || !Array.isArray(comment.json_metadata.tags))
            comment.json_metadata.tags = [];
        if(!comment.json_metadata.image || !Array.isArray(comment.json_metadata.image))
            comment.json_metadata.image = [];
    } catch {
        // JSON Parse error --> set to {} because it is usually '' then
        comment.json_metadata = {tags : [], image : []}
    }

    // Parse Markdown-Body
    let html_body = md.render(comment.body);
    let root = HTMLParser.parse(html_body);  
    const imgs = root.querySelectorAll('img')
    for(let i = 0; i < imgs.length; i ++)
    {    
        let src = imgs[i].attrs.src
        if(src && !comment.json_metadata.image.includes(src))
            comment.json_metadata.image.push(src)
    }

    // Reparse to only get text
    root = HTMLParser.parse(root.text);
    comment.body = root.text;   
    comment.body = comment.body.replace(/\n/g, " \n ");

    return comment;
}

function handleReply(comment){
    
    // Check if comment was made to a stockimage post, so we have to update the image-tags
    if(comment.body.includes("!update-stock-image-tags")){
        // We got an update, check if author is allowed to do this
        if(!([comment.author, "hive-118554", "minismallholding", "crosheille", "kattycrochet"]).includes(comment.author)) 
            return []; // User is not allowed

        // Get & Parse Tags 
        comment.body = comment.body.replace("\n", " ");
        let image_tags = comment.body.split(' ').filter(v=> v.startsWith('#'))
        image_tags = image_tags.map(v=> v.substring(1));
        image_tags = image_tags.join(' ');

        // Get parent_id and update the tags of it
        const parent_id = getCommentID(comment);
        return [
            {update : {_index : "hive-post-data", _id : parent_id}},
            { doc : {stockimage_tags : image_tags}, upsert : {stockimage_tags : image_tags} }
        ]
    }
}

function handleStockImagePost(comment){
    if(
        !comment.json_metadata.tags?.includes("hivestockimages") &&
        !comment.json_metadata.tags?.includes("hive-118554") &&
        comment.parent_permlink !== "hive-118554" &&
        comment.parent_permlink !== "hivestockimages"
    ) // No StockImage Post
        return null;

    // Filter Tags
    let stockimage_tags = comment.body.split(' ').filter(v=> v.startsWith('#'))
    stockimage_tags = stockimage_tags.map(v=> v.substring(1));
    stockimage_tags = stockimage_tags.join(' ');
    return stockimage_tags;
}

async function processOne(comment, failCounter = 0, lastError = null){
    // Create unique identifier for the comment
    const comment_id = getCommentID(comment);

    // Check if comment is cross-post, then we have to update the origin post's parent_permlink
    // TODO: Implement the above code

    if(failCounter > 10) {
        // To many fails, return empty array
        console.error(`Too many fails, aborting entering Comment ${comment.author}/${comment.permlink}`, lastError)
        return []; 
    }

    if(comment.parent_author !== ""){
        // We have a reply
        return; // handleReply(comment);
    }

    // Check if the comment already exists
    const alreadyExists = await osClient.count({index : "hive-posts", body : {query : {ids : {values : [comment_id]}}}}).then(response => {
        return response.body.count > 0;
    }); 
    if(alreadyExists){
        // Get the full comment from HIVE and then update it
        updated_comment = await new Promise((resolve, reject) => {
            hivejs.api.getContent(comment.author, comment.permlink, (err, result) => {
                if(result && !err) 
                    return resolve(result);

                // Something failed
                reject({err, result});
            });
        }).catch(err => {
            lastError = err;
            return null;
        });

        if(!updated_comment) // Retry...
            return await new Promise(resolve => setTimeout(resolve, 750)).then(processOne(comment, failCounter + 1, lastError));    
            
        comment = updated_comment
        comment.timestamp = comment.created;
    }

    // Parse the comment
    comment = parsePost(comment);
    const stockimage_tags = handleStockImagePost(comment);


    // Set it in upsert and doc to be inserted or updates the existing one with these fields
    const post_doc = {
        author : comment.author,
        permlink : comment.permlink,
        text_title : comment.title,
        text_body : comment.body,
        parent_permlink : comment.parent_permlink, 
        tags : comment.json_metadata.tags,
        image : comment.json_metadata.image,
        timestamp : comment.timestamp,
        stockimage_tags : stockimage_tags,
    };

    // Reset Jobs
    const jobs_script = {
        source : "ctx._source.jobs = [:]",
        lang : "painless"
    }

    // Bulk to return
    const parsedTimestamp = new Date(comment.timestamp);
    const index = `hive-post-data-${parsedTimestamp.getMonth()}-${parsedTimestamp.getFullYear()}`;
    return [
        // Update/Upsert comment
        {update : {_index : index, _id : comment_id}},
        { doc : post_doc, upsert : post_doc},
        // Reset jobs
        {update : {_index : index, _id : comment_id}},
        { script : jobs_script, upsert : {jobs : null}},
        // Add last active timestamp
        {update : {_index : "hive-accounts", _id : comment.author}},
        {doc : {last_active : comment.timestamp}, upsert : {name : comment.author, last_active : comment.timestamp}}
    ]
}
module.exports.processOne = processOne;

module.exports.commentBulk = async (comments) => {
    // Process all comments and return the results flatted
    const results = await Promise.all(comments.map(processOne));
    return results.flat();
}