conn = new Mongo();
db = conn.getDB("yadamu");
var exists = db.system.js.find({_id: "yadamu_instance_id"}).count();
if (exists === 0) {
  const formatUUID = (x) => { return `${x.substring(0,8)}-${x.substring(8,12)}-${x.substring(12,16)}-${x.substring(16,20)}-${x.substring(20)}`}
  const uuid = UUID()
  const formattedUUID = formatUUID(new Buffer(uuid.toString('hex')).toString())
  print(formattedUUID)
  var instanceId =  new Function('', `return "${formattedUUID}"`);
  db.system.js.insertOne({_id:  "yadamu_instance_id",
                          value: instanceId})
};
var exists = db.system.js.find({_id: "yadamu_installation_timestamp"}).count();
if (exists === 0) {
  var instanceId =  new Function('', `return "${new Date().toISOString()}"`);
  db.system.js.insertOne({_id:  "yadamu_installation_timestamp",
                          value: instanceId})
};
db.system.js.aggregate([
  {$match: { _id : "yadamu_instance_id"}},
  {$project : { _id : 0, instance_id : { $function : { body: function(f) { return f() }, args : ["$value"], lang: "js"}}}},
  {$lookup: {
     from: "system.js",
     pipeline: [
       { $match: { _id : "yadamu_installation_timestamp" } },
       { $project : { _id : 0, timestamp : { $function : { body: function(f) { return f() }, args : ["$value"], lang: "js"}}}}
     ],
     as: "timestamp"
   }},
   {$project : { _id : 0, instance_id: "$instance_id", timestamp: { $first : "$timestamp.timestamp"}}}
]);
