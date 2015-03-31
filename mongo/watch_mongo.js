var MongoOplog = require('mongo-oplog');
var oplog = MongoOplog('mongodb://production-web-server.c.onefold-1.internal:27017/local', 'test.demo_messages').tail();

var kafka = require('kafka-node'),
     Producer = kafka.Producer,
     client = new kafka.Client('hadoop-w-0.c.onefold-1.internal:2181'),
     producer = new Producer(client);

oplog.on('insert', function (doc) {
  console.log(doc.ns + " inserted: %j", doc.o);

  payloads = [
         { topic: 'truckevent2', messages: JSON.stringify(doc.o), partition: 0 },
     ];

     producer.send(payloads, function(err, data){
         console.log(data)
     });

});

