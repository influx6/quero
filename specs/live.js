var _ = require('stackq'),
quero = require('../quero.js');
require('../adaptors/mongo.js');

_.Jazz('livedb mongo adaptor specification',function($){

  var conn = quero.make({
    adaptor: 'mongodb',
    db: 'localhost/mydb',
  });

  conn.up();

  conn.schema('User',{
    name: 'string',
    email: 'string',
    tel: 'number'
  });

  var users = conn.model('User');

  users
  .insert({ name: 'alex', email: 'trinoxf@gmail.com', tel: 07088986857 })
  .xstream(function(){
    this.in().on(_.tags.tagDefer('insert-stream-in'));
    this.out().on(_.tags.tagDefer('insert-stream-out'));
  })
  .insert({ name: 'john', email: 'buck@gmail.com', tel: 07090086857 })
  .xstream(function(){
    this.in().on(_.tags.tagDefer('insert2-stream-in'));
    this.out().on(_.tags.tagDefer('insert2-stream-out'));
  })
  .save()
  .xstream(function(){
    this.in().on(_.tags.tagDefer('save-stream-in'));
    this.out().on(_.tags.tagDefer('save-stream-out'));
  })
  .end();


  $('can i create a livedb instance',function(k){
    k.sync(function(c,g){
      _.Expects.truthy(c);
      _.Expects.isInstanceOf(c,quero);
    });
    k.for(conn);
  });

  conn.down();

});
