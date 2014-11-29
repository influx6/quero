var _ = require('stackq'),
quero = require('../quero.js');
require('../adaptors/mongo.js');

_.Jazz('livedb mongo adaptor specification',function($){

  var conn = quero.make({
    adaptor: 'mongodb',
    db: '',
  });

  conn.schema('User',{
    name: 'string',
    email: 'email',
    tel: 'number'
  },{

  },{
    'email': function(e,fn){
      return fn.call(null,/\w+/.test(e));
    }
  });

  var users = conn.model('User');
  users
  .where({ name: 'alex'})
  .xstream(function(){})
  .every(1000)
  .update({age: 32})
  .save();

  console.log(users.end());

  $('can i create a livedb instance',function(k){
    k.sync(function(c,g){
      _.Expects.truthy(c);
      _.Expects.isInstanceOf(c,quero);
    });
    k.for(conn);
  });

});
