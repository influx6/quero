var _ = require('stackq'),
quero = require('../quero.js');
require('../adaptors/mongo.js');

_.Jazz('livedb mongo adaptor specification',function($){

  var conn = quero.make({
    adaptor: 'mongodb',
    db: '',
  });

  console.log(conn);

  $('can i create a livedb instance',function(k){
    k.sync(function(c,g){
      _.Expects.truthy(c);
      _.Expects.isInstanceOf(c,quero);
    });
    k.for(conn);
  });

});
