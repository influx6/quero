var _ = require('stackq'),
expects =  _.Expects,
live = require('../quero.js');
require('../adaptors/mongo.js');

_.Jazz('livedb mongo adaptor specification',function($){

  var conn = live.createProvider('mongodb',{
    db: 'localhost/mydb',
    username: 'alex',
    password: 'crocker'
  });

  $('does the mongo connector exists',function(k){
    k.sync(function(c,g){
      expects.truthy(c);
      expects.truthy(c.providers);
      expects.truthy(c.hasProvider('mongodb'));
    });
    k.for(live);
  });

  conn.once('queryError',function(f){
    $('mongo connector can not deal with invalid queries?',function(k){
      k.sync(function(c,g){
        expects.falsy(c.state);
      });
      k.for(f);
    });
  });

  conn.once('up',function(f){
    $('is the mongo connector ready?',function(k){
      k.sync(function(c,g){
        expects.truthy(c.state());
      });
      k.for(f);
    });
  });

  conn.up();

  conn.once('down',function(f){
    $('is the mongo connector dead?',function(k){
      k.sync(function(c,g){
        expects.falsy(c.state());
      });
      k.for(f);
    });
  });

  conn.down();

  conn.up();

  conn.on('get',function(f){
    $('can i get a doc?',function(k){
      k.sync(function(c,g){
        expects.truthy(c);
        expects.isObject(c);
      });
      k.for(f);
    });
  });

  conn.on('create',function(f){
    $('can i create a doc?',function(k){
      k.sync(function(c,g){
        expects.truthy(c);
        expects.isObject(c);
      });
      k.for(f);
    });
  });

  conn.query({
    'query':'get',
    'with': 'comments'
  });

  conn.query({
    'query':'stuck',
    'with': 'articles'
  });

  conn.down();
});
