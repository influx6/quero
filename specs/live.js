var _ = require('stackq'),
quero = require('../quero.js');
require('../adaptors/mongo.js');

_.Jazz('livedb mongo adaptor specification',function($){

  var conn = quero.make({
    adaptor: 'mongodb',
    db: 'localhost/mydb',
  });

  conn.up();

  conn.schema('users',{
    name: 'string',
    email: 'string',
    tel: 'number'
  });

  var users = conn.model('users');


  users
  .index({ indexes:'name', options: { unique: true }})
  .insert({ name: 'alex', email: 'trinoxf@gmail.com', tel: '07088986857' })
  .xstream(function(){
    this.in().on(function(d){
      $('can i receive data from insert stream',function(k){
        k.sync(function(m,g){
          _.Expects.isObject(m);
        });
      }).use(d);
    });

    this.out().on(function(d){
      $('can i send data out of insert stream',function(k){
        k.sync(function(m,g){
          _.Expects.isObject(m);
        });
        k.for(d)
      });
    });
  })
  .insert({ name: 'john', email: 'buck@gmail.com', tel: '07090086857' })
  .group()
  .xstream(function(){
    this.in().on(function(d){
      $('can i group data from group stream',function(k){
        k.sync(function(m,g){
          _.Expects.truthy(m);
        });
      }).use(d);
    });

    this.out().on(function(d){
      $('can i send data out grouped data as array in group stream',function(k){
        k.sync(function(m,g){
          _.Expects.isList(m);
        });
        k.for(d)
      });
    });
  })
  .use('saveChangeAndStream')
  .xstream(function(){
    this.in().on(function(d){
      $('can i receive data from group stream for save stream',function(k){
        k.sync(function(m,g){
          _.Expects.isList(m);
        });
      }).use(d);
    });

    this.out().on(function(d){
      $('can i receive data back for save stream',function(k){
        k.sync(function(m,g){
          _.Expects.isList(m);
        });
        k.for(d)
      });
    });
  })
  .end();

  users.future()
  .onError(function(d){
    console.log('error:',d);
    conn.down();
  })
  .then(function(d){
    console.log('done:',d);
    conn.down();
  });


  $('can i create a livedb instance',function(k){
    k.sync(function(c,g){
      _.Expects.truthy(c);
      _.Expects.isInstanceOf(c,quero);
    });
    k.for(conn);
  });

  // conn.down();

});
