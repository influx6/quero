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
  .where({})
  .xstream(function(){
    this.onError(_.tags.tagDefer('where-error'));
    this.out().on(function(d){
      $('can i retrieve data from db',function(k){
        k.sync(function(m,g){
          console.log('where:',m);
          _.Expects.truthy(m);
        });
      }).use(d);
    });
  })
  .use('update',{ key: 'name', from:'alex', to: 'buttocks'})
  .xstream(function(){
    this.onError(_.tags.tagDefer('update-error'));
  })
  .use('filter',{ key: 'name', value:'john'})
  .xstream(function(){
    this.onError(_.tags.tagDefer('filter-error'));
    this.in().on(function(d){
      $('can i filter data in stream',function(k){
        k.sync(function(m,g){
          console.log('filter-in:',m);
          _.Expects.truthy(m);
        });
      }).use(d);
    });

    this.out().on(function(d){
      $('can i filter data saved from stream',function(k){
        k.sync(function(m,g){
          console.log('filter-out:',m);
          _.Expects.truthy(m);
        });
        k.for(d)
      });
    });
  })
  .use('update',{ key: 'email', to:'trinox3001@yahoo.com'})
  .xstream(function(){
    this.onError(_.tags.tagDefer('update-error'));
    this.out().on(function(d){
      $('can i update email in stream',function(k){
        k.sync(function(m,g){
          console.log('update-out:',m);
          _.Expects.truthy(m);
        });
        k.for(d)
      });
    });
  })
  .use('saveChangeAndStream')
  .xstream(function(){
    this.in().on(function(d){
      $('can i save data in stream',function(k){
        k.sync(function(m,g){
          console.log('save-in:',m);
          _.Expects.truthy(m);
        });
      }).use(d);
    });

    this.out().on(function(d){
      $('can i receive data saved from stream',function(k){
        k.sync(function(m,g){
          console.log('save-out:',m);
          _.Expects.truthy(m);
        });
        k.for(d)
      });
    });

    this.onError(_.tags.tagDefer('save-error:'+new Date()));
  })
  // .use('destroy',{ name: 'felix'})
  // .use('insert',{ name: 'felix', email:'trinoxf@gmail.com', tel: '0645544545'})
  // .use('saveChange')
  // .use('drop')
  .end();


  var fc = users.future();
  fc.changes().on(_.tags.tagDefer('changes-stream'))
  fc.onError(_.tags.tagDefer('finished-error:'+new Date()));
  fc.onError(function(d){
    conn.down();
  })
  .then(function(d){
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
