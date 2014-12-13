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
    this.out().on(function(d){
      $('can i retrieve data from db',function(k){
        k.sync(function(m,g){
          _.Expects.truthy(m);
        });
      }).use(d);
    });
  })
  .use('update',{ key: 'name', from:'alex', to: 'buttocks'})
  .use('filter',{ key: 'name', value:'john'})
  .xstream(function(){
    this.in().on(function(d){
      $('can i filter data in stream',function(k){
        k.sync(function(m,g){
          _.Expects.truthy(m);
        });
      }).use(d);
    });

    this.out().on(function(d){
      $('can i filter data saved from stream',function(k){
        k.sync(function(m,g){
          _.Expects.truthy(m);
        });
        k.for(d)
      });
    });
  })
  .use('update',{ key: 'email', to:'trinox3001@yahoo.com'})
  .xstream(function(){
    this.out().on(function(d){
      $('can i update email in stream',function(k){
        k.sync(function(m,g){
          _.Expects.truthy(m);
        });
        k.for(d)
      });
    });
  })
  .use('saveAndStream')
  .xstream(function(){
    this.in().on(function(d){
      $('can i save data in stream',function(k){
        k.sync(function(m,g){
          _.Expects.truthy(m);
        });
      }).use(d);
    });

    this.out().on(function(d){
      $('can i receive data saved from stream',function(k){
        k.sync(function(m,g){
          _.Expects.truthy(m);
        });
        k.for(d)
      });
    });

    this.then(_.tags.tagDefer('save-done:'+new Date()));
    this.onError(_.tags.tagDefer('save-error:'+new Date()));
  })
  .use('destroy',{ name: 'felix'})
  .use('insert',{ name: 'felix', email:'trinoxf@gmail.com', tel: '0645544545'})
  .xstream(function(){
    this.in().on(function(d){
      console.log('in',d);
    });

    this.out().on(function(d){
      console.log('out',d);
    });
    this.then(_.tags.tagDefer('insert-done:'+new Date()));
    this.onError(_.tags.tagDefer('insert-error:'+new Date()));
  })
  .use('save',{ id: 'name' })
  .xstream(function(){
    this.in().on(function(d){
      console.log('save2-in',d);
    });

    this.out().on(function(d){
      console.log('save2-out',d);
    });
    this.then(_.tags.tagDefer('save2-done:'+new Date()));
    this.onError(_.tags.tagDefer('save2-error:'+new Date()));
  })
  // .use('drop')
  .end();


  var fc = users.future();
  fc.onError(function(d){
    console.log('errord:',d);
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

});
