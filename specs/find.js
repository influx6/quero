var _ = require('stackq'),
quero = require('../quero.js');

_.enums.eachBy(function(e){
  return _.enums.length(e) > 0;
},function(m){

    require(m.module);

    _.Jazz(m.tag,function($){

      var conn = quero.make({
        adaptor: m.db,
        db: m.url,
      });


      $('can i create a livedb instance',function(k){
        k.sync(function(c,g){
          _.Expects.truthy(c);
          _.Expects.isInstanceOf(c,quero);
        });
        k.for(conn);
      });

      conn.up();

      conn.schema('brothers',{
        name: 'string',
        age: 'number',
      });

      var users = conn.model('brothers');

      users
      .use('index',{ index:'name', options: { unique: true }})
      .use('insert',{'name':'alex', age: 29, ps: true})
      .use('insert',{'name':'john', age: 29})
      .use('insert',{'name':'felix', age: 30})
      .use('insert',{'name':'jerry', age: 50})
      .xstream(function(){
        this.out().on(function(d){
          $('can i insert data into db',function(k){
            k.sync(function(m,g){
              _.Expects.isObject(m);
            });
          }).use(d);
        });
      })
      .use('save')
      .xstream(function(){
        this.in().on(function(d){
          $('can i save data into db',function(k){
            k.sync(function(m,g){
              _.Expects.isObject(m);
            });
          }).use(d);
        });
      })
      .use('find',{})
      .xstream(function(){
        this.out().on(function(d){
          $('can i find data into db',function(k){
            k.sync(function(f,g){
              _.Expects.isObject(f);
            });
          }).use(d);
        });
      })
      .use('yank',{ key:'ps'  })
      .use('update',{ key:'age', from: function(i){ return i > 20; }, to: function(i){ return i/100;}})
      .use('contains',{ key:'name', value: /felix/  })
      .use('destroy')
      .use('drop')
      .end();

      users.future()
      .onError(function(d){
        conn.down();
      })
      .then(function(d){
        conn.down();
      });

    });

})([{
    module:'../adaptors/mongo.js',
    db: 'mongodb',
    tag: 'mongodb specification',
    url: 'localhost/mydb',
},{
    module:'../adaptors/inmemory.js',
    db: 'inMemory',
    tag: 'inMemory specification',
    url: 'localhost/mydb'
}]);
