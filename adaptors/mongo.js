(function(){

  var _ = require('stackq'),
      live = require('../quero.js'),
      monk = require('monk');

  live.registerProvider('mongodb',live.Connection.extends({
    init: function(f){
      this.$super(f);
      this.dbpath = this.dbMeta.db;
      this.user = this.dbMeta.username;
      this.pass = this.dbMeta.password;
    },
    up: function(){
      if(this.state()) return;
      this.db = monk(this.dbpath);
      this.__switchOn();
      this.emit('up',this);
    },
    down: function(){
      if(!this.state()) return;
      this.db.close();
      this.__switchOff();
      this.emit('down',this);
    },
    registerQueries: function(){
      //external to internal data fetchers for stream
      this.queryStream.where('$find',function(m,q,sx){});
      this.queryStream.where('$findOne',function(m,q,sx){});
      this.queryStream.where('$stream',function(m,q,sx){});
      this.queryStream.where('$streamOne',function(m,q,sx){});

      //internal adders into stream operations
      this.queryStream.where('$insert',function(m,q,sx){});

      //internal stream-item only operations
      this.queryStream.where('$update',function(m,q,sx){});
      this.queryStream.where('$yank',function(m,q,sx){});

      this.queryStream.where('$contains',function(m,q,sx){});
      this.queryStream.where('$index',function(m,q,sx){});
      this.queryStream.where('$limit',function(m,q,sx){});
      this.queryStream.where('$filter',function(m,q,sx){});
      this.queryStream.where('$cycle',function(m,q,sx){});
      this.queryStream.where('$sort',function(m,q,sx){});
      this.queryStream.where('$destroy',function(m,q,sx){});
    },
    get: function(q){
      var res = { query: q, op: 'get' , atom: null};
      if(this.collections.has(q.with)){
        res.message = live.MessageFormat('getDocument','retrieved "'+q.with+'" from cache!');
        res.state = true;
      }
      else{
        if(_.valids.not.contains(q,'with')){
          res.state = false;
          res.message = live.MessageFormat('getDocument','cant retrieve "'+q.with+'" ,check query!')
        }else{
          var k = this.db.get(q.with);
          res.state = k ? true : false;
          this.collections.add(q.with,k);
          res.message = live.MessageFormat('getDocument','retrieved "'+q.with+'"!');
        }
      }
      this.events.emit('get',res);
      return res;
    },
    drop: function(q){
      var res = { query: q, op: 'drop' , atom: null};
      if(this.collections.has(q.with)){
        this.collections.get(q.with).drop(function(err,f){
          res.state = err ? false : true;
          if(!err) res.message = live.MessageFormat('dropCollection','dropped "'+q.with+'" from cache!');
          else res.message = live.MessageFormat('dropDocument','unable to drop '+q.with,err);
          this.events.emit('drop',res);
        });
      }
      return res;
    },
    create: function(q){
      var r = _.Util.clone(this.get(q));
      r.op = 'create';
      r.message.name = 'createDocument';
      this.emit('create',r);
      return r;
    },
    internalOp: function(q){

    },
  }));

}());
