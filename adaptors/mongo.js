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
      this.collections = {};
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
    get: function(q){
      var res = { query: q, op: 'get' , atom: null};
      if(_.valids.contains(this.collections,q.with)){
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
          this.collections[q.with] = k;
          res.message = live.MessageFormat('getDocument','retrieved "'+q.with+'"!');
        }
      }
      this.events.emit('get',res);
      return res;
    },
    drop: function(q){
      var res = { query: q, op: 'drop' , atom: null};
      if(_.valids.contains(this.collections,q.with)){
        this.collections[q.with].drop(function(err,f){
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
    job: function(q){

    },
    internalOp: function(q){

    },
  }));

}());
