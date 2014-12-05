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
      this.queryStream.where('$find',this.$bind(function(m,q,sx){
        var mode = this.get(m);
        mode
        .find(q.key)
        .error(sx.$bind(sx.completeError))
        .success(sx.$bind(sx.complete));

        sx.then(function(doc){
          var sm = sx.out();
          _.enums.each(doc,sx.$bind(sm.emit),function(_,err){
            sm.emitEvent('dataEnd',true);
          });
        });

      }));

      this.queryStream.where('$findOne',function(m,q,sx,sm){
        var mode = this.get(m);
        mode
        .findOne(q.key)
        .error(sx.$bind(sx.completeError))
        .success(sx.$bind(sx.complete));

        sx.then(function(doc){
          var sm = sx.out();
          sm.emit(doc);
          sm.emitEvent('dataEnd',true);
        });

      });

      this.queryStream.where('$stream',function(m,q,sx,sm){

      });

      this.queryStream.where('$streamOne',function(m,q,sx,sm){

      });

      //internal adders into stream operations
      this.queryStream.where('$insert',function(m,q,sx,sm){
          var k = sx.in().stream(sx.out());
          sx.out().emit(q.key);
          sx.out().emitEvent('dataEnd',true);
          sx.complete(q);
      });

      //internal stream-item only operations
      this.queryStream.where('$update',function(m,q,sx,sm){});

      this.queryStream.where('$yank',function(m,q,sx,sm){});

      this.queryStream.where('$contains',function(m,q,sx,sm){});

      this.queryStream.where('$index',function(m,q,sx,sm){});

      this.queryStream.where('$limit',function(m,q,sx,sm){});

      this.queryStream.where('$filter',function(m,q,sx,sm){});

      this.queryStream.where('$cycle',function(m,q,sx,sm){});

      this.queryStream.where('$sort',function(m,q,sx,sm){});

      this.queryStream.where('$destroy',function(m,q,sx,sm){
        var model = this.get(m);
        sx.in().on(function(doc){
          model.remove(doc)
          .error(sx.$bind(sx.completeError))
          .success(sx.$bind(sx.complete));
        });
      });

      this.queryStream.where('$save',function(m,q,sx,sm){
        // sx.connectStreams();
          var model = this.get(m);
          sx.in().on(function(doc){
              model
              .insert(doc)
              .error(sx.$bind(sx.completeError))
              .success(sx.$bind(sx.complete));
          });
      });
    },
    get: function(q){
      if(!this.collections.has(q)){
        var k = this.db.get(q);
        this.collections.add(q.with,k);
      }
      var kod =  this.collections.get(q.with);
      kod.options.multi = true;
      this.events.emit('get',kod);
      return kod;
    },
    drop: function(q){
      if(this.collections.has(q)){
        this.collections.get(q).drop(function(err,f){
          this.events.emit('drop',f);
        });
      }
    },
    internalOp: function(q){

    },
  }));

}());
