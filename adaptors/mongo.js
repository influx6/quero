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
      _.Util.nextTick(this.$bind(function(){
        this.db.close();
        this.__switchOff();
        this.emit('down',this);
      }));
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
          _.enums.each(doc,sm.$bind(sm.emit),function(_,err){
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
        var mode = this.get(m),
        fn = m.key,
        cursor = mode.find({},{ stream: true});

        cursor.each(function(doc){
          if(fn.call(null,doc)){
            sx.out().emit(doc);
            sx.out().emitEvent('dataEnd',true);
          }
        })
        .error(sx.$bind(sx.completeError))
        .success(sx.$bind(sx.complete));

      });

      this.queryStream.where('$streamOne',function(m,q,sx,sm){
        var mode = this.get(m),
        fn = m.key,
        cursor = mode.find({},{ stream: true});

        cursor.each(function(doc){
          if(fn.call(null,doc)){
            sx.out().emit(doc);
            sx.out().emitEvent('dataEnd',true);
            cursor.destroy();
          }
        })
        .error(sx.$bind(sx.completeError))
        .success(sx.$bind(sx.complete));

      });

      this.queryStream.where('$destroy',function(m,q,sx,sm){
        // sx.loopStream();
        var model = this.get(m);
        sx.in().on(function(doc){
          model.remove(doc)
          .error(sx.$bind(sx.completeError))
          .success(sx.$bind(sx.complete));
        });

        sx.then(function(d){
          sx.out().emitEvent('dataEnd',true);
        });
      });

      this.queryStream.where('$destroyAndStream',function(m,q,sx,sm){
        sx.loopStream();
        var model = this.get(m);
        sx.in().on(function(doc){
          model.remove(doc)
          .error(sx.$bind(sx.completeError))
          .success(sx.$bind(sx.complete));
        });
      });

      this.queryStream.where('$save',function(m,q,sx,sm){
          // sx.loopStream();
          var model = this.get(m), suc = [],cursor, sid = sx.in(), by = q.key ? q.key.id : null;

          sid.on(function(doc){
              if(_.valids.isList(doc)){
                _.enums.eachSync(doc,function(e,i,o,fx){
                  var find = {};
                  if(by && _.valids.containsKey(e,by)) find[by] = e[by];
                  else find['_id'] = e._id;
                  cursor = model.update(find,e,{ upsert: true })
                  .error(fx)
                  .success(function(d){
                    suc.push(d);
                    return fx(null);
                  });
                },function(_,err){
                   if(err) return sx.completeError(err);
                   return sx.complete(suc);
                });
              }else{
                var find = {};
                if(by && _.valids.containsKey(doc,by)) find[by] = doc[by];
                else find['_id'] = doc._id;
                model
                .update(find,doc,{ upsert: true })
                .error(sx.$bind(sx.completeError))
                .success(sx.$bind(sx.complete));
              }
          });

          sx.then(function(d){
            sx.out.emit({});
            sx.out().emitEvent('dataEnd',true);
          });
      });

      this.queryStream.where('$saveAndStream',function(m,q,sx,sm){
          sx.loopStream();
          var model = this.get(m), suc = [],cursor, sid = sx.in(),by = q.key ? q.key.id : null;

          sid.on(function(doc){
              if(_.valids.isList(doc)){
                _.enums.eachSync(doc,function(e,i,o,fx){
                  var find = {};
                  if(by && _.valids.containsKey(e,by)) find[by] = e[by];
                  else find['_id'] = e._id;
                  cursor = model.update(find,e,{ upsert: true })
                  .error(fx)
                  .success(function(d){
                    suc.push(d);
                    return fx(null);
                  });
                },function(_,err){
                   if(err) return sx.completeError(err);
                   return sx.complete(suc);
                });
              }else{
                var find = {};
                if(by && _.valids.containsKey(doc,by)) find[by] = doc[by];
                else find['_id'] = doc._id;
                model
                .update(find,doc,{ upsert: true })
                .error(sx.$bind(sx.completeError))
                .success(sx.$bind(sx.complete));
              }
          });

          // sx.then(function(d){
          //   sx.out().emitEvent('dataEnd',true);
          // });
      });

      this.queryStream.where('$index',function(m,q,sx,sm){
        var mode = this.get(m), ind;

        if(_.valids.isString(q.key)){ ind = {}; ind.indexes = q.key; };
        if(_.valids.isObject(q.key)){ ind = q.key; }
        if(_.valids.isList(q.key)){ ind = {}; ind.indexes = q.key };
        if(_.valids.not.isString(q.key) && _.valids.not.isList(q.key) && _.valids.not.isObject(q.key)) return;
        ind.options = q.key.options;

        mode
        .index(ind.indexes,ind.options)
        .error(sx.$bind(sx.completeError))
        .success(sx.$bind(sx.complete));

        sx.then(function(doc){
          var sm = sx.out();
          sm.emitEvent('dataEnd',true);
        });

      });

      this.queryStream.where('$drop',function(m,q,sx,sm){
          sx.loopStream();
          sx.connectStreams();

          sx.then(this.$bind(function(){
            var model = this.get(m);
            model
            .drop()
            .error(sx.$bind(sx.completeError))
            .success(sx.$bind(sx.complete));
          }));

      });

    },
    get: function(q){
      if(!this.collections.has(q)){
        var k = this.db.get(q);
        this.collections.add(q.with,k);
      }
      var kod =  this.collections.get(q.with);
      // kod.options.multi = true;
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
