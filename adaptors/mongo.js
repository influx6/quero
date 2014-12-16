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
      return this.$super(function(){
        this.db = monk(this.dbpath);
      });
    },
    down: function(){
      return this.$super(function(){
        this.db.close();
      });
    },
    registerQueries: function(){
      //external to internal data fetchers for stream
      this.queryStream.where('$find',function(m,q,sx){

        var mode = this.get(m);
        mode
        .find(q.key)
        .error(sx.$bind(sx.completeError))
        .success(function(d){
          sx.complete(d);
        });

        sx.then(function(doc){
          var sm = sx.out();
          _.enums.each(doc,sm.$bind(sm.emit),function(_,err){
            sm.emitEvent('dataEnd',true);
          });
        });


      });

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
        var map = this.get(m), count = 0;
        var index = this.index, by = index ? index.index : null;

        sx.in().on(this.$bind(function(doc){
          map.remove(doc)
          .error(sx.$bind(sx.completeError))
          .success(this.$bind(function(){
             this.changes.emit({ 'i': 'd', record: doc });
             count += 1;
          }));
        }));

        sx.in().onEvent('dataEnd',function(){
          sx.out().emit({});
          sx.out().endData();
          sx.complete({ 'records': count, with: q.key, op: 'destroy' });
        });

      });

      this.queryStream.where('$destroyAndStream',function(m,q,sx,sm){
        sx.loopStream();
        var map = this.get(m), count = 0;
        var index = this.index, by = index ? index.index : null;

        sx.in().on(this.$bind(function(doc){
          map.remove(doc)
          .error(sx.$bind(sx.completeError))
          .success(this.$bind(function(){
             this.changes.emit({ 'i': 'd', record: doc });
             count += 1;
          }));
        }));

        sx.in().onEvent('dataEnd',function(){
          sx.complete({ 'records': count, with: q.key, op: 'destroy' });
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

        if(_.valids.isString(q.key)){ ind = {}; ind.index = q.key; };
        if(_.valids.isObject(q.key)){ ind = q.key; }
        if(_.valids.isList(q.key)){ ind = {}; ind.index = q.key };
        if(_.valids.not.isString(q.key) && _.valids.not.isList(q.key) && _.valids.not.isObject(q.key)) return;
        ind.options = q.key.options;

        mode
        .index(ind.index,ind.options)
        .error(sx.$bind(sx.completeError))
        .success(function(d){
          sx.complete({'op':'index', by: q.key, res: d});
        });

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
            model.drop();
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
