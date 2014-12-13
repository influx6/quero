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
          var model = this.get(m), suc = [],cursor;

          var sid = sx.in();

          sid.onEvent('dataEnd',function(){
            if(sid.isEmpty()) sx.complete(null);
          });

          sid.on(function(doc){
              if(_.valids.isList(doc)){
                _.enums.eachSync(doc,function(e,i,o,fx){
                  cursor =  model.insert(e)
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
                model
                .insert(doc)
                .error(sx.$bind(sx.completeError))
                .success(sx.$bind(sx.complete));
              }
          });

          sx.then(function(d){
            sx.out().emitEvent('dataEnd',true);
          });
      });

      this.queryStream.where('$saveAndStream',function(m,q,sx,sm){
          sx.loopStream();
          var model = this.get(m), suc = [],cursor;

          var sid = sx.in();

          // sid.onEvent('dataEnd',function(){
          //   if(sid.isEmpty()) sx.complete(true);
          // });

          sx.in().on(function(doc){
              if(_.valids.isList(doc)){
                _.enums.eachSync(doc,function(e,i,o,fx){
                  cursor =  model.insert(e)
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
                var id = doc._id;
                model
                .insert(doc)
                .error(sx.$bind(sx.completeError))
                .success(sx.$bind(sx.complete));
              }
          });

          // sx.then(function(d){
          //   // sx.out().emitEvent('dataEnd',true);
          // });
      });

      this.queryStream.where('$saveChange',function(m,q,sx,sm){
          // sx.loopStream();
          var model = this.get(m), suc = [],cursor, sid = sx.in();

          // sid.onEvent('dataEnd',function(){
          //   if(sid.isEmpty()) sx.complete(true);
          // });

          sid.on(function(doc){
              if(_.valids.isList(doc)){
                _.enums.eachSync(doc,function(e,i,o,fx){
                  cursor = model.update({ _id: doc._id },doc,{ upsert: true })
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
                var id = doc._id;
                model
                .update({ _id: doc._id },doc,{ upsert: true })
                .error(sx.$bind(sx.completeError))
                .success(sx.$bind(sx.complete));
              }
          });

          sx.then(function(d){
            sx.out().emitEvent('dataEnd',true);
          });
      });

      this.queryStream.where('$saveChangeAndStream',function(m,q,sx,sm){
          sx.loopStream();
          var model = this.get(m), suc = [],cursor,sid= sx.in();

          // sid.onEvent('dataEnd',function(){
          //   if(sid.isEmpty()) sx.complete(true);
          // });

          sid.on(function(doc){
              if(_.valids.isList(doc)){
                _.enums.eachSync(doc,function(e,i,o,fx){
                  cursor = model.update({ _id: e._id },e,{ upsert: true })
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
                var id = doc._id;
                model
                .updateById(doc._id,doc)
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

      // //internal stream-item only operations
      // this.queryStream.where('$insert',function(m,q,sx,sm){
      //     sx.loopStream();
      //     sx.in().emit(q.key);
      //     sx.complete(q);
      // });
      //
      // this.queryStream.where('$group',function(m,q,sx,sm){
      //   var six = sx.in(), data = [];
      //   six.on(function(d){ data.push(d); });
      //   six.afterEvent('dataEnd',function(){ sx.complete(data); });
      //   sx.then(function(d){
      //     sx.out().emit(d);
      //     sx.out().emitEvent('dataEnd',true);
      //   });
      // });
      //
      // this.queryStream.where('$ungroup',function(m,q,sx,sm){
      //   var six = sx.in(), data = [];
      //
      //   six.on(function(d){
      //     if(_.valids.isList(d)) data = d;
      //     else data.push(d);
      //   });
      //
      //   six.afterEvent('dataEnd',function(){
      //     if(_.valids.notExists(data)) return;
      //     _.enums.each(data,function(e,i,o,fn){
      //       sx.out().emit(e);
      //       return fn(null);
      //     },function(_,err){
      //       sx.out().emitEvent('dataEnd',true);
      //       data = null;
      //     });
      //     sx.complete(data);
      //   });
      //
      // });
      //
      // //format: { condition: , key:, from: , to: , mutator: }
      // this.queryStream.where('$update',function(m,q,sx,sm){
      //   var data = q.key,
      //       condfn = data.condition,
      //       mutator = data.mutator,
      //       key = data.key,
      //       from  = data.from,
      //       to = data.to;
      //
      //   if(_.valids.not.isObject(data)) return sx.completeError(new Error('invalid query object'));
      //
      //   var clond,sid = sx.in(), soud = sx.out(), scand = sx.changes(), updator = function(doc,ke){
      //     var vf = doc[key];
      //     if(from){
      //       if(_.Util.isType(vf) === _.Util.isType(vf) && vf == from){
      //         if(_.Util.isFunction(to)){ doc[key] = to.call(doc,doc[key],from); }
      //         else{
      //           doc[key] = to;
      //         }
      //         return doc;
      //       }
      //       if(_.valids.isRegExp(from) && from.test(vf)){
      //         if(_.Util.isFunction(to)){ doc[key] = to.call(doc,doc[key],from); }
      //         else{
      //           doc[key] = to;
      //         }
      //         return doc;
      //       }
      //     }else{
      //       if(_.Util.isFunction(to)){ doc[key] = to.call(doc,doc[key],from); }
      //       else{
      //         doc[key] = to;
      //       }
      //       return doc;
      //     }
      //   };
      //
      //   sid.afterEvent('dataEnd',function(){
      //     sx.complete(true);
      //     soud.emitEvent.apply(soud,['dataEnd'].concat(_.enums.toArray(arguments)));
      //   });
      //
      //   sid.on(function(doc){
      //
      //     clond = _.Util.clone(doc);
      //     if(_.valids.isFunction(data)){
      //       var ndoc = data.call(doc,doc);
      //       if(_.valids.exist(ndoc)){
      //         soud.emit(ndoc);
      //         scand.emit([ndoc,clond]);
      //       }
      //     }
      //     if(_.valids.isObject(data)){
      //       var udoc;
      //       if(condfn){
      //         if(condfn.call(data,doc)){
      //           if(_.valids.isFunction(mutator)){
      //             udoc = mutator.call(data,doc);
      //           }else{
      //             if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
      //               udoc = updator(doc,key);
      //             }
      //             if(_.valids.isPrimitive(doc)){
      //               if(from && doc == from){
      //                 if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
      //                 else{ udoc = to; }
      //               }
      //               else if(_.valids.isRegExp(from) && from.test(doc)){
      //                 if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
      //                 else{ udoc = to; }
      //               }
      //               else{
      //                 if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
      //                 else{ udoc = to; }
      //               }
      //             }
      //           }
      //         }
      //       }else{
      //         if(_.valids.isFunction(mutator)){
      //           udoc = mutator.call(data,doc);
      //         }else{
      //           if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
      //             udoc = updator(doc,key);
      //           }
      //           if(_.valids.isPrimitive(doc)){
      //             if(from && doc == from){
      //               if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
      //               else{ udoc = to; }
      //             }
      //             else if(_.valids.isRegExp(from) && from.test(doc)){
      //               if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
      //               else{ udoc = to; }
      //             }
      //             else{
      //               if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
      //               else{ udoc = to; }
      //             }
      //           }
      //         }
      //       }
      //
      //       if(_.valids.exists(udoc)){
      //         soud.emit(udoc);
      //         scand.emit([udoc,clond]);
      //       }else{
      //         soud.emit(doc);
      //       }
      //     }
      //   });
      //
      // });
      //
      // //format: { condition: , key:, value:, getter:  , setter:}
      // this.queryStream.where('$yank',function(m,q,sx,sm){
      //   var data = q.key,
      //       condfn = data.condition,
      //       getter = data.getter,
      //       key = data.key,
      //       value  = data.value;
      //
      //   if(_.valids.not.isObject(data)) return;
      //
      //   var clond,sid = sx.in(), soud = sx.out(),report = sx.changes();
      //
      //   sid.afterEvent('dataEnd',function(){
      //     soud.emitEvent.apply(soud,['dataEnd'].concat(_.enums.toArray(arguments)));
      //   });
      //
      //   var udoc = false;
      //   sid.on(function(doc){
      //       if(condfn){
      //         if(condfn.call(data,doc)){
      //           if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
      //             if(value){
      //               vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
      //               if(value && value == vak){ udoc = true; }
      //               if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
      //               if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
      //             }else{
      //               udoc = true;
      //             }
      //           }
      //           if(_.valids.isPrimitive(doc)){
      //             if(value){
      //               if(value && value == doc){ udoc = true; }
      //               if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
      //               if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
      //             }else{
      //               udoc = true;
      //             }
      //           }
      //         }
      //       }else{
      //         if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
      //           if(value){
      //             vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
      //             if(value && value == vak){ udoc = true; }
      //             if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
      //             if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
      //           }else{
      //             udoc = true;
      //           }
      //         }
      //         if(_.valids.isPrimitive(doc)){
      //           if(value){
      //             if(value && value == doc){ udoc = true; }
      //             if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
      //             if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
      //           }else{
      //             udoc = true;
      //           }
      //         }
      //       }
      //       if(udoc){
      //         if(_.valids.isObject(doc)){
      //           var ddoc = _.Util.clone(doc);
      //           if(_.valids.isFunction(setter)){
      //             setter.call(doc,key,null);
      //           }else{
      //             delete doc[key];
      //           };
      //           report.emit([ddoc,doc]);
      //           soud.emit(doc);
      //         }else{
      //           var ddoc = (_.valids.isFunction(setter) ? setter.call(doc,key) : doc);
      //           report.emit([ddoc,doc]);
      //           soud.emit(ddoc);
      //         }
      //       }else{
      //         soud.emit(doc);
      //       }
      //   });
      //
      // });
      //
      // //format: { condition: , key:, value:, getter:  }
      // //format: { condition: , key:, value:  }
      // this.queryStream.where('$contains',function(m,q,sx,sm){
      //   var data = q.key,
      //       condfn = data.condition,
      //       getter = data.getter,
      //       key = data.key,
      //       value  = data.value;
      //
      //   var clond,sid = sx.in(), soud = sx.out();
      //
      //   sid.afterEvent('dataEnd',function(){
      //     soud.emitEvent.apply(soud,['dataEnd'].concat(_.enums.toArray(arguments)));
      //   });
      //
      //   sid.on(function(doc){
      //     if(_.valids.isObject(data)){
      //       var udoc;
      //       if(condfn){
      //         if(condfn.call(data,doc)){
      //           if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
      //             if(value){
      //               var vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
      //               if(value && value == vak){ udoc = true; }
      //               if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
      //               if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
      //             }else{
      //               udoc = true;
      //             }
      //           }
      //           if(_.valids.isPrimitive(doc)){
      //             if(value){
      //               if(value && value == doc){ udoc = true; }
      //               if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
      //               if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
      //             }else{
      //               udoc = true;
      //             }
      //           }
      //         }
      //       }else{
      //         if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
      //           if(value){
      //             var vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
      //             if(value && value == vak){ udoc = true; }
      //             if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
      //             if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
      //           }else{
      //             udoc = true;
      //           }
      //         }
      //         if(_.valids.isPrimitive(doc)){
      //           if(value){
      //             if(value && value == doc){ udoc = true; }
      //             if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
      //             if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
      //           }else{
      //             udoc = true;
      //           }
      //         }
      //       }
      //       if(udoc){
      //         soud.emit(doc);
      //       }
      //     }
      //   });
      //
      // });
      //
      // //format: (total)
      // this.queryStream.where('$limit',function(m,q,sx,sm){
      //   var data = q.key,sid = sx.in(), soud = sx.out(),count = 0;
      //
      //   if(_.valids.not.isNumber(data)) return;
      //
      //   sid.on(function(doc){
      //     if(count >= data) return sid.close();
      //     soud.emit(doc);
      //     count += 1;
      //   });
      //
      //   sid.afterEvent('dataEnd',sx.$bind(sx.complete));
      //
      //
      // });
      //
      // //format: { condition: , key:, value:, getter:  }
      // //format: { condition: , key:, value:  }
      // this.queryStream.where('$filter',function(m,q,sx,sm){
      //   var data = q.key,
      //       condfn = data.condition,
      //       getter = data.getter,
      //       key = data.key,
      //       count = 0,
      //       value  = data.value;
      //
      //   var clond,sid = sx.in(), soud = sx.out();
      //
      //   sid.afterEvent('dataEnd',function(){
      //     if(count <= 0) sx.completeError(new Error('NonFound!'));
      //     else sx.complete(true);
      //     soud.emitEvent.apply(soud,['dataEnd'].concat(_.enums.toArray(arguments)));
      //   });
      //
      //   var udoc = false;
      //   sid.on(function(doc){
      //     if(_.valids.isObject(data)){
      //       if(condfn){
      //         if(condfn.call(data,doc)){
      //           if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
      //             if(value){
      //               var vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
      //               if(value && value == vak){ udoc = true; }
      //               if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
      //               if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
      //             }else{
      //               udoc = true;
      //             }
      //           }
      //           if(_.valids.isPrimitive(doc)){
      //             if(value){
      //               if(value && value == doc){ udoc = true; }
      //               if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
      //               if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
      //             }else{
      //               udoc = true;
      //             }
      //           }
      //         }
      //       }else{
      //         if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
      //           if(value){
      //             var vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
      //             if(value && value == vak){ udoc = true; }
      //             if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
      //             if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
      //           }else{
      //             udoc = true;
      //           }
      //         }
      //         if(_.valids.isPrimitive(doc)){
      //           if(value){
      //             if(value && value == doc){ udoc = true; }
      //             if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
      //             if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
      //           }else{
      //             udoc = true;
      //           }
      //         }
      //       }
      //
      //       if(udoc){
      //         soud.emit(doc);
      //         count += 1;
      //       }
      //     }
      //   });
      //
      // });
      //
      // //format: { condition: , bykey: or byvalue:, getter:  }
      // //format: { condition: , byKey: or byValue:  }
      // this.queryStream.where('$sort',function(m,q,sx,sm){
      //
      // });
      //
      // this.queryStream.where('$byCount',function(m,q,sx,sm){
      //   var total = q.key.total, data = [];
      //
      //   sx.complete(true);
      //   sx.then(function(){
      //
      //     var pushOut = function(){
      //       var cur = _.enums.nthRest(data,0,total);
      //       sx.out().emit(cur);
      //       sx.out().emitEvent('dataEnd',true);
      //     };
      //
      //     sx.in().on(function(d){
      //       if(total && data.length >= total) pushOut();
      //       data.push(d);
      //     });
      //
      //   });
      //
      // });
      //
      // this.queryStream.where('$byMs',function(m,q,sx,sm){
      //   var ms = q.key.ms, data = [];
      //
      //   sx.complete(true);
      //
      //   sx.then(function(){
      //
      //     sx.in().on(function(d){
      //       data.push(d);
      //     });
      //
      //     var pushOut = function(){
      //       var cur = data; data = [];
      //       sx.out().emit(cur);
      //       sx.out().emitEvent('dataEnd',true);
      //     };
      //
      //     var time = setInterval(function(){
      //       return pushOut();
      //     },ms)
      //
      //     sx.onError(function(){
      //       time.cancel();
      //     });
      //
      //   });
      //
      //
      // });


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
