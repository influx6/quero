module.exports = (function(){

  var _ = require('stackq');

  /*
    livedb db connection format
    {
      db: db_name,
      username: user_name,
      password: pass_word
    }

  */
  var MessageFormat = function(name,message,err){
    return {
      name: name,
      message: message,
      err: err,
    };
  };

  var QueryFormat = function(query,op,atom,status,message){
    return {
      query: query,
      op: op,
      atom: atom,
      status: status,
      message: message,
    };
  };

  var Connections = _.Class({
    init: function(){
      this.providers = _.Storage.make('dbProviders');
    },
    register: function(name,col){
      if(!Connection.isType(col)) return;
      this.providers.add(name,col);
    },
    unregister: function(name){
      this.providers.remove(name);
    },
    create: function(name,n){
      return this.providers.Q(name).make(n);
    },
    has: function(name){
      return this.providers.has(name);
    }
  });

  var Connection = _.Class({
    init: function(meta){
      _.Asserted(_.valids.isObject(meta),'a configuration object/map must be passed!');
      _.Asserted(_.valids.contains(meta,'db'),'a "db" attr must exists within the config object');
      this.dbMeta = meta;
      this.events = _.EventStream.make();
      this.queryStream = _.QueryStream(this);
      this.silentEvents = meta.silentEvents || false;
      this.collections = _.Storage.make('connector-collections');
      this.changes = _.Stream.make();
      this.queries = _.Stream.make();

      this.changes.transform(function(i){
        if(_.valids.isObject(i)) return i;
        return null;
      });

      this.queries.transform(function(i){
        if(_.valids.isObject(i)) return i;
        return null;
      });

      var state = _.Switch();
      this.state = function(){ return state.isOn(); };
      this.__switchOn = function(){ return state.on(); };
      this.__switchOff = function(){ return state.off(); };

      this.silentEvents = function(fn){
        if(!this.silentEvents) return;
        return fn.apply(this,_.enums.toArray(arguments,1));
      };

      //top-level connection-step events
      this.events.events('up:fail');
      this.events.events('down:fail');
      this.events.events('querySync:fail');
      this.events.events('query');
      this.events.events('query:fail');
      this.events.events('up');
      this.events.events('down');
      this.events.events('drop');
      this.events.events('get');
      this.events.events('create');
      this.events.events('internalOp');

      //low-level queryStream events
      this.events.events('query:done');
      this.events.events('save');
      this.events.events('save:fail');
      this.events.events('update');
      this.events.events('update:fail');
      this.events.events('find');
      this.events.events('find:fail');
      this.events.events('destroy');
      this.events.events('destroy:fail');

      //hook up object-to-event proxies
      this.events.hookProxy(this);
      this.registerQueries();

      this.$secure('queryJob',function(q,fx){
        if(!_.Query.isQuery(q)) return;
        this.after('up',this.$bind(function(){
          var res = this.queryStream.query(q);
          if(_.valids.isFunction(fx)) fx.call(res);
          // this.emit('query',q);
        }));
        this.after('up:fail',this.$bind(function(){
          this.emit('querySync:fail',q);
        }));
      });

    },
    up: function(fn){
      if(this.state()) return;
      _.funcs.immediate(this.$bind(fn));
      this.__switchOn();
      this.emit('up',this);
    },
    down: function(fn){
      if(!this.state()) return;
      _.Util.nextTick(this.$bind(function(){
        _.funcs.immediate(this.$bind(fn));
        this.__switchOff();
        this.emit('down',this);
      }));
    },
    registerQueries: function(){
      /* define the queries types for the querystream*/
    },
    requestQuery: function(qr,fn){
      if(!_.Query.isQuery(qr)) return;
      var packets = [], ft = _.FutureStream.make();
      var push = _.funcs.bind(function(f){ this.push(f); },packets);
      var ftemit = ft.changes().$bind(ft.changes().emit);

      qr.defineWith(fn);

      this.changes.on(push);
      this.changes.on(ftemit)
      qr.future = function(){ return ft; };
      qr.notify.addOnce(this.$bind(function(q){
        ft.query = q;

        ft.then(this.$bind(function(){
          this.emit('query',q);
        }));

        ft.onError(this.$bind(function(){
          this.emit('query:fail',q);
        }));

        this.queryJob(q,function(){
          if(this && _.FutureStream.isType(this)){
            this.chain(ft);
          }
        });

      }));

      qr.future().then(this.$bind(function(){
        this.changes.endData();
        this.changes.end();
        this.changes.off(push);
        this.changes.off(ftemit);
        this.queries.emit({ q: ft.query,  changes: packets});
      }));
      return qr;
    },
    request: function(title,fx,fn){
      _.Asserted(_.valids.isString(title),'a string title/name for model must be supplied');
      title = title.toLowerCase();
      var packets = [], qr = _.Query(title,fx,fn), ft = _.FutureStream.make();
      var push = _.funcs.bind(function(f){ this.push(f); },packets);
      var ftemit = ft.changes().$bind(ft.changes().emit);

      this.changes.on(push);
      this.changes.on(ftemit)
      qr.future = function(){ return ft; };
      qr.notify.addOnce(this.$bind(function(q){
        ft.query = q;

        ft.then(this.$bind(function(){
          this.emit('query',q);
        }));

        ft.onError(this.$bind(function(){
          this.emit('query:fail',q);
        }));

        this.queryJob(q,function(){
          if(this && _.FutureStream.isType(this)){
            this.chain(ft);
          }
        });

      }));

      qr.future().then(this.$bind(function(){
        this.changes.endData();
        this.changes.end();
        this.changes.off(push);
        this.changes.off(ftemit);
        this.queries.emit({ q: ft.query,  changes: packets});
      }));
      return qr;
    },
    query: function(q){
      if(_.valids.not.isObject(q) || _.valids.not.contains(q,'query')) return;
      var qr = q.query;
      var res;
      switch(qr){
        case 'get':
          res = this.get(q);
          break;
        case 'drop':
          res = this.drop(q);
          break;
        case 'query':
          res = this.queryJob(q);
          break;
        case 'internalOp':
          res = this.internalOp(q);
          break;
        default:
          res = QueryFormat(q,qr,null,false);
          res.message = MessageFormat('queryError','query "'+qr+'" can not be handled',new Error('unknown query'));
          this.emit('query:fail',res);
          break;
      };
      return res;
    },
    get: function(q){
      _.Asserted(false,"must redefine function 'get' in subclass");
    },
    drop: function(q){
      _.Asserted(false,"must redefine function 'drop' in subclass");
    },
    internalOp: function(){
      _.Asserted(false,"must redefine function 'internalOp' in subclass");
    },
  });

  var Quero = _.Configurable.extends({
      init: function(meta){
        _.Asserted(_.valids.contains(meta,'adaptor'),'adaptor used not specified in config, eg. { adaptor: mongodb,.. }');
        _.Asserted(_.valids.contains(meta,'db'),'db path used not specified in config, eg. { adaptor:redis,db: localhost:3009/db,.. }');
        _.Asserted(Quero.hasProvider(meta['adaptor']),'unknown adaptor/connector "'+meta['adaptor']+'", check config');
        this.$super();

        this.config({
          silentEvents: false,
        });

        this.config(meta);
        this.uuid = _.Util.guid();
        this.events = _.EventStream.make();
        this.schemas = _.Storage.make('quero-schemas');
        this.slaves = _.Storage.make('quero-slaves');
        this.connection = Quero.createProvider(this.getConfigAttr('adaptor'),meta);
        this.qstreamCache = [];

        Quero.schemaOperands.each(this.$bind(function(e,i,o,fx){
          if(this.connection.queryStream.hasWhere(i)) return fx(null);
          this.connection.queryStream.where(i,e);
        }));

        var master = null;

        this.silentEvents = function(fn){
          if(!this.getConfigAttr(silentEvents)) return;
          return fn.apply(this,_.enums.toArray(arguments,1));
        };

        this.master = function(con){
          if(this.hasMaster() || (!Connection.isType(con) && !Quero.isType(con))) return;
          master = con;
          if(Quero.isInstance(master)){
            master.on('querySync',this.syncQuery);
            master.slave(this);
          }else{
            master.on('query',this.syncQuery);
          }
          this.emit('newMaster',master);
        };

        this.isMaster = function(n){
          return master === n;
        };

        this.hasMaster = function(){
          return master != null;
        };

        this.releaseMaster = function(){
          if(!this.hasMaster()) return;
          master.off('querySync',this.syncQuery);
          this.emit('deadMaster',master);
          master = null;
        };

        this.events.events('up');
        this.events.events('down');
        this.events.events('up:fail');
        this.events.events('down:fail');
        this.events.events('querySync:fail');
        this.events.events('querySync');
        this.events.events('newMaster');
        this.events.events('deadMaster');
        this.events.events('newSlave');
        this.events.events('deadSlave');


        this.connection.after('query',this.$bind(function(){
          this.emit('querySync',this);
        }));
        this.connection.after('query:fail',this.$bind(function(){
          this.emit('querySync:fail',this);
        }));
        this.connection.after('up',this.$bind(function(){
          this.emit('up',this);
        }));
        this.connection.after('down',this.$bind(function(){
          this.emit('down',this);
        }));
        this.connection.after('up:fail',this.$bind(function(){
          this.emit('up:fail',this);
        }));
        this.connection.after('down:fail',this.$bind(function(){
          this.emit('down:fail',this);
        }));

        this.events.hookProxy(this);

      },
      up: function(){
        return this.connection.up();
      },
      down: function(){
        return this.connection.down();
      },
      schema: function(title,map,meta,vals){
        _.Asserted(_.valids.isString(title),'a string title/name for model must be supplied');
        _.Asserted(_.valids.isObject(map),'a object map of model properties must be supplied');
        title = title.toLowerCase();
        if(this.schemas.has(title)) return;
        this.schemas.add(title,_.Schema({},map,meta,vals));
      },
      model: function(title,fn){
        _.Asserted(_.valids.isString(title),'a string title/name for model must be supplied');
        title = title.toLowerCase();
        var qr = this.connection.request(title,this.$bind(function(){
          return this.schemas.get(title);
        }),fn);
        return qr;
      },
      modelQuery:function(q,fn){
        _.Asserted(_.Query.isQuery(q),'must be a query object');
        var qr = this.connection.requestQuery(q,this.$bind(function(){
          return this.schemas.get(q.with);
        }),fn);
        return qr;
      },
      slave: function(t,conf){
        if(Quero.isType(t)) return this.slaveInstance(t,conf);
        if(Connection.isType(t)) return this.slaveConnection(t,conf);
      },
      unslave: function(t,conf){
        if(Quero.isType(t)) return this.unslaveInstance(t,conf);
        if(Connection.isType(t)) return this.unslaveConnection(t,conf);
      },
      slaveInstance: function(t,conf){
        if(!Quero.isType(t)) return;
        if(t.hasMaster() || this.isMaster(t)) return;
        t.config(conf);
        t.master(this);
        this.slaves.add(con);
        this.on('querySync',t.querySync);
        this.emit('newSlave',t,conf);
      },
      unslaveInstance: function(t,conf){
        if(!Quero.isType(t)) return;
        if(!t.hasMaster() || !this.isMaster(t) || !this.slaves.has(t)) return;
        t.releaseMaster();
        this.emit('deadSlave',t,conf);
      },
      slaveConnection: function(type,meta){
        if(_.valids.isString(type) && Quero.hasProvider(type)){
          var con =  Quero.createProvider(type,meta);
          this.emit('newSlave',con,meta);
          this.on('querySync',con.queryJob);
          this.slaves.add(con);
        }
        if(!Connection.isType(type) || this.isMaster(type)) return;
        if(this.slaves.has(type)) return;
        this.on('querySync',type.queryJob);
        this.slaves.add(type);
        this.emit('newSlave',type,meta);
      },
      unslaveConnection: function(type){
        if(!Connection.isType(type) || !this.slaves.has(type)) return;
        this.off('querySync',type.queryJob);
        this.slaves.remove(type);
        this.emit('deadSlave',type);
      },
    },{
      operations: {
        'i': 'insert',
        'u':'update',
        'd': 'destroy',
        'y': ['yank','removeProperty','removeAttr'],
        'dr':'drop'
      },
      schemaOperands: _.Storage.make('schema-operations-map'),
      providers: Connections.make(),
      Connections: Connections,
      Connection: Connection,
      MessageFormat: MessageFormat,
      QueryFormat: QueryFormat,
      hasWhereSchema: function(tag){
        return Quero.schemaOperands.has(tag);
      },
      whereSchema: function(tag,fn){
        return Quero.schemaOperands.add(tag,fn);
      },
      unwhereSchema: function(tag,fn){
        return Quero.schemaOperands.remove(tag);
      },
      createProvider: function(n,f){
        return Quero.providers.create(n,f);
      },
      registerProvider: function(n,f){
        return Quero.providers.register(n,f);
      },
      unregisterProvider: function(n,f){
        return Quero.providers.unregister(n,f);
      },
      hasProvider: function(f){
        return Quero.providers.has(f);
      }
  });

  //internal stream-item only operations
  Quero.whereSchema('$insert',function(m,q,sx,sm){
      sx.loopStream();
      sx.in().onceEvent('drain',function(){ sx.in().endData(); });
      sx.in().emit(q.key);
      this.changes.emit({ 'i': 'i', record: q.key });
      sx.complete({'op':'insert','state':true});
  });

  Quero.whereSchema('$group',function(m,q,sx,sm){
    var six = sx.in(), data = [];
    six.on(function(d){ data.push(d); });
    six.afterEvent('dataEnd',function(){
      sx.complete({'op':'group','state': data.length > 0 ? true : false });
    });
    sx.then(function(d){
      sx.out().emit(data);
      sx.out().emitEvent('dataEnd',true);
    });
  });

  Quero.whereSchema('$ungroup',function(m,q,sx,sm){
    var six = sx.in(), data = [];

    six.on(function(d){
      if(_.valids.isList(d)) data = d;
      else data.push(d);
    });

    six.afterEvent('dataEnd',function(){
      if(_.valids.notExists(data)) return;
      _.enums.each(data,function(e,i,o,fn){
        sx.out().emit(e);
        return fn(null);
      },function(_,err){
        sx.out().emitEvent('dataEnd',true);
        data = null;
      });
      sx.complete(data);
    });

  });

  //format: { condition: , key:, from: , to: , mutator: }
  Quero.whereSchema('$update',function(m,q,sx,sm){
    var data = q.key,
        condfn = data.condition,
        mutator = data.mutator,
        key = data.key,
        from  = data.from,
        to = data.to;

    if(_.valids.not.isObject(data)) return sx.completeError(new Error('invalid query object'));

    var clond,sid = sx.in(), soud = sx.out(), scand = sx.changes(), updator = function(doc,ke){
      var vf = doc[key];
      if(from){
        if(_.Util.isType(vf) === _.Util.isType(vf) && vf == from){
          if(_.Util.isFunction(to)){ doc[key] = to.call(doc,doc[key],from); }
          else{
            doc[key] = to;
          }
          return doc;
        }
        if(_.valids.isFunction(from) && from.call(null,vf)){
          if(_.Util.isFunction(to)){ doc[key] = to.call(doc,doc[key],from); }
          else{
            doc[key] = to;
          }
          return doc;
        }
        if(_.valids.isRegExp(from) && from.test(vf)){
          if(_.Util.isFunction(to)){ doc[key] = to.call(doc,doc[key],from); }
          else{
            doc[key] = to;
          }
          return doc;
        }
      }else{
        if(_.Util.isFunction(to)){ doc[key] = to.call(doc,doc[key],from); }
        else{
          doc[key] = to;
        }
        return doc;
      }
    };

    sid.afterEvent('dataEnd',function(){
      sx.complete(true);
      soud.emitEvent.apply(soud,['dataEnd'].concat(_.enums.toArray(arguments)));
    });

    sid.on(this.$bind(function(doc){

      clond = _.Util.clone(doc);
      if(_.valids.isFunction(data)){
        var ndoc = data.call(doc,doc);
        if(_.valids.exist(ndoc)){
          soud.emit(ndoc);
          scand.emit([ndoc,clond]);
        }
      }
      if(_.valids.isObject(data)){
        var udoc;
        if(condfn){
          if(condfn.call(data,doc)){
            if(_.valids.isFunction(mutator)){
              udoc = mutator.call(data,doc);
            }else{
              if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
                udoc = updator(doc,key);
              }
              if(_.valids.isPrimitive(doc)){
                if(from && doc == from){
                  if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
                  else{ udoc = to; }
                }
                else if(_.valids.isFunction(from) && from.call(null,doc)){
                  if(_.Util.isFunction(to)){ udoc = to.call(doc,doc[key],from); }
                  else{
                    doc[key] = to;
                  }
                  return doc;
                }
                else if(_.valids.isRegExp(from) && from.test(doc)){
                  if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
                  else{ udoc = to; }
                }
                else{
                  if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
                  else{ udoc = to; }
                }
              }
            }
          }
        }else{
          if(_.valids.isFunction(mutator)){
            udoc = mutator.call(data,doc);
          }else{
            if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
              udoc = updator(doc,key);
            }
            if(_.valids.isPrimitive(doc)){
              if(from && doc == from){
                if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
                else{ udoc = to; }
              }
              else if(_.valids.isRegExp(from) && from.test(doc)){
                if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
                else{ udoc = to; }
              }
              else{
                if(_.Util.isFunction(to)){ udoc = to.call(doc,doc,from); }
                else{ udoc = to; }
              }
            }
          }
        }

        if(_.valids.exists(udoc)){
          soud.emit(udoc);
          this.changes.emit({ 'i': 'u', old: clond, new: udoc });
        }else{
          soud.emit(doc);
        }
      }
    }));

  });

  //format: { condition: , key:, value:, getter:  , setter:}
  Quero.whereSchema('$yank',function(m,q,sx,sm){
    var data = q.key,
        condfn = data.condition,
        getter = data.getter,
        setter = data.setter,
        key = data.key,
        value  = data.value;

    if(_.valids.not.isObject(data)) return;

    var clond,sid = sx.in(), soud = sx.out(),report = sx.changes(),total = 0;

    sid.afterEvent('dataEnd',function(){
      sx.complete({'op':'yank', records: total});
      soud.emitEvent.apply(soud,['dataEnd'].concat(_.enums.toArray(arguments)));
    });

    var udoc = false;
    sid.on(this.$bind(function(doc){
        if(condfn){
          if(condfn.call(data,doc)){
            if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
              if(value){
                vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
                if(value && value == vak){ udoc = true; }
                if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
                if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
              }else{
                udoc = true;
              }
            }
            if(_.valids.isPrimitive(doc)){
              if(value){
                if(value && value == doc){ udoc = true; }
                if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
                if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
              }else{
                udoc = true;
              }
            }
          }
        }else{
          if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
            if(value){
              vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
              if(value && value == vak){ udoc = true; }
              if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
              if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
            }else{
              udoc = true;
            }
          }
          if(_.valids.isPrimitive(doc)){
            if(value){
              if(value && value == doc){ udoc = true; }
              if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
              if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
            }else{
              udoc = true;
            }
          }
        }
        if(udoc){
          total +=1;
          if(_.valids.isObject(doc)){
            var ddoc = _.Util.clone(doc);
            if(_.valids.isFunction(setter)){
              setter.call(doc,key,null);
            }else{
              delete doc[key];
            };
            // report.emit([ddoc,doc]);
            this.changes.emit({ 'i': 'y', old: ddoc, new: doc });
            soud.emit(doc);
          }else{
            var ddoc = (_.valids.isFunction(setter) ? setter.call(doc,key) : doc);
            this.changes.emit({ 'i': 'y', old: ddoc, new: doc });
            // report.emit([ddoc,doc]);
            soud.emit(ddoc);
          }
        }else{
          soud.emit(doc);
        }
    }));

  });

  //format: { condition: , key:, value:, getter:  }
  //format: { condition: , key:, value:  }
  Quero.whereSchema('$contains',function(m,q,sx,sm){
    var data = q.key,
        condfn = data.condition,
        getter = data.getter,
        key = data.key,
        value  = data.value;

    var clond,sid = sx.in(), soud = sx.out(),total = 0;

    sid.afterEvent('dataEnd',function(){
      sx.complete({'op':'contains', records: total, by: data});
      soud.emitEvent.apply(soud,['dataEnd'].concat(_.enums.toArray(arguments)));
    });

    sid.on(function(doc){
      if(_.valids.isObject(data)){
        var udoc;
        if(condfn){
          if(condfn.call(data,doc)){
            if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
              if(value){
                var vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
                if(value && value == vak){ udoc = true; }
                if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
                if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
              }else{
                udoc = true;
              }
            }
            if(_.valids.isPrimitive(doc)){
              if(value){
                if(value && value == doc){ udoc = true; }
                if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
                if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
              }else{
                udoc = true;
              }
            }
          }
        }else{
          if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
            if(value){
              var vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
              if(value && value == vak){ udoc = true; }
              if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
              if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
            }else{
              udoc = true;
            }
          }
          if(_.valids.isPrimitive(doc)){
            if(value){
              if(value && value == doc){ udoc = true; }
              if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
              if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
            }else{
              udoc = true;
            }
          }
        }
        if(udoc){
          total += 1;
          soud.emit(doc);
        }
      }
    });

  });

  //format: (total)
  Quero.whereSchema('$limit',function(m,q,sx,sm){
    var data = q.key,sid = sx.in(), soud = sx.out(),count = 0;

    if(_.valids.not.isNumber(data)) return;

    sid.on(function(doc){
      if(count >= data) return sid.close();
      soud.emit(doc);
      count += 1;
    });

    sid.afterEvent('dataEnd',sx.$bind(sx.complete));

  });

  //format: { condition: , key:, value:, getter:  }
  //format: { condition: , key:, value:  }
  Quero.whereSchema('$filter',function(m,q,sx,sm){
    var data = q.key,
        condfn = data.condition,
        getter = data.getter,
        key = data.key,
        count = 0,
        value  = data.value;

    var clond,sid = sx.in(), soud = sx.out();

    sid.afterEvent('dataEnd',function(){
      if(count <= 0) sx.completeError(new Error('NonFound!'));
      else sx.complete({'op':'filter', record: count});
      soud.emitEvent.apply(soud,['dataEnd'].concat(_.enums.toArray(arguments)));
    });

    var udoc = false;
    sid.on(function(doc){
      if(_.valids.isObject(data)){
        if(condfn){
          if(condfn.call(data,doc)){
            if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
              if(value){
                var vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
                if(value && value == vak){ udoc = true; }
                if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
                if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
              }else{
                udoc = true;
              }
            }
            if(_.valids.isPrimitive(doc)){
              if(value){
                if(value && value == doc){ udoc = true; }
                if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
                if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
              }else{
                udoc = true;
              }
            }
          }
        }else{
          if(_.valids.isObject(doc) && _.valids.exists(key) && _.valids.contains(doc,key)){
            if(value){
              var vak = _.valids.isFunction(getter) ? getter.call(doc,key) : doc[key];
              if(value && value == vak){ udoc = true; }
              if(_.valids.isRegExp(value) && value.test(vak)){ udoc = true; }
              if(_.valids.isFunction(value) && value.call(data,doc,vak)){ udoc = true; }
            }else{
              udoc = true;
            }
          }
          if(_.valids.isPrimitive(doc)){
            if(value){
              if(value && value == doc){ udoc = true; }
              if(_.valids.isRegExp(value) && value.test(doc)){ udoc = true; }
              if(_.valids.isFunction(value) && value.call(data,doc)){ udoc = true; }
            }else{
              udoc = true;
            }
          }
        }

        if(udoc){
          soud.emit(doc);
          count += 1;
        }
      }
    });

  });

  //format: { condition: , bykey: or byvalue:, getter:  }
  //format: { condition: , byKey: or byValue:  }
  Quero.whereSchema('$sort',function(m,q,sx,sm){

  });

  Quero.whereSchema('$byCount',function(m,q,sx,sm){
    var total = q.key.total, data = [];

    sx.complete(true);
    sx.then(function(){

      var pushOut = function(){
        var cur = _.enums.nthRest(data,0,total);
        sx.out().emit(cur);
        sx.out().emitEvent('dataEnd',true);
      };

      sx.in().on(function(d){
        if(total && data.length >= total) pushOut();
        data.push(d);
      });

    });

  });

  Quero.whereSchema('$byMs',function(m,q,sx,sm){
    var ms = q.key.ms, data = [];

    sx.complete(true);

    sx.then(function(){

      sx.in().on(function(d){
        data.push(d);
      });

      var pushOut = function(){
        var cur = data; data = [];
        sx.out().emit(cur);
        sx.out().emitEvent('dataEnd',true);
      };

      var time = setInterval(function(){
        return pushOut();
      },ms)

      sx.onError(function(){
        time.cancel();
      });

    });


  });

  Quero.whereSchema('$index',function(m,q,sx,sm){
    var mode = this.get(m), ind = {};

    if(_.valids.isString(q.key)){ ind.index = q.key; };
    if(_.valids.isObject(q.key)){ ind = q.key; }
    if(_.valids.isList(q.key)){ ind = {}; ind.index = q.key };
    if(_.valids.not.isString(q.key) && _.valids.not.isList(q.key) && _.valids.not.isObject(q.key)) return;
    ind.options = q.key.options;

    this.index = ind;

    sx.loopStream();
    sx.complete({'op': 'index',state:'true'});

  });

  return Quero;
}());
