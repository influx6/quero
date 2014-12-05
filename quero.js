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
      _.Asserted(_.valids.contains(meta,'db'),'a "db" must exists within the config object');
      this.dbMeta = meta;
      this.events = _.EventStream.make();
      this.queryStream = _.QueryStream(this);
      this.silentEvents = meta.silentEvents || false;
      this.collections = _.Storage.make('connector-collections');

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

      this.$secure('queryJob',function(q){
        if(!_.Query.isQuery(q)) return;
        this.emit('query',q);
        return this.queryStream.query(q);
      });

    },
    registerQueries: function(){
      /* define the queries types for the querystream*/
    },
    up: function(){
      /* create the connections needed*/
      _.Asserted(false,"must redefine function 'up' in subclass");
    },
    down: function(){
      /* drop/end the connections needed*/
      _.Asserted(false,"must redefine function 'down' in subclass");
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

        this.events.hookProxy(this);
        var master = null;

        this.$secure('syncQuery',function(q){
          if(!_.Query.isQuery(q)) return;
          this.after('up',this.$bind(function(){
            this.connection.queryJob(q);
            this.qstreamCache.push(q);
          }));
          this.after('up:fail',this.$bind(function(){
            this.emit('querySync:fail',q);
          }));
        });

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
      model: function(title){
        _.Asserted(_.valids.isString(title),'a string title/name for model must be supplied');
        title = title.toLowerCase();
        if(!this.schemas.has(title)) return;
        var qr = _.Query(title,this.schemas.get(title));
        qr.notify.add(this.$bind(this.syncQuery));
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
      providers: Connections.make(),
      Connections: Connections,
      Connection: Connection,
      MessageFormat: MessageFormat,
      QueryFormat: QueryFormat,
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

  return Quero;
}());
