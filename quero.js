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

      var state = _.Switch();
      this.state = function(){ return state.isOn(); };
      this.__switchOn = function(){ return state.on(); };
      this.__switchOff = function(){ return state.off(); };

      this.events.hookProxy(this);

      this.events.events('queryError');
      this.events.events('up');
      this.events.events('down');
      this.events.events('drop');
      this.events.events('get');
      this.events.events('create');
      this.events.events('internalOp');
      this.events.events('query');
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
        case 'create':
          res = this.create(q);
          break;
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
          this.emit('queryError',res);
          break;
      };
      return res;
    },
    queryJob: function(q){
      var res = QueryFormat(q,q.query,null,false);
      res.message = MessageFormat('query','query recieved');
      return this.queryStream.query(q);
    },
    get: function(q){
      _.Asserted(false,"must redefine function 'get' in subclass");
    },
    drop: function(q){
      _.Asserted(false,"must redefine function 'drop' in subclass");
    },
    create: function(q){
      _.Asserted(false,"must redefine function 'create' in subclass");
    },
    internalOp: function(){
      _.Asserted(false,"must redefine function 'internalOp' in subclass");
    },
  });

  var Quero = _.Configurable.extends({
      init: function(meta){
        _.Asserted(_.valids.contains(meta,'adaptor'),'adaptor used not specified in config, eg. { adaptor: mongodb,.. }');
        _.Asserted(_.valids.contains(meta,'db'),'db path used not specified in config, eg. { adaptor:redis,db: localhost:3009/db,.. }');
        this.$super();
        var master = null;
        this.uuid = _.Util.guid();
        this.events = _.EventStream.make();
        this.schemas = _.Storage.make('quero-schemas');
        this.slaves = _.Storage.make('quero-slaves');

        this.config({
          silentEvents: false,
        });

        this.config(meta);

        this.connection = null;

        this.silentEvents = function(fn){
          if(!this.getConfigAttr(silentEvents)) return;
          fn.call(this);
        };

        this.master = function(con){
          if(this.hasMaster()) return;
          master = con;
          master.on('querySync',this.syncQuery);
          master.slave(this);
        };

        this.hasMaster = function(){
          return master != null;
        };

        this.releaseMaster = function(){
          if(!this.hasMaster()) return;
          master.off('querySync',this.syncQuery);
          master = null;
        };

        this.events.events('up');
        this.events.events('down');
        this.events.events('querySync');
        this.events.hookProxy(this);
      },
      schema: function(title,map,meta){

      },
      slave: function(type,meta){
        if(_.valids.isString(type) && Quero.hasProvider(type)){
          var con =  Quero.createProvider(type,meta);
          this.emit('newSlave',con,meta);
        }
        if(!Connection.isType(type)) return;
        if(this.slaves.has(type) || type.hasMaster()) return;
        type.master(this);
        this.slaves.add(type);
        this.emit('newSlave',type,meta);
      },
      syncQuery: function(q){

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
