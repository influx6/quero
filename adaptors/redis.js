(function(){

    var _ = require('stackq'), live = require('../quero.js'), redis = require('redis');

    live.registerProvider('redisdb',live.Connection.extends({
      init: function(c){
        this.$super(c);
        this.dbUrl = this.dbMeta.db;
        this.port = this.dbMeta.port;
        this.username = this.dbMeta.username;
        this.password = this.dbMeta.password;

      },
      up: function(){
        this.$super(function(){
          this.db = redis.createClient(this.port,this.dbUrl);
        });
      },
      down: function(){
        this.$super(function(){
          this.db.close();
        });
      },
      registerQueries: function(){

        this.queryStream.where('$find',function(m,q,sx,sm){

        });

        this.queryStream.where('$findOne',function(m,q,sx,sm){

        });

        this.queryStream.where('$stream',function(m,q,sx,sm){

        });

        this.queryStream.where('$streamOne',function(m,q,sx,sm){

        });

        this.queryStream.where('$destroy',function(m,q,sx,sm){

        });

        this.queryStream.where('$destroyAndStream',function(m,q,sx,sm){
          sx.loopStream();
        });

        this.queryStream.where('$save',function(m,q,sx,sm){

        });

        this.queryStream.where('$saveAndStream',function(m,q,sx,sm){
            sx.loopStream();
        });


      },

    }));

})();
