(function(){

    var _ = require('stackq'), live = require('../quero.js');

    live.registerProvider('Listdb',live.Connection.extends({
      init: function(map){
        this.$super(map);
        this.index = { 'index': '_id'};
        this.collection = _.Storage.make('bufferCollection');
      },
      registerQueries: function(){

        this.queryStream.where('save',function(m,q,sx,sm){

          var sid = sx.in(), sout = sx.out(), count = 0, cold = this.get(m);

          sid.on(this.$bind(function(d){
            cold.concat(d.toJSON ? d.toJSON() : d);
            count += 1;
            this.changes.emit({'i': 's', records: count, doc: d });
          }));

          sid.onEvent('dataEnd',function(){
            sx.complete({'op':'save', records: count});
          });

        });
      },
      up: function(){
        return this.$super(function(){});
      },
      down: function(){
        return this.$super(function(){});
      },
      get: function(id){
        if(!this.collection.has(id)) this.collection.add(id,_.Buffer([]));
        return this.collection.get(id);
      },
      drop: function(id){
        this.collection.remove(id);
      }
    }));

})();
