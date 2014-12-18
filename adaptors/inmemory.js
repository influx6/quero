(function(){

    var _ = require('stackq'), live = require('../quero.js');

    live.registerProvider('inMemory',live.Connection.extends({
      init: function(c){
        this.$super(c);
        this.index = { index: '_id' };
        this.querieFilters = {
          match: function(map,val,name){
            if(_.valids.containsKey(map,name)){
              if(_.valids.isRegExp(val)) return val.test(map[name]);
              if(_.valids.isFunction(val)) return val.call(null,map[name]);
              return val == map[name];
            }
          },
        };
      },
      up: function(){
        this.$super(function(){
          this.db = _.Storage.make('internal storage');
        });
      },
      down: function(){
        this.$super(function(){
          this.db.clear();
        });
      },
      get: function(q){
        _.Asserted(_.valids.isString(q),'only a string is allowed');
        if(!this.state()) return;
        if(!this.db.has(q)) this.db.add(q,_.Storage.make(q));
        var kod = this.db.get(q);
        this.events.emit('get',kod);
        return kod;
      },
      drop: function(q){
        _.Asserted(_.valids.isString(q),'only a string is allowed');
        if(!this.state() || !this.db.has(q)) return;
        var kod = this.db.get(q);
        this.events.emit('down',true);
        kod.clear();
        return null;
      },
      internalOp: function(q){},
      registerQueries: function(){


        this.queryStream.where('$find',function(m,q,sx,sm){

          var index = this.index, by = index ? index.index : null;
          var map = this.get(m), query = q.key;

          if(map.isEmpty()){
            sx.out().emitEvent('dataEnd',true);
            sx.complete({ 'records':0, with: query });
            return;
          };

          if(_.valids.isPrimitive(query)){
            if(!map.has(query)) return sx.complete({op: 'destroy', with: query, records: 0});
              // return sx.completeError(new Error(_.Util.String(' ',query,'does not exists')));

            sx.out.emit(map.get(query));
            sx.out.emitEvent('dataEnd',true);
            sx.complete({ 'records':1, with: query });
            return;
          }

          if(_.valids.isObject(query)){

            if(_.enums.length(query) <= 0){

              _.enums.each(map.registry,function(e,i,o,fx){
                sx.out().emit(e);
                return fx(null);
              },function(_,err){
                sx.complete(true);
              });

            }else{

              var pick = _.enums.pickMatchBy(query,this.querieFilters.match)(function(m,fn){
                sx.out().emit(m);
                return fn(null);
              },function(){
                sx.complete(true);
              });

              pick(map.registry);

            }
          }

          sx.then(function(){
            sx.out().emitEvent('dataEnd',true);
            sx.out().complete(true);
          });

        });

        this.queryStream.where('$findOne',function(m,q,sx,sm){

          var index = this.index, by = index ? null : index.index;
          var map = this.get(m), query = q.key;

          if(map.isEmpty()){
            sx.out.emitEvent('dataEnd',true);
            sx.complete({ 'records':0, with: query });
            return;
          };

          if(_.valids.isPrimitive(query)){
            if(!map.has(query)) return sx.complete({op: 'destroy', with: query, records: 0});
              // return sx.completeError(new Error(_.Util.String(' ',query,'does not exists')));

            sx.out.emit(map.get(query));
            sx.out.emitEvent('dataEnd',true);
            sx.complete({ 'records':1, with: query });
            return;
          }

          if(_.valids.isObject(query)){

            if(_.enums.length(query) <= 0){

              var ix = _.enums.each(map.registry,function(e,i,o,fx){
                sx.out().emit(e);
                return fx(true);
              },function(_,err){
                sx.complete(true);
              });

            }else{

              var pick = _.enums.pickMatchBy(query,this.querieFilters.match)(function(m,fn){
                sx.out().emit(m);
                return;
              },function(){
                sx.complete(true);
              });

              pick(map.registry);

            }
          }

          sx.then(function(){
            sx.out().emitEvent('dataEnd',true);
            sx.out().complete(true);
          });

        });

        this.queryStream.where('$drop',function(m,q,sx,sm){
          var map = this.get(m), total = map.size();
          map.clear();
          sx.complete({'op':'drop', state: true, records: total});
        });

        this.queryStream.where('$save',function(m,q,sx,sm){
          var sid = sx.in(), sout = sx.out, total = 0;
          var index = this.index, by = index ? index.index : null;
          var map = this.get(m), query = q.key;

          sid.on(function(m){

            if(_.valids.not.isObject(m)) return;

            if(_.valids.contains(m,by)){
              if(map.has(by)) map.overwrite(m[by],m);
              else map.add(m[by],m);
            }else{
              var index = _.Util.guid();
              m[by] = index;
              map.add(index,m);
            }
            total += 1;
          });

          sid.onEvent('dataEnd',function(){
            sout.emitEvent('dataEnd',{'op':save, total: total });
            sx.complete(true);
          });

        });

        this.queryStream.where('$saveAndStream',function(m,q,sx,sm){
            sx.loopStream();

          var sid = sx.in(), sout = sx.out, total = 0;
          var index = this.index, by = index ? index.index : null;
          var map = this.get(m), query = q.key;

          sid.on(function(m){

            if(_.valids.not.isObject(m)) return;

            if(_.valids.contains(m,by)){
              if(map.has(by)) map.overwrite(m[by],m);
              else map.add(m[by],m);
            }else{
              var index = _.Util.guid();
              m[by] = index;
              map.add(index,m);
            }
            total += 1;
          });

          sid.onEvent('dataEnd',function(){
            sout.emitEvent('dataEnd',{'op':save, total: total });
            sx.complete(true);
          });

        });

        this.queryStream.where('$stream',function(m,q,sx,sm){

          if(_.valids.not.isFunction(q.key)) return;

          var map = this.get(m), query = q.key, count = 0;
            _.enums.each(map.registry,function(e,i,o,fx){
              if(query.call(this,e,i)){
                count += 1;
                sx.out().emit(e);
              }
              return fx(null);
            },function(_,err){
              sx.complete({'op':'streamOne', records: count});
            },this);
        });

        this.queryStream.where('$streamOne',function(m,q,sx,sm){
          if(_.valids.not.isFunction(q.key)) return;

          var map = this.get(m), query = q.key;
            _.enums.each(map.registry,function(e,i,o,fx){
              if(query.call(this,e,i)){
                sx.out().emit(e);
                return fx(true);
              }
              return fx(null);
            },function(_,err){
              sx.complete({'op':'streamOne', records: 1});
            },this);
        });

        this.queryStream.where('$destroy',function(m,q,sx,sm){
          // sx.loopStream();

          var map = this.get(m), count = 0;
          var index = this.index, by = index ? index.index : null;

          sx.in().on(this.$bind(function(doc){
            map.remove(doc[by]);
            if(!map.has(doc[by])){
             this.changes.emit({ 'i': 'd', record: doc });
             count += 1;
            }
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
            map.remove(doc[by]);
            if(!map.has(doc[by])){
             this.changes.emit({ 'i': 'd', record: doc });
             count += 1;
            }
          }));

          sx.in().onEvent('dataEnd',function(){
            sx.complete({ 'records': count, with: q.key, op: 'destroy' });
          });
        });

      }
    }));

})();
