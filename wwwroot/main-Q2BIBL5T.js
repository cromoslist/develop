var iI=Object.defineProperty,rI=Object.defineProperties;var oI=Object.getOwnPropertyDescriptors;var Eb=Object.getOwnPropertySymbols;var aI=Object.prototype.hasOwnProperty,sI=Object.prototype.propertyIsEnumerable;var Db=(t,n,e)=>n in t?iI(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,C=(t,n)=>{for(var e in n||={})aI.call(n,e)&&Db(t,e,n[e]);if(Eb)for(var e of Eb(n))sI.call(n,e)&&Db(t,e,n[e]);return t},ne=(t,n)=>rI(t,oI(n));var sn=null,ud=!1,pp=1,lI=null,Dt=Symbol("SIGNAL");function ce(t){let n=sn;return sn=t,n}function md(){return sn}var Br={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Hr(t){if(ud)throw new Error("");if(sn===null)return;sn.consumerOnSignalRead(t);let n=sn.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=sn.recomputing;if(i&&(e=n!==void 0?n.nextProducer:sn.producers,e!==void 0&&e.producer===t)){sn.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===sn&&(!i||dI(r,sn)))return;let o=Ta(sn),a={producer:t,consumer:sn,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};sn.producersTail=a,n!==void 0?n.nextProducer=a:sn.producers=a,o&&Tb(t,a)}function Mb(){pp++}function To(t){if(!(Ta(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===pp)){if(!t.producerMustRecompute(t)&&!Sa(t)){Ia(t);return}t.producerRecomputeValue(t),Ia(t)}}function hp(t){if(t.consumers===void 0)return;let n=ud;ud=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||cI(i)}}finally{ud=n}}function gp(){return sn?.consumerAllowSignalWrites!==!1}function cI(t){t.dirty=!0,hp(t),t.consumerMarkedDirty?.(t)}function Ia(t){t.dirty=!1,t.lastCleanEpoch=pp}function ur(t){return t&&Ib(t),ce(t)}function Ib(t){t.producersTail=void 0,t.recomputing=!0}function Ur(t,n){ce(n),t&&Sb(t)}function Sb(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Ta(t))do e=_p(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Sa(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(To(e),i!==e.version))return!0}return!1}function zr(t){if(Ta(t)){let n=t.producers;for(;n!==void 0;)n=_p(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Tb(t,n){let e=t.consumersTail,i=Ta(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)Tb(r.producer,r)}function _p(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Ta(n)){let o=n.producers;for(;o!==void 0;)o=_p(o)}return e}function Ta(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function nl(t){lI?.(t)}function dI(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function il(t,n){return Object.is(t,n)}function rl(t,n){let e=Object.create(uI);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(To(e),Hr(e),e.value===zi)throw e.error;return e.value};return i[Dt]=e,nl(e),i}var Io=Symbol("UNSET"),So=Symbol("COMPUTING"),zi=Symbol("ERRORED"),uI=ne(C({},Br),{value:Io,dirty:!0,error:null,equal:il,kind:"computed",producerMustRecompute(t){return t.value===Io||t.value===So},producerRecomputeValue(t){if(t.value===So)throw new Error("");let n=t.value;t.value=So;let e=ur(t),i,r=!1;try{i=t.computation(),ce(null),r=n!==Io&&n!==zi&&i!==zi&&t.equal(n,i)}catch(o){i=zi,t.error=o}finally{Ur(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function mI(){throw new Error}var Ab=mI;function kb(t){Ab(t)}function vp(t){Ab=t}var fI=null;function bp(t,n){let e=Object.create(ol);e.value=t,n!==void 0&&(e.equal=n);let i=()=>Ob(e);return i[Dt]=e,nl(e),[i,a=>Ao(e,a),a=>fd(e,a)]}function Ob(t){return Hr(t),t.value}function Ao(t,n){gp()||kb(t),t.equal(t.value,n)||(t.value=n,pI(t))}function fd(t,n){gp()||kb(t),Ao(t,n(t.value))}var ol=ne(C({},Br),{equal:il,value:void 0,kind:"signal"});function pI(t){t.version++,Mb(),hp(t),fI?.(t)}var yp=ne(C({},Br),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Cp(t){if(t.dirty=!1,t.version>0&&!Sa(t))return;t.version++;let n=ur(t);try{t.cleanup(),t.fn()}finally{Ur(t,n)}}function be(t){return typeof t=="function"}function Aa(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var pd=Aa(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function ko(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var de=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(be(i))try{i()}catch(o){n=o instanceof pd?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Rb(o)}catch(a){n=n??[],a instanceof pd?n=[...n,...a.errors]:n.push(a)}}if(n)throw new pd(n)}}add(n){var e;if(n&&n!==this)if(this.closed)Rb(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&ko(e,n)}remove(n){let{_finalizers:e}=this;e&&ko(e,n),n instanceof t&&n._removeParent(this)}};de.EMPTY=(()=>{let t=new de;return t.closed=!0,t})();var xp=de.EMPTY;function hd(t){return t instanceof de||t&&"closed"in t&&be(t.remove)&&be(t.add)&&be(t.unsubscribe)}function Rb(t){be(t)?t():t.unsubscribe()}var di={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ka={setTimeout(t,n,...e){let{delegate:i}=ka;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=ka;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function gd(t){ka.setTimeout(()=>{let{onUnhandledError:n}=di;if(n)n(t);else throw t})}function Oo(){}var Nb=wp("C",void 0,void 0);function Fb(t){return wp("E",void 0,t)}function Pb(t){return wp("N",t,void 0)}function wp(t,n,e){return{kind:t,value:n,error:e}}var Ro=null;function Oa(t){if(di.useDeprecatedSynchronousErrorHandling){let n=!Ro;if(n&&(Ro={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Ro;if(Ro=null,e)throw i}}else t()}function Lb(t){di.useDeprecatedSynchronousErrorHandling&&Ro&&(Ro.errorThrown=!0,Ro.error=t)}var No=class extends de{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,hd(n)&&n.add(this)):this.destination=_I}static create(n,e,i){return new mr(n,e,i)}next(n){this.isStopped?Dp(Pb(n),this):this._next(n)}error(n){this.isStopped?Dp(Fb(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Dp(Nb,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},hI=Function.prototype.bind;function Ep(t,n){return hI.call(t,n)}var Mp=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){_d(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){_d(i)}else _d(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){_d(e)}}},mr=class extends No{constructor(n,e,i){super();let r;if(be(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&di.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Ep(n.next,o),error:n.error&&Ep(n.error,o),complete:n.complete&&Ep(n.complete,o)}):r=n}this.destination=new Mp(r)}};function _d(t){di.useDeprecatedSynchronousErrorHandling?Lb(t):gd(t)}function gI(t){throw t}function Dp(t,n){let{onStoppedNotification:e}=di;e&&ka.setTimeout(()=>e(t,n))}var _I={closed:!0,next:Oo,error:gI,complete:Oo};var Ra=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Ln(t){return t}function Ip(...t){return Sp(t)}function Sp(t){return t.length===0?Ln:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ae=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=bI(e)?e:new mr(e,i,r);return Oa(()=>{let{operator:a,source:s}=this;o.add(a?a.call(o,s):s?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=jb(i),new i((r,o)=>{let a=new mr({next:s=>{try{e(s)}catch(l){o(l),a.unsubscribe()}},error:o,complete:r});this.subscribe(a)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[Ra](){return this}pipe(...e){return Sp(e)(this)}toPromise(e){return e=jb(e),new e((i,r)=>{let o;this.subscribe(a=>o=a,a=>r(a),()=>i(o))})}}return t.create=n=>new t(n),t})();function jb(t){var n;return(n=t??di.Promise)!==null&&n!==void 0?n:Promise}function vI(t){return t&&be(t.next)&&be(t.error)&&be(t.complete)}function bI(t){return t&&t instanceof No||vI(t)&&hd(t)}function yI(t){return be(t?.lift)}function he(t){return n=>{if(yI(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function ye(t,n,e,i,r){return new Tp(t,n,e,i,r)}var Tp=class extends No{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=r?function(s){try{r(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var Vb=Aa(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var E=(()=>{class t extends ae{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new vd(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new Vb}next(e){Oa(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Oa(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Oa(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?xp:(this.currentObservers=null,o.push(e),new de(()=>{this.currentObservers=null,ko(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new ae;return e.source=this,e}}return t.create=(n,e)=>new vd(n,e),t})(),vd=class extends E{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:xp}};var Vt=class extends E{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var al={now(){return(al.delegate||Date).now()},delegate:void 0};var $r=class extends E{constructor(n=1/0,e=1/0,i=al){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let l=1;l<i.length&&i[l]<=a;l+=2)s=l;s&&i.splice(0,s+1)}}};var bd=class extends de{constructor(n,e){super()}schedule(n,e=0){return this}};var sl={setInterval(t,n,...e){let{delegate:i}=sl;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=sl;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var yd=class extends bd{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return sl.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&sl.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,ko(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Na=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Na.now=al.now;var Cd=class extends Na{constructor(n,e=Na.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Fo=new Cd(yd),Bb=Fo;var ot=new ae(t=>t.complete());function xd(t){return t&&be(t.schedule)}function Ap(t){return t[t.length-1]}function wd(t){return be(Ap(t))?t.pop():void 0}function $i(t){return xd(Ap(t))?t.pop():void 0}function Hb(t,n){return typeof Ap(t)=="number"?t.pop():n}function zb(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(f){try{c(i.next(f))}catch(g){a(g)}}function l(f){try{c(i.throw(f))}catch(g){a(g)}}function c(f){f.done?o(f.value):r(f.value).then(s,l)}c((i=i.apply(t,n||[])).next())})}function Ub(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Po(t){return this instanceof Po?(this.v=t,this):new Po(t)}function $b(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(b){return function(w){return Promise.resolve(w).then(b,g)}}function s(b,w){i[b]&&(r[b]=function(z){return new Promise(function(q,X){o.push([b,z,q,X])>1||l(b,z)})},w&&(r[b]=w(r[b])))}function l(b,w){try{c(i[b](w))}catch(z){_(o[0][3],z)}}function c(b){b.value instanceof Po?Promise.resolve(b.value.v).then(f,g):_(o[0][2],b)}function f(b){l("next",b)}function g(b){l("throw",b)}function _(b,w){b(w),o.shift(),o.length&&l(o[0][0],o[0][1])}}function qb(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof Ub=="function"?Ub(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),r(s,l,a.done,a.value)})}}function r(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var Ed=t=>t&&typeof t.length=="number"&&typeof t!="function";function Dd(t){return be(t?.then)}function Md(t){return be(t[Ra])}function Id(t){return Symbol.asyncIterator&&be(t?.[Symbol.asyncIterator])}function Sd(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function CI(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Td=CI();function Ad(t){return be(t?.[Td])}function kd(t){return $b(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Po(e.read());if(r)return yield Po(void 0);yield yield Po(i)}}finally{e.releaseLock()}})}function Od(t){return be(t?.getReader)}function Ue(t){if(t instanceof ae)return t;if(t!=null){if(Md(t))return xI(t);if(Ed(t))return wI(t);if(Dd(t))return EI(t);if(Id(t))return Gb(t);if(Ad(t))return DI(t);if(Od(t))return MI(t)}throw Sd(t)}function xI(t){return new ae(n=>{let e=t[Ra]();if(be(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function wI(t){return new ae(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function EI(t){return new ae(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,gd)})}function DI(t){return new ae(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Gb(t){return new ae(n=>{II(t,n).catch(e=>n.error(e))})}function MI(t){return Gb(kd(t))}function II(t,n){var e,i,r,o;return zb(this,void 0,void 0,function*(){try{for(e=qb(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Mn(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Rd(t,n=0){return he((e,i)=>{e.subscribe(ye(i,r=>Mn(i,t,()=>i.next(r),n),()=>Mn(i,t,()=>i.complete(),n),r=>Mn(i,t,()=>i.error(r),n)))})}function Nd(t,n=0){return he((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function Wb(t,n){return Ue(t).pipe(Nd(n),Rd(n))}function Yb(t,n){return Ue(t).pipe(Nd(n),Rd(n))}function Zb(t,n){return new ae(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function Qb(t,n){return new ae(e=>{let i;return Mn(e,n,()=>{i=t[Td](),Mn(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>be(i?.return)&&i.return()})}function Fd(t,n){if(!t)throw new Error("Iterable cannot be null");return new ae(e=>{Mn(e,n,()=>{let i=t[Symbol.asyncIterator]();Mn(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function Kb(t,n){return Fd(kd(t),n)}function Xb(t,n){if(t!=null){if(Md(t))return Wb(t,n);if(Ed(t))return Zb(t,n);if(Dd(t))return Yb(t,n);if(Id(t))return Fd(t,n);if(Ad(t))return Qb(t,n);if(Od(t))return Kb(t,n)}throw Sd(t)}function Qe(t,n){return n?Xb(t,n):Ue(t)}function Q(...t){let n=$i(t);return Qe(t,n)}function ll(t,n){let e=be(t)?t:()=>t,i=r=>r.error(e());return new ae(n?r=>n.schedule(i,0,r):i)}function cl(t){return!!t&&(t instanceof ae||be(t.lift)&&be(t.subscribe))}var Lo=Aa(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Jb(t){return t instanceof Date&&!isNaN(t)}function $(t,n){return he((e,i)=>{let r=0;e.subscribe(ye(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:SI}=Array;function TI(t,n){return SI(n)?t(...n):t(n)}function Pd(t){return $(n=>TI(t,n))}var{isArray:AI}=Array,{getPrototypeOf:kI,prototype:OI,keys:RI}=Object;function Ld(t){if(t.length===1){let n=t[0];if(AI(n))return{args:n,keys:null};if(NI(n)){let e=RI(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function NI(t){return t&&typeof t=="object"&&kI(t)===OI}function jd(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function dl(...t){let n=$i(t),e=wd(t),{args:i,keys:r}=Ld(t);if(i.length===0)return Qe([],n);let o=new ae(FI(i,n,r?a=>jd(r,a):Ln));return e?o.pipe(Pd(e)):o}function FI(t,n,e=Ln){return i=>{ey(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let l=0;l<r;l++)ey(n,()=>{let c=Qe(t[l],n),f=!1;c.subscribe(ye(i,g=>{o[l]=g,f||(f=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function ey(t,n,e){t?Mn(e,t,n):n()}function ty(t,n,e,i,r,o,a,s){let l=[],c=0,f=0,g=!1,_=()=>{g&&!l.length&&!c&&n.complete()},b=z=>c<i?w(z):l.push(z),w=z=>{o&&n.next(z),c++;let q=!1;Ue(e(z,f++)).subscribe(ye(n,X=>{r?.(X),o?b(X):n.next(X)},()=>{q=!0},void 0,()=>{if(q)try{for(c--;l.length&&c<i;){let X=l.shift();a?Mn(n,a,()=>w(X)):w(X)}_()}catch(X){n.error(X)}}))};return t.subscribe(ye(n,b,()=>{g=!0,_()})),()=>{s?.()}}function Gt(t,n,e=1/0){return be(n)?Gt((i,r)=>$((o,a)=>n(i,o,r,a))(Ue(t(i,r))),e):(typeof n=="number"&&(e=n),he((i,r)=>ty(i,r,t,e)))}function qr(t=1/0){return Gt(Ln,t)}function ny(){return qr(1)}function qi(...t){return ny()(Qe(t,$i(t)))}function jn(t){return new ae(n=>{Ue(t()).subscribe(n)})}function jo(...t){let n=wd(t),{args:e,keys:i}=Ld(t),r=new ae(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let f=0;f<a;f++){let g=!1;Ue(e[f]).subscribe(ye(o,_=>{g||(g=!0,c--),s[f]=_},()=>l--,void 0,()=>{(!l||!g)&&(c||o.next(i?jd(i,s):s),o.complete())}))}});return n?r.pipe(Pd(n)):r}function Vd(t=0,n,e=Bb){let i=-1;return n!=null&&(xd(n)?e=n:i=n),new ae(r=>{let o=Jb(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function at(...t){let n=$i(t),e=Hb(t,1/0),i=t;return i.length?i.length===1?Ue(i[0]):qr(e)(Qe(i,n)):ot}function oe(t,n){return he((e,i)=>{let r=0;e.subscribe(ye(i,o=>t.call(n,o,r++)&&i.next(o)))})}function iy(t){return he((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(ye(e,c=>{i=!0,r=c,o||Ue(t(c)).subscribe(o=ye(e,s,l))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function Bd(t,n=Fo){return iy(()=>Vd(t,n))}function Gr(t){return he((n,e)=>{let i=null,r=!1,o;i=n.subscribe(ye(e,void 0,void 0,a=>{o=Ue(t(a,Gr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Vo(t,n){return be(n)?Gt(t,n,1):Gt(t,1)}function ui(t,n=Fo){return he((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=a+t,f=n.now();if(f<c){r=this.schedule(void 0,c-f),i.add(r);return}s()}e.subscribe(ye(i,c=>{o=c,a=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function ry(t){return he((n,e)=>{let i=!1;n.subscribe(ye(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function ke(t){return t<=0?()=>ot:he((n,e)=>{let i=0;n.subscribe(ye(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function oy(){return he((t,n)=>{t.subscribe(ye(n,Oo))})}function Fa(t){return $(()=>t)}function kp(t,n){return n?e=>qi(n.pipe(ke(1),oy()),e.pipe(kp(t))):Gt((e,i)=>Ue(t(e,i)).pipe(ke(1),Fa(e)))}function Op(t,n=Fo){let e=Vd(t,n);return kp(()=>e)}function Pa(t,n=Ln){return t=t??PI,he((e,i)=>{let r,o=!0;e.subscribe(ye(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function PI(t,n){return t===n}function ay(t=LI){return he((n,e)=>{let i=!1;n.subscribe(ye(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function LI(){return new Lo}function Bo(t){return he((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function fr(t,n){let e=arguments.length>=2;return i=>i.pipe(t?oe((r,o)=>t(r,o,i)):Ln,ke(1),e?ry(n):ay(()=>new Lo))}function Hd(t){return t<=0?()=>ot:he((n,e)=>{let i=[];n.subscribe(ye(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Ud(){return he((t,n)=>{let e,i=!1;t.subscribe(ye(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function ul(t={}){let{connector:n=()=>new E,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,l,c=0,f=!1,g=!1,_=()=>{s?.unsubscribe(),s=void 0},b=()=>{_(),a=l=void 0,f=g=!1},w=()=>{let z=a;b(),z?.unsubscribe()};return he((z,q)=>{c++,!g&&!f&&_();let X=l=l??n();q.add(()=>{c--,c===0&&!g&&!f&&(s=Rp(w,r))}),X.subscribe(q),!a&&c>0&&(a=new mr({next:Ze=>X.next(Ze),error:Ze=>{g=!0,_(),s=Rp(b,e,Ze),X.error(Ze)},complete:()=>{f=!0,_(),s=Rp(b,i),X.complete()}}),Ue(z).subscribe(a))})(o)}}function Rp(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new mr({next:()=>{i.unsubscribe(),t()}});return Ue(n(...e)).subscribe(i)}function zd(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,ul({connector:()=>new $r(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function ml(t){return oe((n,e)=>t<=e)}function Ie(...t){let n=$i(t);return he((e,i)=>{(n?qi(t,e,n):qi(t,e)).subscribe(i)})}function We(t,n){return he((e,i)=>{let r=null,o=0,a=!1,s=()=>a&&!r&&i.complete();e.subscribe(ye(i,l=>{r?.unsubscribe();let c=0,f=o++;Ue(t(l,f)).subscribe(r=ye(i,g=>i.next(n?n(l,g,f,c++):g),()=>{r=null,s()}))},()=>{a=!0,s()}))})}function De(t){return he((n,e)=>{Ue(t).subscribe(ye(e,()=>e.complete(),Oo)),!e.closed&&n.subscribe(e)})}function Np(t,n=!1){return he((e,i)=>{let r=0;e.subscribe(ye(i,o=>{let a=t(o,r++);(a||n)&&i.next(o),!a&&i.complete()}))})}function st(t,n,e){let i=be(t)||n||e?{next:t,error:n,complete:e}:t;return i?he((r,o)=>{var a;(a=i.subscribe)===null||a===void 0||a.call(i);let s=!0;r.subscribe(ye(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;s=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;s=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;s&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Ln}var Fp;function $d(){return Fp}function Gi(t){let n=Fp;return Fp=t,n}var sy=Symbol("NotFound");function La(t){return t===sy||t?.name==="\u0275NotFound"}function Pp(t,n,e){let i=Object.create(jI);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(To(i),Hr(i),i.value===zi)throw i.error;return i.value};return o[Dt]=i,nl(i),o}function ly(t,n){To(t),Ao(t,n),Ia(t)}function cy(t,n){if(To(t),t.value===zi)throw t.error;fd(t,n),Ia(t)}var jI=ne(C({},Br),{value:Io,dirty:!0,error:null,equal:il,kind:"linkedSignal",producerMustRecompute(t){return t.value===Io||t.value===So},producerRecomputeValue(t){if(t.value===So)throw new Error("");let n=t.value;t.value=So;let e=ur(t),i,r=!1;try{let o=t.source(),a=n!==Io&&n!==zi,s=a?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,s),t.sourceValue=o,ce(null),r=a&&i!==zi&&t.equal(n,i)}catch(o){i=zi,t.error=o}finally{Ur(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function dy(t){let n=ce(null);try{return t()}finally{ce(n)}}var Xd="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",O=class extends Error{code;constructor(n,e){super(hr(n,e)),this.code=n}};function VI(t){return`NG0${Math.abs(t)}`}function hr(t,n){return`${VI(t)}${n?": "+n:""}`}var Kr=globalThis;function Pe(t){for(let n in t)if(t[n]===Pe)return n;throw Error("")}function hy(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function yl(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(yl).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Jd(t,n){return t?n?`${t} ${n}`:t:n||""}var BI=Pe({__forward_ref__:Pe});function Yt(t){return t.__forward_ref__=Yt,t}function Bt(t){return Zp(t)?t():t}function Zp(t){return typeof t=="function"&&t.hasOwnProperty(BI)&&t.__forward_ref__===Yt}function x(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function L(t){return{providers:t.providers||[],imports:t.imports||[]}}function Cl(t){return HI(t,eu)}function Qp(t){return Cl(t)!==null}function HI(t,n){return t.hasOwnProperty(n)&&t[n]||null}function UI(t){let n=t?.[eu]??null;return n||null}function jp(t){return t&&t.hasOwnProperty(Wd)?t[Wd]:null}var eu=Pe({\u0275prov:Pe}),Wd=Pe({\u0275inj:Pe}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=x({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Kp(t){return t&&!!t.\u0275providers}var xl=Pe({\u0275cmp:Pe}),wl=Pe({\u0275dir:Pe}),Xp=Pe({\u0275pipe:Pe}),Jp=Pe({\u0275mod:Pe}),pl=Pe({\u0275fac:Pe}),qo=Pe({__NG_ELEMENT_ID__:Pe}),uy=Pe({__NG_ENV_ID__:Pe});function eh(t){return nu(t,"@NgModule"),t[Jp]||null}function gr(t){return nu(t,"@Component"),t[xl]||null}function tu(t){return nu(t,"@Directive"),t[wl]||null}function gy(t){return nu(t,"@Pipe"),t[Xp]||null}function nu(t,n){if(t==null)throw new O(-919,!1)}function _r(t){return typeof t=="string"?t:t==null?"":String(t)}var _y=Pe({ngErrorCode:Pe}),zI=Pe({ngErrorMessage:Pe}),$I=Pe({ngTokenPath:Pe});function th(t,n){return vy("",-200,n)}function iu(t,n){throw new O(-201,!1)}function vy(t,n,e){let i=new O(n,t);return i[_y]=n,i[zI]=t,e&&(i[$I]=e),i}function qI(t){return t[_y]}var Vp;function by(){return Vp}function _n(t){let n=Vp;return Vp=t,n}function nh(t,n,e){let i=Cl(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;iu(t,"")}var GI={},Ho=GI,WI="__NG_DI_FLAG__",Bp=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Uo(e)||0;try{return this.injector.get(n,i&8?null:Ho,i)}catch(r){if(La(r))return r;throw r}}};function YI(t,n=0){let e=$d();if(e===void 0)throw new O(-203,!1);if(e===null)return nh(t,void 0,n);{let i=ZI(n),r=e.retrieve(t,i);if(La(r)){if(i.optional)return null;throw r}return r}}function K(t,n=0){return(by()||YI)(Bt(t),n)}function d(t,n){return K(t,Uo(n))}function Uo(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function ZI(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Hp(t){let n=[];for(let e=0;e<t.length;e++){let i=Bt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new O(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],l=QI(s);typeof l=="number"?l===-1?r=s.token:o|=l:r=s}n.push(K(r,o))}else n.push(K(i))}return n}function QI(t){return t[WI]}function Yr(t,n){let e=t.hasOwnProperty(pl);return e?t[pl]:null}function yy(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function Cy(t){return t.flat(Number.POSITIVE_INFINITY)}function ru(t,n){t.forEach(e=>Array.isArray(e)?ru(e,n):n(e))}function ih(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function El(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function xy(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function wy(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function ou(t,n,e){let i=Va(t,n);return i>=0?t[i|1]=e:(i=~i,wy(t,i,n,e)),i}function au(t,n){let e=Va(t,n);if(e>=0)return t[e|1]}function Va(t,n){return KI(t,n,1)}function KI(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var mi={},ln=[],Xr=new y(""),rh=new y("",-1),oh=new y(""),hl=class{get(n,e=Ho){if(e===Ho){let r=vy("",-201);throw r.name="\u0275NotFound",r}return e}};function vr(t){return{\u0275providers:t}}function Ey(t){return vr([{provide:Xr,multi:!0,useValue:t}])}function Dy(...t){return{\u0275providers:ah(!0,t),\u0275fromNgModule:!0}}function ah(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return ru(n,a=>{let s=a;Yd(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&My(r,o),e}function My(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];sh(r,o=>{n(o,i)})}}function Yd(t,n,e,i){if(t=Bt(t),!t)return!1;let r=null,o=jp(t),a=!o&&gr(t);if(!o&&!a){let l=t.ngModule;if(o=jp(l),o)r=l;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)Yd(c,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let c;ru(o.imports,f=>{Yd(f,n,e,i)&&(c||=[],c.push(f))}),c!==void 0&&My(c,n)}if(!s){let c=Yr(r)||(()=>new r);n({provide:r,useFactory:c,deps:ln},r),n({provide:oh,useValue:r,multi:!0},r),n({provide:Xr,useValue:()=>K(r),multi:!0},r)}let l=o.providers;if(l!=null&&!s){let c=t;sh(l,f=>{n(f,c)})}}else return!1;return r!==t&&t.providers!==void 0}function sh(t,n){for(let e of t)Kp(e)&&(e=e.\u0275providers),Array.isArray(e)?sh(e,n):n(e)}var XI=Pe({provide:String,useValue:Pe});function Iy(t){return t!==null&&typeof t=="object"&&XI in t}function JI(t){return!!(t&&t.useExisting)}function eS(t){return!!(t&&t.useFactory)}function zo(t){return typeof t=="function"}function Sy(t){return!!t.useClass}var Dl=new y(""),qd={},my={},Lp;function Ba(){return Lp===void 0&&(Lp=new hl),Lp}var Ne=class{},$o=class extends Ne{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,zp(n,a=>this.processProvider(a)),this.records.set(rh,ja(void 0,this)),r.has("environment")&&this.records.set(Ne,ja(void 0,this));let o=this.records.get(Dl);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(oh,ln,{self:!0}))}retrieve(n,e){let i=Uo(e)||0;try{return this.get(n,Ho,i)}catch(r){if(La(r))return r;throw r}}destroy(){fl(this),this._destroyed=!0;let n=ce(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),ce(n)}}onDestroy(n){return fl(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){fl(this);let e=Gi(this),i=_n(void 0),r;try{return n()}finally{Gi(e),_n(i)}}get(n,e=Ho,i){if(fl(this),n.hasOwnProperty(uy))return n[uy](this);let r=Uo(i),o,a=Gi(this),s=_n(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let f=oS(n)&&Cl(n);f&&this.injectableDefInScope(f)?c=ja(Up(n),qd):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?Ba():this.parent;return e=r&8&&e===Ho?null:e,l.get(n,e)}catch(l){let c=qI(l);throw c===-200||c===-201?new O(c,null):l}finally{_n(s),Gi(a)}}resolveInjectorInitializers(){let n=ce(null),e=Gi(this),i=_n(void 0),r;try{let o=this.get(Xr,ln,{self:!0});for(let a of o)a()}finally{Gi(e),_n(i),ce(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Bt(n);let e=zo(n)?n:Bt(n&&n.provide),i=nS(n);if(!zo(n)&&n.multi===!0){let r=this.records.get(e);r||(r=ja(void 0,qd,!0),r.factory=()=>Hp(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=ce(null);try{if(e.value===my)throw th("");return e.value===qd&&(e.value=my,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&rS(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{ce(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Bt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Up(t){let n=Cl(t),e=n!==null?n.factory:Yr(t);if(e!==null)return e;if(t instanceof y)throw new O(-204,!1);if(t instanceof Function)return tS(t);throw new O(-204,!1)}function tS(t){if(t.length>0)throw new O(-204,!1);let e=UI(t);return e!==null?()=>e.factory(t):()=>new t}function nS(t){if(Iy(t))return ja(void 0,t.useValue);{let n=lh(t);return ja(n,qd)}}function lh(t,n,e){let i;if(zo(t)){let r=Bt(t);return Yr(r)||Up(r)}else if(Iy(t))i=()=>Bt(t.useValue);else if(eS(t))i=()=>t.useFactory(...Hp(t.deps||[]));else if(JI(t))i=(r,o)=>K(Bt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Bt(t&&(t.useClass||t.provide));if(iS(t))i=()=>new r(...Hp(t.deps));else return Yr(r)||Up(r)}return i}function fl(t){if(t.destroyed)throw new O(-205,!1)}function ja(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function iS(t){return!!t.deps}function rS(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function oS(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function zp(t,n){for(let e of t)Array.isArray(e)?zp(e,n):e&&Kp(e)?zp(e.\u0275providers,n):n(e)}function Zt(t,n){let e;t instanceof $o?(fl(t),e=t):e=new Bp(t);let i,r=Gi(e),o=_n(void 0);try{return n()}finally{Gi(r),_n(o)}}function Ty(){return by()!==void 0||$d()!=null}var fi=0,ie=1,ue=2,Wt=3,Xn=4,bn=5,Ha=6,Ua=7,rn=8,Jr=9,pi=10,$e=11,za=12,ch=13,Go=14,In=15,eo=16,Wo=17,Yi=18,to=19,dh=20,pr=21,su=22,Zr=23,Vn=24,Yo=25,$a=26,lt=27,Ay=1;var no=7,Ml=8,Zo=9,cn=10;function br(t){return Array.isArray(t)&&typeof t[Ay]=="object"}function hi(t){return Array.isArray(t)&&t[Ay]===!0}function uh(t){return(t.flags&4)!==0}function yr(t){return t.componentOffset>-1}function qa(t){return(t.flags&1)===1}function gi(t){return!!t.template}function Ga(t){return(t[ue]&512)!==0}function Qo(t){return(t[ue]&256)===256}var Re=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(Re||{}),Gd,gl="svg",mh="math";function ky(){return Gd||(Gd={},Wr(Re.HTML,void 0,[["iframe",["srcdoc"]],["*",["innerHTML","outerHTML"]]]),Wr(Re.STYLE,void 0,[["*",["style"]]]),Wr(Re.URL,void 0,[["*",["formAction"]],["area",["href"]],["a",["href","xlink:href"]],["form",["action"]],["img",["src"]],["video",["src"]]]),Wr(Re.URL,mh,[["*",["href","xlink:href"]],["annotation",["href","xlink:href"]],["annotation-xml",["href","xlink:href"]],["maction",["href","xlink:href"]],["malignmark",["href","xlink:href"]],["math",["href","xlink:href"]],["mroot",["href","xlink:href"]],["msqrt",["href","xlink:href"]],["merror",["href","xlink:href"]],["mfrac",["href","xlink:href"]],["mglyph",["href","xlink:href"]],["msub",["href","xlink:href"]],["msup",["href","xlink:href"]],["msubsup",["href","xlink:href"]],["mmultiscripts",["href","xlink:href"]],["mprescripts",["href","xlink:href"]],["mi",["href","xlink:href"]],["mn",["href","xlink:href"]],["mo",["href","xlink:href"]],["mpadded",["href","xlink:href"]],["mphantom",["href","xlink:href"]],["mrow",["href","xlink:href"]],["ms",["href","xlink:href"]],["mspace",["href","xlink:href"]],["mstyle",["href","xlink:href"]],["mtable",["href","xlink:href"]],["mtd",["href","xlink:href"]],["mtr",["href","xlink:href"]],["mtext",["href","xlink:href"]],["mover",["href","xlink:href"]],["munder",["href","xlink:href"]],["munderover",["href","xlink:href"]],["semantics",["href","xlink:href"]],["none",["href","xlink:href"]]]),Wr(Re.RESOURCE_URL,void 0,[["base",["href"]],["embed",["src"]],["frame",["src"]],["iframe",["src"]],["link",["href"]],["object",["codebase","data"]]]),Wr(Re.URL,gl,[["a",["href","xlink:href"]]]),Wr(Re.ATTRIBUTE_NO_BINDING,gl,[["animate",["attributeName","values","to","from"]],["set",["to","attributeName"]],["animateMotion",["attributeName"]],["animateTransform",["attributeName"]]]),Wr(Re.ATTRIBUTE_NO_BINDING,void 0,[["unknown",["attributeName","values","to","from","sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority"]],["iframe",["sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority"]]])),Gd}function Wr(t,n,e){for(let[i,r]of e){let o=n&&i!=="unknown"?`:${n}:${i}`:i;o=o.toLowerCase();for(let a of r)Gd[`${o}|${a.toLowerCase()}`]=t}}function Oy(t,n,e){let i=ky(),r=t.toLowerCase(),o=n.toLowerCase(),a=e&&r!=="*"&&r!=="unknown"?i[`:${e}:${r}|${o}`]:void 0,s=e?i[`:${e}:*|${o}`]:void 0;return a??s??i[`${r}|${o}`]??i[`*|${o}`]??Re.NONE}function Jn(t){for(;Array.isArray(t);)t=t[fi];return t}function fh(t,n){return Jn(n[t])}function ei(t,n){return Jn(n[t.index])}function lu(t,n){return t.data[n]}function ph(t,n){return t[n]}function hh(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function ti(t,n){let e=n[t];return br(e)?e:e[fi]}function Ry(t){return(t[ue]&4)===4}function cu(t){return(t[ue]&128)===128}function Ny(t){return hi(t[Wt])}function _i(t,n){return n==null?null:t[n]}function gh(t){t[Wo]=0}function _h(t){t[ue]&1024||(t[ue]|=1024,cu(t)&&Ko(t))}function Fy(t,n){for(;t>0;)n=n[Go],t--;return n}function Il(t){return!!(t[ue]&9216||t[Vn]?.dirty)}function du(t){t[pi].changeDetectionScheduler?.notify(8),t[ue]&64&&(t[ue]|=1024),Il(t)&&Ko(t)}function Ko(t){t[pi].changeDetectionScheduler?.notify(0);let n=Qr(t);for(;n!==null&&!(n[ue]&8192||(n[ue]|=8192,!cu(n)));)n=Qr(n)}function vh(t,n){if(Qo(t))throw new O(911,!1);t[pr]===null&&(t[pr]=[]),t[pr].push(n)}function Py(t,n){if(t[pr]===null)return;let e=t[pr].indexOf(n);e!==-1&&t[pr].splice(e,1)}function Qr(t){let n=t[Wt];return hi(n)?n[Wt]:n}function bh(t){return t[Ua]??=[]}function yh(t){return t.cleanup??=[]}function Ly(t,n,e,i){let r=bh(n);r.push(e),t.firstCreatePass&&yh(t).push(i,r.length-1)}var xe={lFrame:Yy(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var $p=!1;function jy(){return xe.lFrame.elementDepthCount}function Vy(){xe.lFrame.elementDepthCount++}function Ch(){xe.lFrame.elementDepthCount--}function uu(){return xe.bindingsEnabled}function xh(){return xe.skipHydrationRootTNode!==null}function wh(t){return xe.skipHydrationRootTNode===t}function Eh(){xe.skipHydrationRootTNode=null}function re(){return xe.lFrame.lView}function Ke(){return xe.lFrame.tView}function se(t){return xe.lFrame.contextLView=t,t[rn]}function le(t){return xe.lFrame.contextLView=null,t}function Ht(){let t=Dh();for(;t!==null&&t.type===64;)t=t.parent;return t}function Dh(){return xe.lFrame.currentTNode}function By(){let t=xe.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Wa(t,n){let e=xe.lFrame;e.currentTNode=t,e.isParent=n}function Mh(){return xe.lFrame.isParent}function Ih(){xe.lFrame.isParent=!1}function Hy(){return xe.lFrame.contextLView}function Sh(){return $p}function _l(t){let n=$p;return $p=t,n}function Sl(){let t=xe.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function Th(){return xe.lFrame.bindingIndex}function Uy(t){return xe.lFrame.bindingIndex=t}function Xo(){return xe.lFrame.bindingIndex++}function Tl(t){let n=xe.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function zy(){return xe.lFrame.inI18n}function $y(t,n){let e=xe.lFrame;e.bindingIndex=e.bindingRootIndex=t,mu(n)}function qy(){return xe.lFrame.currentDirectiveIndex}function mu(t){xe.lFrame.currentDirectiveIndex=t}function Gy(t){let n=xe.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function fu(){return xe.lFrame.currentQueryIndex}function Al(t){xe.lFrame.currentQueryIndex=t}function aS(t){let n=t[ie];return n.type===2?n.declTNode:n.type===1?t[bn]:null}function Ah(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=aS(o),r===null||(o=o[Go],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=xe.lFrame=Wy();return i.currentTNode=n,i.lView=t,!0}function pu(t){let n=Wy(),e=t[ie];xe.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Wy(){let t=xe.lFrame,n=t===null?null:t.child;return n===null?Yy(t):n}function Yy(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function Zy(){let t=xe.lFrame;return xe.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var kh=Zy;function hu(){let t=Zy();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Qy(t){return(xe.lFrame.contextLView=Fy(t,xe.lFrame.contextLView))[rn]}function vi(){return xe.lFrame.selectedIndex}function io(t){xe.lFrame.selectedIndex=t}function Ya(){let t=xe.lFrame;return lu(t.tView,t.selectedIndex)}function ni(){xe.lFrame.currentNamespace=gl}function kl(){sS()}function sS(){xe.lFrame.currentNamespace=null}function Oh(){return xe.lFrame.currentNamespace}var Ky=!0;function gu(){return Ky}function Ol(t){Ky=t}function qp(t,n=null,e=null,i){let r=Rh(t,n,e,i);return r.resolveInjectorInitializers(),r}function Rh(t,n=null,e=null,i,r=new Set){let o=[e||ln,Dy(t)],a;return new $o(o,n||Ba(),a||null,r)}var G=class t{static THROW_IF_NOT_FOUND=Ho;static NULL=new hl;static create(n,e){if(Array.isArray(n))return qp({name:""},e,n,"");{let i=n.name??"";return qp({name:i},n.parent,n.providers,i)}}static \u0275prov=x({token:t,providedIn:"any",factory:()=>K(rh)});static __NG_ELEMENT_ID__=-1},W=new y(""),yn=(()=>{class t{static __NG_ELEMENT_ID__=lS;static __NG_ENV_ID__=e=>e}return t})(),Zd=class extends yn{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Qo(this._lView)}onDestroy(n){let e=this._lView;return vh(e,n),()=>Py(e,n)}};function lS(){return new Zd(re())}var Xy=!1,Jy=new y(""),Cr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Vt(!1);debugTaskTracker=d(Jy,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ae(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=x({token:t,providedIn:"root",factory:()=>new t})}return t})(),Gp=class extends E{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Ty()&&(this.destroyRef=d(yn,{optional:!0})??void 0,this.pendingTasks=d(Cr,{optional:!0})??void 0)}emit(n){let e=ce(null);try{super.next(n)}finally{ce(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof de&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},F=Gp;function Qd(...t){}function Nh(t){let n,e;function i(){t=Qd;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function e0(t){return queueMicrotask(()=>t()),()=>{t=Qd}}var Fh="isAngularZone",vl=Fh+"_ID",cS=0,P=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new F(!1);onMicrotaskEmpty=new F(!1);onStable=new F(!1);onError=new F(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=Xy}=n;if(typeof Zone>"u")throw new O(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,mS(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Fh)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new O(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new O(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,dS,Qd,Qd);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},dS={};function Ph(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function uS(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Nh(()=>{t.callbackScheduled=!1,Wp(t),t.isCheckStableRunning=!0,Ph(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Wp(t)}function mS(t){let n=()=>{uS(t)},e=cS++;t._inner=t._inner.fork({name:"angular",properties:{[Fh]:!0,[vl]:e,[vl+e]:!0},onInvokeTask:(i,r,o,a,s,l)=>{if(fS(l))return i.invokeTask(o,a,s,l);try{return fy(t),i.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),py(t)}},onInvoke:(i,r,o,a,s,l,c)=>{try{return fy(t),i.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!pS(l)&&n(),py(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,Wp(t),Ph(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function Wp(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function fy(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function py(t){t._nesting--,Ph(t)}var bl=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new F;onMicrotaskEmpty=new F;onStable=new F;onError=new F;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function fS(t){return t0(t,"__ignore_ng_zone__")}function pS(t){return t0(t,"__scheduler_tick__")}function t0(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var vn=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Sn=new y("",{factory:()=>{let t=d(P),n=d(Ne),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(vn),e.handleError(i))})}}}),n0={provide:Xr,useValue:()=>{let t=d(vn,{optional:!0})},multi:!0},hS=new y("",{factory:()=>{let t=d(W).defaultView;if(!t)return;let n=d(Sn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(yn).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function Lh(){return vr([Ey(()=>{d(hS)})])}function ee(t,n){let[e,i,r]=bp(t,n?.equal),o=e,a=o[Dt];return o.set=i,o.update=r,o.asReadonly=_u.bind(o),o}function _u(){let t=this[Dt];if(t.readonlyFn===void 0){let n=()=>this();n[Dt]=t,t.readonlyFn=n}return t.readonlyFn}var Za=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=gS}return t})();function gS(){return new Za(re(),Ht())}var Wi=class{},Rl=new y("",{factory:()=>!0});var jh=new y(""),Nl=(()=>{class t{internalPendingTasks=d(Cr);scheduler=d(Wi);errorHandler=d(Sn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=x({token:t,providedIn:"root",factory:()=>new t})}return t})(),vu=(()=>{class t{static \u0275prov=x({token:t,providedIn:"root",factory:()=>new Yp})}return t})(),Yp=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Kd=class{[Dt];constructor(n){this[Dt]=n}destroy(){this[Dt].destroy()}};function xr(t,n){let e=n?.injector??d(G),i=n?.manualCleanup!==!0?e.get(yn):null,r,o=e.get(Za,null,{optional:!0}),a=e.get(Wi);return o!==null?(r=bS(o.view,a,t),i instanceof Zd&&i._lView===o.view&&(i=null)):r=yS(t,e.get(vu),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Kd(r)}var i0=ne(C({},yp),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=_l(!1);try{Cp(this)}finally{_l(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=ce(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],ce(t)}}}),_S=ne(C({},i0),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(zr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),vS=ne(C({},i0),{consumerMarkedDirty(){this.view[ue]|=8192,Ko(this.view),this.notifier.notify(13)},destroy(){if(zr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Zr]?.delete(this)}});function bS(t,n,e){let i=Object.create(vS);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=r0(i,e),t[Zr]??=new Set,t[Zr].add(i),i.consumerMarkedDirty(i),i}function yS(t,n,e){let i=Object.create(_S);return i.fn=r0(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function r0(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function zl(t){return{toString:t}.toString()}function IS(t){return typeof t=="function"}function V0(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Su=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},je=(()=>{let t=()=>B0;return t.ngInherit=!0,t})();function B0(t){return t.type.prototype.ngOnChanges&&(t.setInput=TS),SS}function SS(){let t=U0(this),n=t?.current;if(n){let e=t.previous;if(e===mi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function TS(t,n,e,i,r){let o=this.declaredInputs[i],a=U0(t)||AS(t,{previous:mi,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new Su(c&&c.currentValue,e,l===mi),V0(t,n,r,e)}var H0="__ngSimpleChanges__";function U0(t){return t[H0]||null}function AS(t,n){return t[H0]=n}var o0=[];var Le=function(t,n=null,e){for(let i=0;i<o0.length;i++){let r=o0[i];r(t,n,e)}},Ae=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Ae||{});function kS(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=B0(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function z0(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:f}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),f!=null&&(t.destroyHooks??=[]).push(e,f)}}function wu(t,n,e){$0(t,n,3,e)}function Eu(t,n,e,i){(t[ue]&3)===e&&$0(t,n,e,i)}function Vh(t,n){let e=t[ue];(e&3)===n&&(e&=16383,e+=1,t[ue]=e)}function $0(t,n,e,i){let r=i!==void 0?t[Wo]&65535:0,o=i??-1,a=n.length-1,s=0;for(let l=r;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],i!=null&&s>=i)break}else n[l]<0&&(t[Wo]+=65536),(s<o||o==-1)&&(OS(t,e,n,l),t[Wo]=(t[Wo]&4294901760)+l+2),l++}function a0(t,n){Le(Ae.LifecycleHookStart,t,n);let e=ce(null);try{n.call(t)}finally{ce(e),Le(Ae.LifecycleHookEnd,t,n)}}function OS(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[ue]>>14<t[Wo]>>16&&(t[ue]&3)===n&&(t[ue]+=16384,a0(s,o)):a0(s,o)}var Ka=-1,ea=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function RS(t){return(t.flags&8)!==0}function NS(t){return(t.flags&16)!==0}function FS(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];PS(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function q0(t){return t===3||t===4||t===6}function PS(t){return t.charCodeAt(0)===64}function Xa(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?s0(t,e,r,null,n[++i]):s0(t,e,r,null,null))}}return t}function s0(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function G0(t){return t!==Ka}function Tu(t){return t&32767}function LS(t){return t>>16}function Au(t,n){let e=LS(t),i=n;for(;e>0;)i=i[Go],e--;return i}var Yh=!0;function ku(t){let n=Yh;return Yh=t,n}var jS=256,W0=jS-1,Y0=5,VS=0,Zi={};function BS(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(qo)&&(i=e[qo]),i==null&&(i=e[qo]=VS++);let r=i&W0,o=1<<r;n.data[t+(r>>Y0)]|=o}function Ou(t,n){let e=Z0(t,n);if(e!==-1)return e;let i=n[ie];i.firstCreatePass&&(t.injectorIndex=n.length,Bh(i.data,t),Bh(n,null),Bh(i.blueprint,null));let r=Mg(t,n),o=t.injectorIndex;if(G0(r)){let a=Tu(r),s=Au(r,n),l=s[ie].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=r,o}function Bh(t,n){t.push(0,0,0,0,0,0,0,0,n)}function Z0(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Mg(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=eC(r),i===null)return Ka;if(e++,r=r[Go],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Ka}function Zh(t,n,e){BS(t,n,e)}function HS(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(q0(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function Q0(t,n,e){if(e&8||t!==void 0)return t;iu(n,"NodeInjector")}function K0(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[Jr],o=_n(void 0);try{return r?r.get(n,i,e&8):nh(n,i,e&8)}finally{_n(o)}}return Q0(i,n,e)}function X0(t,n,e,i=0,r){if(t!==null){if(n[ue]&2048&&!(i&2)){let a=qS(t,n,e,i,Zi);if(a!==Zi)return a}let o=J0(t,n,e,i,Zi);if(o!==Zi)return o}return K0(n,e,i,r)}function J0(t,n,e,i,r){let o=zS(e);if(typeof o=="function"){if(!Ah(n,t,i))return i&1?Q0(r,e,i):K0(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))iu(e);else return a}finally{kh()}}else if(typeof o=="number"){let a=null,s=Z0(t,n),l=Ka,c=i&1?n[In][bn]:null;for((s===-1||i&4)&&(l=s===-1?Mg(t,n):n[s+8],l===Ka||!c0(i,!1)?s=-1:(a=n[ie],s=Tu(l),n=Au(l,n)));s!==-1;){let f=n[ie];if(l0(o,s,f.data)){let g=US(s,n,e,a,i,c);if(g!==Zi)return g}l=n[s+8],l!==Ka&&c0(i,n[ie].data[s+8]===c)&&l0(o,s,n)?(a=f,s=Tu(l),n=Au(l,n)):s=-1}}return r}function US(t,n,e,i,r,o){let a=n[ie],s=a.data[t+8],l=i==null?yr(s)&&Yh:i!=a&&(s.type&3)!==0,c=r&1&&o===s,f=Du(s,a,e,l,c);return f!==null?jl(n,a,f,s,r):Zi}function Du(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,f=o>>20,g=i?s:s+f,_=r?s+f:c;for(let b=g;b<_;b++){let w=a[b];if(b<l&&e===w||b>=l&&w.type===e)return b}if(r){let b=a[l];if(b&&gi(b)&&b.type===e)return l}return null}function jl(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof ea){let s=o;if(s.resolving)throw th("");let l=ku(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],f,g=s.injectImpl?_n(s.injectImpl):null,_=Ah(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&kS(e,a[e],n)}finally{g!==null&&_n(g),ku(l),s.resolving=!1,kh()}}return o}function zS(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(qo)?t[qo]:void 0;return typeof n=="number"?n>=0?n&W0:$S:n}function l0(t,n,e){let i=1<<t;return!!(e[n+(t>>Y0)]&i)}function c0(t,n){return!(t&2)&&!(t&1&&n)}var Jo=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return X0(this._tNode,this._lView,n,Uo(i),e)}};function $S(){return new Jo(Ht(),re())}function Ve(t){return zl(()=>{let n=t.prototype.constructor,e=n[pl]||Qh(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[pl]||Qh(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Qh(t){return Zp(t)?()=>{let n=Qh(Bt(t));return n&&n()}:Yr(t)}function qS(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[ue]&2048&&!Ga(a);){let s=J0(o,a,e,i|2,Zi);if(s!==Zi)return s;let l=o.parent;if(!l){let c=a[dh];if(c){let f=c.get(e,Zi,i&-5);if(f!==Zi)return f}l=eC(a),a=a[Go]}o=l}return r}function eC(t){let n=t[ie],e=n.type;return e===2?n.declTNode:e===1?t[bn]:null}function $l(t){return HS(Ht(),t)}function GS(){return ns(Ht(),re())}function ns(t,n){return new N(ei(t,n))}var N=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=GS}return t})();function tC(t){return t instanceof N?t.nativeElement:t}function WS(){return this._results[Symbol.iterator]()}var Bn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new E}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=Cy(n);(this._changesDetected=!yy(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=WS};function nC(t){return(t.flags&128)===128}var Ig=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(Ig||{}),iC=new Map,YS=0;function ZS(){return YS++}function QS(t){iC.set(t[to],t)}function Kh(t){iC.delete(t[to])}var d0="__ngContext__";function Ja(t,n){br(n)?(t[d0]=n[to],QS(n)):t[d0]=n}function rC(t){return aC(t[za])}function oC(t){return aC(t[Xn])}function aC(t){for(;t!==null&&!hi(t);)t=t[Xn];return t}var KS;function Sg(t){KS=t}var oo=new y("",{factory:()=>XS}),XS="ng";var $u=new y(""),ia=new y("",{providedIn:"platform",factory:()=>"unknown"}),ql=new y(""),ra=new y("",{factory:()=>d(W).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var sC=!1,lC=new y("",{factory:()=>sC});var u0=new WeakMap;function JS(t,n){if(t==null||typeof t!="object")return;let e=u0.get(t);e||(e=new WeakSet,u0.set(t,e)),e.add(n)}var eT=(t,n,e,i)=>{};function tT(t,n,e,i){eT(t,n,e,i)}function qu(t){return(t.flags&32)===32}var nT=()=>null;function cC(t,n,e=!1){return nT(t,n,e)}function dC(t,n){let e=t.contentQueries;if(e!==null){let i=ce(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];Al(o),s.contentQueries(2,n[a],a)}}}finally{ce(i)}}}function Xh(t,n,e){Al(0);let i=ce(null);try{n(t,e)}finally{ce(i)}}function Tg(t,n,e){if(uh(n)){let i=ce(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{ce(i)}}}var Ci=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Ci||{});var iT="svg",rT="math",oT={"http://www.w3.org/2000/svg":iT,"http://www.w3.org/1998/Math/MathML":rT},bu;function aT(){if(bu===void 0&&(bu=null,Kr.trustedTypes))try{bu=Kr.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return bu}function Gu(t){return aT()?.createHTML(t)||t}var yu;function sT(){if(yu===void 0&&(yu=null,Kr.trustedTypes))try{yu=Kr.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return yu}function m0(t){return sT()?.createScriptURL(t)||t}var wr=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Xd})`}},Jh=class extends wr{getTypeName(){return"HTML"}},eg=class extends wr{getTypeName(){return"Style"}},tg=class extends wr{getTypeName(){return"Script"}},ng=class extends wr{getTypeName(){return"URL"}},ig=class extends wr{getTypeName(){return"ResourceURL"}};function xi(t){return t instanceof wr?t.changingThisBreaksApplicationSecurity:t}function Er(t,n){let e=uC(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Xd})`)}return e===n}function uC(t){return t instanceof wr&&t.getTypeName()||null}function Ag(t){return new Jh(t)}function kg(t){return new eg(t)}function Og(t){return new tg(t)}function Rg(t){return new ng(t)}function Ng(t){return new ig(t)}function lT(t){let n=new og(t);return cT()?new rg(n):n}var rg=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Gu(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},og=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Gu(n),e}};function cT(){try{return!!new window.DOMParser().parseFromString(Gu(""),"text/html")}catch{return!1}}var dT=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Gl(t){return t=String(t),t.match(dT)?t:"unsafe:"+t}function Dr(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Wl(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var mC=Dr("area,br,col,hr,img,wbr"),fC=Dr("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),pC=Dr("rp,rt"),uT=Wl(pC,fC),mT=Wl(fC,Dr("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),fT=Wl(pC,Dr("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),f0=Wl(mC,mT,fT,uT),hC=Dr("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),pT=Dr("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),hT=Dr("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),gT=Wl(hC,pT,hT),_T=Dr("script,style,template"),ag=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=yT(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=bT(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=p0(n).toLowerCase();if(!f0.hasOwnProperty(e))return this.sanitizedSomething=!0,!_T.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!gT.hasOwnProperty(s)){this.sanitizedSomething=!0;continue}let l=o.value;hC[s]&&(l=Gl(l)),this.buf.push(" ",a,'="',h0(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=p0(n).toLowerCase();f0.hasOwnProperty(e)&&!mC.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(h0(n))}};function vT(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function bT(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw gC(n);return n}function yT(t){let n=t.firstChild;if(n&&vT(t,n))throw gC(n);return n}function p0(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function gC(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var CT=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,xT=/([^\#-~ |!])/g;function h0(t){return t.replace(/&/g,"&amp;").replace(CT,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(xT,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Cu;function Fg(t,n){let e=null;try{Cu=Cu||lT(t);let i=n?String(n):"";e=Cu.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Cu.getInertBodyElement(i)}while(i!==o);let s=new ag().sanitizeChildren(g0(e)||e);return Gu(s)}finally{if(e){let i=g0(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function g0(t){return"content"in t&&wT(t)?t.content:null}function wT(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var ET=/^>|^->|<!--|-->|--!>|<!-$/g,DT=/(<|>)/g,MT="\u200B$1\u200B";function IT(t){return t.replace(ET,n=>n.replace(DT,MT))}function ST(t,n){return t.createText(n)}function TT(t,n,e){t.setValue(n,e)}function AT(t,n){return t.createComment(IT(n))}function _C(t,n,e){return t.createElement(n,e)}function Ru(t,n,e,i,r){t.insertBefore(n,e,i,r)}function vC(t,n,e){t.appendChild(n,e)}function _0(t,n,e,i,r){i!==null?Ru(t,n,e,i,r):vC(t,n,e)}function kT(t,n,e,i){t.removeChild(null,n,e,i)}function OT(t,n,e){t.setAttribute(n,"style",e)}function RT(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function bC(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&FS(t,n,i),r!==null&&RT(t,n,r),o!==null&&OT(t,n,o)}function NT(t,n=!0){if(t[0]!=":")return[null,t];let e=t.indexOf(":",1);if(e===-1){if(n)throw new Error(`Unsupported format "${t}" expecting ":namespace:name"`);return[null,t]}return[t.slice(1,e),t.slice(e+1)]}function yC(t){let n=xC();return n?n.sanitize(Re.URL,t)||"":Er(t,"URL")?xi(t):Gl(_r(t))}function CC(t){let n=xC();if(n)return m0(n.sanitize(Re.RESOURCE_URL,t)||"");if(Er(t,"ResourceURL"))return m0(xi(t));throw new O(904,!1)}function FT(t,n){switch(PT(t,n)){case Re.RESOURCE_URL:return CC;case Re.URL:return yC;default:return null}}function Pg(t,n,e){return FT(n,e)?.(t)??t}function xC(){let t=re();return t&&t[pi].sanitizer}function PT(t,n){let[e,i]=LT(t);return Oy(i,n,e)}function LT(t){t=t.toLowerCase();let n=NT(t,!1);if(n[0])return n;let i=vi()===-1?null:Ya(),r=i?.namespace;if(t==="#host"&&i?.type===2){let o=ei(i,re());if(o.tagName&&(t=o.tagName.toLowerCase()),r==null){let a=o.namespaceURI;r=a&&oT[a]}}return[r,t]}function wC(t){return t instanceof Function?t():t}function jT(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var EC="ng-template";function VT(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&jT(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(Lg(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Lg(t){return t.type===4&&t.value!==EC}function BT(t,n,e){let i=t.type===4&&!e?EC:t.value;return n===i}function HT(t,n,e){let i=4,r=t.attrs,o=r!==null?$T(r):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!bi(i)&&!bi(l))return!1;if(a&&bi(l))continue;a=!1,i=l|i&1;continue}if(!a)if(i&4){if(i=2|i&1,l!==""&&!BT(t,l,e)||l===""&&n.length===1){if(bi(i))return!1;a=!0}}else if(i&8){if(r===null||!VT(t,r,l,e)){if(bi(i))return!1;a=!0}}else{let c=n[++s],f=UT(l,r,Lg(t),e);if(f===-1){if(bi(i))return!1;a=!0;continue}if(c!==""){let g;if(f>o?g="":g=r[f+1].toLowerCase(),i&2&&c!==g){if(bi(i))return!1;a=!0}}}}return bi(i)||a}function bi(t){return(t&1)===0}function UT(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return qT(n,t)}function DC(t,n,e=!1){for(let i=0;i<n.length;i++)if(HT(t,n[i],e))return!0;return!1}function zT(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function $T(t){for(let n=0;n<t.length;n++){let e=t[n];if(q0(e))return n}return t.length}function qT(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function GT(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function v0(t,n){return t?":not("+n.trim()+")":n}function WT(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!bi(a)&&(n+=v0(o,r),r=""),i=a,o=o||!bi(i);e++}return r!==""&&(n+=v0(o,r)),n}function YT(t){return t.map(WT).join(",")}function ZT(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!bi(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Qt={};function jg(t,n,e,i,r,o,a,s,l,c,f){let g=lt+i,_=g+r,b=QT(g,_),w=typeof c=="function"?c():c;return b[ie]={type:t,blueprint:b,template:e,queries:null,viewQuery:s,declTNode:n,data:b.slice().fill(null,g),bindingStartIndex:g,expandoStartIndex:_,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:w,incompleteFirstPass:!1,ssrId:f}}function QT(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Qt);return e}function KT(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=jg(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Vg(t,n,e,i,r,o,a,s,l,c,f){let g=n.blueprint.slice();return g[fi]=r,g[ue]=i|4|128|8|64|1024,(c!==null||t&&t[ue]&2048)&&(g[ue]|=2048),gh(g),g[Wt]=g[Go]=t,g[rn]=e,g[pi]=a||t&&t[pi],g[$e]=s||t&&t[$e],g[Jr]=l||t&&t[Jr]||null,g[bn]=o,g[to]=ZS(),g[Ha]=f,g[dh]=c,g[In]=n.type==2?t[In]:g,g}function XT(t,n,e){let i=ei(n,t),r=KT(e),o=t[pi].rendererFactory,a=Bg(t,Vg(t,r,null,MC(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function MC(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function IC(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function Bg(t,n){return t[za]?t[ch][Xn]=n:t[za]=n,t[ch]=n,n}function h(t=1){SC(Ke(),re(),vi()+t,!1)}function SC(t,n,e,i){if(!i)if((n[ue]&3)===3){let o=t.preOrderCheckHooks;o!==null&&wu(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Eu(n,o,0,e)}io(e)}var Wu=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Wu||{});function sg(t,n,e,i){let r=ce(null);try{let[o,a,s]=t.inputs[e],l=null;(a&Wu.SignalBased)!==0&&(l=n[o][Dt]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):V0(n,l,o,i)}finally{ce(r)}}var Qi=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Qi||{}),JT;function Hg(t,n){return JT(t,n)}var i5=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var lg=new WeakMap,Fl=new WeakSet;function eA(t,n){let e=lg.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let a=e[o],s=a.parentNode;a===n?(e.splice(o,1),Fl.add(a),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&a===r||s&&i&&s!==i)&&(e.splice(o,1),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),a.parentNode?.removeChild(a))}}function tA(t,n){let e=lg.get(t);e?e.includes(n)||e.push(n):lg.set(t,[n])}var es=new Set,Yu=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Yu||{}),wi=new y(""),b0=new Set;function oa(t){b0.has(t)||(b0.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Zu=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=x({token:t,providedIn:"root",factory:()=>new t})}return t})(),Ug=[0,1,2,3],zg=(()=>{class t{ngZone=d(P);scheduler=d(Wi);errorHandler=d(vn,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(wi,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Le(Ae.AfterRenderHooksStart),this.executing=!0;for(let i of Ug)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Le(Ae.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Yo]??=[]).push(e),Ko(i),i[ue]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Yu.AFTER_NEXT_RENDER,e):e()}static \u0275prov=x({token:t,providedIn:"root",factory:()=>new t})}return t})(),Vl=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Yo];n&&(this.view[Yo]=n.filter(e=>e!==this))}};function ft(t,n){let e=n?.injector??d(G);return oa("NgAfterNextRender"),iA(t,e,n,!0)}function nA(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function iA(t,n,e,i){let r=n.get(Zu);r.impl??=n.get(zg);let o=n.get(wi,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(yn):null,s=n.get(Za,null,{optional:!0}),l=new Vl(r.impl,nA(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(l),l}var rA=new y("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(Ne)})});function TC(t,n,e){let i=t.get(rA);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function oA(t,n){for(let[e,i]of n)TC(t,i.animateFns)}function y0(t,n,e,i){let r=t?.[$a]?.enter;n!==null&&r&&r.has(e.index)&&oA(i,r)}function Qa(t,n,e,i,r,o,a,s){if(r!=null){let l,c=!1;hi(r)?l=r:br(r)&&(c=!0,r=r[fi]);let f=Jn(r);t===0&&i!==null?(y0(s,i,o,e),a==null?vC(n,i,f):Ru(n,i,f,a||null,!0)):t===1&&i!==null?(y0(s,i,o,e),Ru(n,i,f,a||null,!0),eA(o,f)):t===2?(s?.[$a]?.leave?.has(o.index)&&tA(o,f),Fl.delete(f),C0(s,o,e,g=>{if(Fl.has(f)){Fl.delete(f);return}kT(n,f,c,g)})):t===3&&(Fl.delete(f),C0(s,o,e,()=>{n.destroyNode(f)})),l!=null&&gA(n,t,e,l,o,i,a)}}function aA(t,n){AC(t,n),n[fi]=null,n[bn]=null}function sA(t,n,e,i,r,o){i[fi]=r,i[bn]=n,Qu(t,i,e,1,r,o)}function AC(t,n){n[pi].changeDetectionScheduler?.notify(9),Qu(t,n,n[$e],2,null,null)}function lA(t){let n=t[za];if(!n)return Hh(t[ie],t);for(;n;){let e=null;if(br(n))e=n[za];else{let i=n[cn];i&&(e=i)}if(!e){for(;n&&!n[Xn]&&n!==t;)br(n)&&Hh(n[ie],n),n=n[Wt];n===null&&(n=t),br(n)&&Hh(n[ie],n),e=n&&n[Xn]}n=e}}function $g(t,n){let e=t[Zo],i=e.indexOf(n);e.splice(i,1)}function qg(t,n){if(Qo(n))return;let e=n[$e];e.destroyNode&&Qu(t,n,e,3,null,null),lA(n)}function Hh(t,n){if(Qo(n))return;let e=ce(null);try{n[ue]&=-129,n[ue]|=256,n[Vn]&&zr(n[Vn]),uA(t,n),dA(t,n),n[ie].type===1&&n[$e].destroy();let i=n[eo];if(i!==null&&hi(n[Wt])){i!==n[Wt]&&$g(i,n);let r=n[Yi];r!==null&&r.detachView(t)}Kh(n)}finally{ce(e)}}function C0(t,n,e,i){let r=t?.[$a];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&es.add(t[to]),TC(e,()=>{if(r.leave&&r.leave.has(n.index)){let a=r.leave.get(n.index),s=[];if(a){for(let l=0;l<a.animateFns.length;l++){let c=a.animateFns[l],{promise:f}=c();s.push(f)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(s),cA(t,i)}else t&&es.delete(t[to]),i(!1)},r)}function cA(t,n){let e=t[$a]?.running;if(e){e.then(()=>{t[$a].running=void 0,es.delete(t[to]),n(!0)});return}n(!1)}function dA(t,n){let e=t.cleanup,i=n[Ua];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[Ua]=null);let r=n[pr];if(r!==null){n[pr]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[Zr];if(o!==null){n[Zr]=null;for(let a of o)a.destroy()}}function uA(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof ea)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],l=o[a+1];Le(Ae.LifecycleHookStart,s,l);try{l.call(s)}finally{Le(Ae.LifecycleHookEnd,s,l)}}else{Le(Ae.LifecycleHookStart,r,o);try{o.call(r)}finally{Le(Ae.LifecycleHookEnd,r,o)}}}}}function kC(t,n,e){return mA(t,n.parent,e)}function mA(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[fi];if(yr(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Ci.None||r===Ci.Emulated)return null}return ei(i,e)}function OC(t,n,e){return pA(t,n,e)}function fA(t,n,e){return t.type&40?ei(t,e):null}var pA=fA,x0;function Gg(t,n,e,i){let r=kC(t,i,n),o=n[$e],a=i.parent||n[bn],s=OC(a,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)_0(o,r,e[l],s,!1);else _0(o,r,e,s,!1);x0!==void 0&&x0(o,i,n,e,r)}function Pl(t,n){if(n!==null){let e=n.type;if(e&3)return ei(n,t);if(e&4)return cg(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Pl(t,i);{let r=t[n.index];return hi(r)?cg(-1,r):Jn(r)}}else{if(e&128)return Pl(t,n.next);if(e&32)return Hg(n,t)()||Jn(t[n.index]);{let i=RC(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Qr(t[In]);return Pl(r,i)}else return Pl(t,n.next)}}}return null}function RC(t,n){if(n!==null){let i=t[In][bn],r=n.projection;return i.projection[r]}return null}function cg(t,n){let e=cn+t+1;if(e<n.length){let i=n[e],r=i[ie].firstChild;if(r!==null)return Pl(i,r)}return n[no]}function Wg(t,n,e,i,r,o,a){for(;e!=null;){let s=i[Jr];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(a&&n===0&&(l&&Ja(Jn(l),i),e.flags|=2),!qu(e))if(c&8)Wg(t,n,e.child,i,r,o,!1),Qa(n,t,s,r,l,e,o,i);else if(c&32){let f=Hg(e,i),g;for(;g=f();)Qa(n,t,s,r,g,e,o,i);Qa(n,t,s,r,l,e,o,i)}else c&16?NC(t,n,i,e,r,o):Qa(n,t,s,r,l,e,o,i);e=a?e.projectionNext:e.next}}function Qu(t,n,e,i,r,o){Wg(e,i,t.firstChild,n,r,o,!1)}function hA(t,n,e){let i=n[$e],r=kC(t,e,n),o=e.parent||n[bn],a=OC(o,e,n);NC(i,0,n,e,r,a)}function NC(t,n,e,i,r,o){let a=e[In],l=a[bn].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let f=l[c];Qa(n,t,e[Jr],r,f,i,o,e)}else{let c=l,f=a[Wt];nC(i)&&(c.flags|=128),Wg(t,n,c,f,r,o,!0)}}function gA(t,n,e,i,r,o,a){let s=i[no],l=Jn(i);s!==l&&Qa(n,t,e,o,s,r,a);for(let c=cn;c<i.length;c++){let f=i[c];Qu(f[ie],f,t,n,o,s)}}function _A(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Qi.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Qi.Important),t.setStyle(e,i,r,o))}}function FC(t,n,e,i,r){let o=vi(),a=i&2;try{io(-1),a&&n.length>lt&&SC(t,n,lt,!1);let s=a?Ae.TemplateUpdateStart:Ae.TemplateCreateStart;Le(s,r,e),e(i,r)}finally{io(o);let s=a?Ae.TemplateUpdateEnd:Ae.TemplateCreateEnd;Le(s,r,e)}}function Ku(t,n,e){wA(t,n,e),(e.flags&64)===64&&EA(t,n,e)}function Yl(t,n,e=ei){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function vA(t,n,e,i){let o=i.get(lC,sC)||e===Ci.ShadowDom||e===Ci.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);return bA(a),a}function bA(t){yA(t)}var yA=()=>null;function CA(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function PC(t,n,e,i,r,o){let a=n[ie];if(Kg(t,a,n,e,i)){yr(t)&&xA(n,t.index);return}t.type&3&&(e=CA(e)),LC(t,n,e,i,r,o)}function LC(t,n,e,i,r,o){if(t.type&3){let a=ei(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(a,e,i)}else t.type&12}function xA(t,n){let e=ti(n,t);e[ue]&16||(e[ue]|=64)}function wA(t,n,e){let i=e.directiveStart,r=e.directiveEnd;yr(e)&&XT(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Ou(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],l=jl(n,t,a,e);if(Ja(l,n),o!==null&&SA(n,a-i,l,s,e,o),gi(s)){let c=ti(e.index,n);c[rn]=jl(n,t,a,e)}}}function EA(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=qy();try{io(o);for(let s=i;s<r;s++){let l=t.data[s],c=n[s];mu(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&DA(l,c)}}finally{io(-1),mu(a)}}function DA(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function Yg(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];DC(n,o.selectors,!1)&&(i??=[],gi(o)?i.unshift(o):i.push(o))}return i}function MA(t,n,e,i,r,o){let a=ei(t,n);IA(n[$e],a,o,t.value,e,i,r)}function IA(t,n,e,i,r,o,a){if(o==null)t.removeAttribute(n,r,e);else{let s=a==null?_r(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function SA(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];sg(i,e,l,c)}}function Zg(t,n,e,i,r){let o=lt+e,a=n[ie],s=r(a,n,t,i,e);n[o]=s,Wa(t,!0);let l=t.type===2;return l?(bC(n[$e],s,t),(jy()===0||qa(t))&&Ja(s,n),Vy()):Ja(s,n),gu()&&(!l||!qu(t))&&Gg(a,n,s,t),t}function Qg(t){let n=t;return Mh()?Ih():(n=n.parent,Wa(n,!1)),n}function TA(t,n){let e=t[Jr];if(!e)return;let i;try{i=e.get(Sn,null)}catch{i=null}i?.(n)}function Kg(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],f=a[l+1],g=n.data[c];sg(g,e[c],f,r),s=!0}if(o)for(let l of o){let c=e[l],f=n.data[l];sg(f,c,i,r),s=!0}return s}function AA(t,n){let e=ti(n,t),i=e[ie];kA(i,e);let r=e[fi];r!==null&&e[Ha]===null&&(e[Ha]=cC(r,e[Jr])),Le(Ae.ComponentStart);try{Xg(i,e,e[rn])}finally{Le(Ae.ComponentEnd,e[rn])}}function kA(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function Xg(t,n,e){pu(n);try{let i=t.viewQuery;i!==null&&Xh(1,i,e);let r=t.template;r!==null&&FC(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Yi]?.finishViewCreation(t),t.staticContentQueries&&dC(t,n),t.staticViewQueries&&Xh(2,t.viewQuery,e);let o=t.components;o!==null&&OA(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[ue]&=-5,hu()}}function OA(t,n){for(let e=0;e<n.length;e++)AA(t,n[e])}function Jg(t,n,e,i){let r=ce(null);try{let o=n.tView,s=t[ue]&4096?4096:16,l=Vg(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[eo]=c;let f=t[Yi];return f!==null&&(l[Yi]=f.createEmbeddedView(o)),Xg(o,l,e),l}finally{ce(r)}}function Nu(t,n){return!n||n.firstChild===null||nC(t)}function Bl(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(Jn(o)),hi(o)&&jC(o,i);let a=e.type;if(a&8)Bl(t,n,e.child,i);else if(a&32){let s=Hg(e,n),l;for(;l=s();)i.push(l)}else if(a&16){let s=RC(n,e);if(Array.isArray(s))i.push(...s);else{let l=Qr(n[In]);Bl(l[ie],l,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function jC(t,n){for(let e=cn;e<t.length;e++){let i=t[e],r=i[ie].firstChild;r!==null&&Bl(i[ie],i,r,n)}t[no]!==t[fi]&&n.push(t[no])}function VC(t){if(t[Yo]!==null){for(let n of t[Yo])n.impl.addSequence(n);t[Yo].length=0}}var BC=[];function RA(t){return t[Vn]??NA(t)}function NA(t){let n=BC.pop()??Object.create(PA);return n.lView=t,n}function FA(t){t.lView[Vn]!==t&&(t.lView=null,BC.push(t))}var PA=ne(C({},Br),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Ko(t.lView)},consumerOnSignalRead(){this.lView[Vn]=this}});function LA(t){let n=t[Vn]??Object.create(jA);return n.lView=t,n}var jA=ne(C({},Br),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=Qr(t.lView);for(;n&&!HC(n[ie]);)n=Qr(n);n&&_h(n)},consumerOnSignalRead(){this.lView[Vn]=this}});function HC(t){return t.type!==2}function UC(t){if(t[Zr]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Zr])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[ue]&8192)}}var VA=100;function zC(t,n=0){let i=t[pi].rendererFactory,r=!1;r||i.begin?.();try{BA(t,n)}finally{r||i.end?.()}}function BA(t,n){let e=Sh();try{_l(!0),dg(t,n);let i=0;for(;Il(t);){if(i===VA)throw new O(103,!1);i++,dg(t,1)}}finally{_l(e)}}function HA(t,n,e,i){if(Qo(n))return;let r=n[ue],o=!1,a=!1;pu(n);let s=!0,l=null,c=null;o||(HC(t)?(c=RA(n),l=ur(c)):md()===null?(s=!1,c=LA(n),l=ur(c)):n[Vn]&&(zr(n[Vn]),n[Vn]=null));try{gh(n),Uy(t.bindingStartIndex),e!==null&&FC(t,n,e,2,i);let f=(r&3)===3;if(!o)if(f){let b=t.preOrderCheckHooks;b!==null&&wu(n,b,null)}else{let b=t.preOrderHooks;b!==null&&Eu(n,b,0,null),Vh(n,0)}if(a||UA(n),UC(n),$C(n,0),t.contentQueries!==null&&dC(t,n),!o)if(f){let b=t.contentCheckHooks;b!==null&&wu(n,b)}else{let b=t.contentHooks;b!==null&&Eu(n,b,1),Vh(n,1)}$A(t,n);let g=t.components;g!==null&&GC(n,g,0);let _=t.viewQuery;if(_!==null&&Xh(2,_,i),!o)if(f){let b=t.viewCheckHooks;b!==null&&wu(n,b)}else{let b=t.viewHooks;b!==null&&Eu(n,b,2),Vh(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[su]){for(let b of n[su])b();n[su]=null}o||(VC(n),n[ue]&=-73)}catch(f){throw o||Ko(n),f}finally{c!==null&&(Ur(c,l),s&&FA(c)),hu()}}function $C(t,n){for(let e=rC(t);e!==null;e=oC(e))for(let i=cn;i<e.length;i++){let r=e[i];qC(r,n)}}function UA(t){for(let n=rC(t);n!==null;n=oC(n)){if(!(n[ue]&2))continue;let e=n[Zo];for(let i=0;i<e.length;i++){let r=e[i];_h(r)}}}function zA(t,n,e){Le(Ae.ComponentStart);let i=ti(n,t);try{qC(i,e)}finally{Le(Ae.ComponentEnd,i[rn])}}function qC(t,n){cu(t)&&dg(t,n)}function dg(t,n){let i=t[ie],r=t[ue],o=t[Vn],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&Sa(o)),a||=!1,o&&(o.dirty=!1),t[ue]&=-9217,a)HA(i,t,i.template,t[rn]);else if(r&8192){let s=ce(null);try{UC(t),$C(t,1);let l=i.components;l!==null&&GC(t,l,1),VC(t)}finally{ce(s)}}}function GC(t,n,e){for(let i=0;i<n.length;i++)zA(t,n[i],e)}function $A(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)io(~r);else{let o=r,a=e[++i],s=e[++i];$y(a,o);let l=n[o];Le(Ae.HostBindingsUpdateStart,l);try{s(2,l)}finally{Le(Ae.HostBindingsUpdateEnd,l)}}}}finally{io(-1)}}function e_(t,n){let e=Sh()?64:1088;for(t[pi].changeDetectionScheduler?.notify(n);t;){t[ue]|=e;let i=Qr(t);if(Ga(t)&&!i)return t;t=i}return null}function WC(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function qA(t,n){let e=cn+n;if(e<t.length)return t[e]}function t_(t,n,e,i=!0){let r=n[ie];if(WA(r,n,t,e),i){let a=cg(e,t),s=n[$e],l=s.parentNode(t[no]);l!==null&&sA(r,t[bn],s,n,l,a)}let o=n[Ha];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function GA(t,n){let e=Fu(t,n);return e!==void 0&&qg(e[ie],e),e}function Fu(t,n){if(t.length<=cn)return;let e=cn+n,i=t[e];if(i){let r=i[eo];r!==null&&r!==t&&$g(r,i),n>0&&(t[e-1][Xn]=i[Xn]);let o=El(t,cn+n);aA(i[ie],i);let a=o[Yi];a!==null&&a.detachView(o[ie]),i[Wt]=null,i[Xn]=null,i[ue]&=-129}return i}function WA(t,n,e,i){let r=cn+i,o=e.length;i>0&&(e[r-1][Xn]=n),i<o-cn?(n[Xn]=e[r],ih(e,cn+i,n)):(e.push(n),n[Xn]=null),n[Wt]=e;let a=n[eo];a!==null&&e!==a&&YC(a,n);let s=n[Yi];s!==null&&s.insertView(t),du(n),n[ue]|=128}function YC(t,n){let e=t[Zo],i=n[Wt];if(br(i))t[ue]|=2;else{let r=i[Wt][In];n[In]!==r&&(t[ue]|=2)}e===null?t[Zo]=[n]:e.push(n)}var ro=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[ie];return Bl(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[rn]}set context(n){this._lView[rn]=n}get destroyed(){return Qo(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Wt];if(hi(n)){let e=n[Ml],i=e?e.indexOf(this):-1;i>-1&&(Fu(n,i),El(e,i))}this._attachedToViewContainer=!1}qg(this._lView[ie],this._lView)}onDestroy(n){vh(this._lView,n)}markForCheck(){e_(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[ue]&=-129}reattach(){du(this._lView),this._lView[ue]|=128}detectChanges(){this._lView[ue]|=1024,zC(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new O(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Ga(this._lView),e=this._lView[eo];e!==null&&!n&&$g(e,this._lView),AC(this._lView[ie],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new O(902,!1);this._appRef=n;let e=Ga(this._lView),i=this._lView[eo];i!==null&&!e&&YC(i,this._lView),du(this._lView)}};var St=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=YA;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Jg(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new ro(o)}}return t})();function YA(){return Xu(Ht(),re())}function Xu(t,n){return t.type&4?new St(n,t,ns(t,n)):null}function is(t,n,e,i,r){let o=t.data[n];if(o===null)o=ZA(t,n,e,i,r),zy()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=By();o.injectorIndex=a===null?-1:a.injectorIndex}return Wa(o,!0),o}function ZA(t,n,e,i,r){let o=Dh(),a=Mh(),s=a?o:o&&o.parent,l=t.data[n]=KA(t,s,e,n,i,r);return QA(t,l,o,a),l}function QA(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function KA(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return xh()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:Oh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var XA=()=>null,JA=()=>null;function ug(t,n){return XA(t,n)}function ek(t,n,e){return JA(t,n,e)}var ZC=class{},Ju=class{},mg=class{resolveComponentFactory(n){throw new O(917,!1)}},Zl=class{static NULL=new mg},Tt=class{},Se=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>tk()}return t})();function tk(){let t=re(),n=Ht(),e=ti(n.index,t);return(br(e)?e:t)[$e]}var QC=(()=>{class t{static \u0275prov=x({token:t,providedIn:"root",factory:()=>null})}return t})();var Mu={},fg=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Mu,i);return r!==Mu||e===Mu?r:this.parentInjector.get(n,e,i)}};function Pu(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=Jd(r,s);else if(o==2){let l=s,c=n[++a];i=Jd(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function M(t,n=0){let e=re();if(e===null)return K(t,n);let i=Ht();return X0(i,e,Bt(t),n)}function em(){let t="invalid";throw new Error(t)}function KC(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,l=null,c=null;for(let f of a)if(f.resolveHostDirectives!==null){[s,l,c]=f.resolveHostDirectives(a);break}rk(t,n,e,s,o,l,c)}o!==null&&i!==null&&nk(e,i,o)}function nk(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new O(-301,!1);i.push(n[r],o)}}function ik(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function rk(t,n,e,i,r,o,a){let s=i.length,l=null;for(let _=0;_<s;_++){let b=i[_];l===null&&gi(b)&&(l=b,ik(t,e,_)),Zh(Ou(e,n),t,b.type)}dk(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let _=0;_<s;_++){let b=i[_];b.providersResolver&&b.providersResolver(b)}let c=!1,f=!1,g=IC(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let _=0;_<s;_++){let b=i[_];if(e.mergedAttrs=Xa(e.mergedAttrs,b.hostAttrs),ak(t,e,n,g,b),ck(g,b,r),a!==null&&a.has(b)){let[z,q]=a.get(b);e.directiveToIndex.set(b.type,[g,z+e.directiveStart,q+e.directiveStart])}else(o===null||!o.has(b))&&e.directiveToIndex.set(b.type,g);b.contentQueries!==null&&(e.flags|=4),(b.hostBindings!==null||b.hostAttrs!==null||b.hostVars!==0)&&(e.flags|=64);let w=b.type.prototype;!c&&(w.ngOnChanges||w.ngOnInit||w.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!f&&(w.ngOnChanges||w.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),f=!0),g++}ok(t,e,o)}function ok(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))w0(0,n,r,i),w0(1,n,r,i),D0(n,i,!1);else{let o=e.get(r);E0(0,n,o,i),E0(1,n,o,i),D0(n,i,!0)}}}function w0(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),XC(n,o)}}function E0(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),XC(n,a)}}function XC(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function D0(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||Lg(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let l=i[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let f of c)if(f===n){a??=[],a.push(l,i[s+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let f=0;f<c.length;f+=2)if(c[f]===n){a??=[],a.push(c[f+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function ak(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Yr(r.type,!0)),a=new ea(o,gi(r),M,null);t.blueprint[i]=a,e[i]=a,sk(t,n,i,IC(t,e,r.hostVars,Qt),r)}function sk(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;lk(a)!=s&&a.push(s),a.push(e,i,o)}}function lk(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function ck(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;gi(n)&&(e[""]=t)}}function dk(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function n_(t,n,e,i,r,o,a,s){let l=n[ie],c=l.consts,f=_i(c,a),g=is(l,t,e,i,f);return o&&KC(l,n,g,_i(c,s),r),g.mergedAttrs=Xa(g.mergedAttrs,g.attrs),g.attrs!==null&&Pu(g,g.attrs,!1),g.mergedAttrs!==null&&Pu(g,g.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,g),g}function i_(t,n){z0(t,n),uh(n)&&t.queries.elementEnd(n)}function uk(t,n,e,i,r,o){let a=n.consts,s=_i(a,r),l=is(n,t,e,i,s);if(l.mergedAttrs=Xa(l.mergedAttrs,l.attrs),o!=null){let c=_i(a,o);l.localNames=[];for(let f=0;f<c.length;f+=2)l.localNames.push(c[f],-1)}return l.attrs!==null&&Pu(l,l.attrs,!1),l.mergedAttrs!==null&&Pu(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}function r_(t){return ex(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function JC(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function ex(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function o_(t,n,e){return t[n]=e}function tx(t,n){return t[n]}function ii(t,n,e){if(e===Qt)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function nx(t,n,e,i){let r=ii(t,n,e);return ii(t,n+1,i)||r}function mk(t,n,e,i,r){let o=nx(t,n,e,i);return ii(t,n+2,r)||o}function Iu(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&JS(r,o);let a=yr(t)?ti(t.index,n):n;e_(a,5);let s=n[rn],l=M0(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)l=M0(n,s,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function M0(t,n,e,i){let r=ce(null);try{return Le(Ae.OutputStart,n,e),e(i)!==!1}catch(o){return TA(t,o),!1}finally{Le(Ae.OutputEnd,n,e),ce(r)}}function ix(t,n,e,i,r,o,a,s){let l=qa(t),c=!1,f=null;if(!i&&l&&(f=pk(n,e,o,t.index)),f!==null){let g=f.__ngLastListenerFn__||f;g.__ngNextListenerFn__=a,f.__ngLastListenerFn__=a,c=!0}else{let g=ei(t,e),_=i?i(g):g;tT(e,_,o,s),i||(s.__ngNativeEl__=g);let b=r.listen(_,o,s);if(!fk(o)){let w=i?z=>i(Jn(z[t.index])):t.index;rx(w,n,e,o,s,b,!1)}}return c}function fk(t){return t.startsWith("animation")||t.startsWith("transition")}function pk(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[Ua],l=r[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function rx(t,n,e,i,r,o,a){let s=n.firstCreatePass?yh(n):null,l=bh(e),c=l.length;l.push(r,o),s&&s.push(i,t,c,(c+1)*(a?-1:1))}function I0(t,n,e,i,r,o){let a=n[e],s=n[ie],c=s.data[e].outputs[i],g=a[c].subscribe(o);rx(t.index,s,n,r,o,g,!0)}var pg=Symbol("BINDING");function ox(t){return t.debugInfo?.className||t.type.name||null}var Lu=class extends Zl{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=gr(n);return new ta(e,this.ngModule)}};function hk(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Wu.SignalBased)!==0};return r&&(o.transform=r),o})}function gk(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function _k(t,n,e){let i=n instanceof Ne?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new fg(e,i):e}function vk(t){let n=t.get(Tt,null);if(n===null)throw new O(407,!1);let e=t.get(QC,null),i=t.get(Wi,null),r=t.get(wi,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function bk(t,n){let e=ax(t);return _C(n,e,e==="svg"?gl:e==="math"?mh:null)}function yk(t){if(t?.toLowerCase()==="script")throw new O(905,!1)}function ax(t){return(t.selectors[0][0]||"div").toLowerCase()}var ta=class extends Ju{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=hk(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=gk(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=YT(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a){Le(Ae.DynamicComponentStart);let s=ce(null);try{let l=this.componentDef,c=_k(l,r||this.ngModule,n),f=vk(c),g=f.tracingService;return g&&g.componentCreate?g.componentCreate(ox(l),()=>this.createComponentRef(f,c,e,i,o,a)):this.createComponentRef(f,c,e,i,o,a)}finally{ce(s)}}createComponentRef(n,e,i,r,o,a){let s=this.componentDef,l=Ck(r,s,a,o),c=n.rendererFactory.createRenderer(null,s),f=r?vA(c,r,s.encapsulation,e):bk(s,c);yk(f?.tagName);let g=a?.some(S0)||o?.some(w=>typeof w!="function"&&w.bindings.some(S0)),_=Vg(null,l,null,512|MC(s),null,null,n,c,e,null,cC(f,e,!0));_[lt]=f,pu(_);let b=null;try{let w=n_(lt,_,2,"#host",()=>l.directiveRegistry,!0,0);bC(c,f,w),Ja(f,_),Ku(l,_,w),Tg(l,w,_),i_(l,w),i!==void 0&&wk(w,this.ngContentSelectors,i),b=ti(w.index,_),_[rn]=b[rn],Xg(l,_,null)}catch(w){throw b!==null&&Kh(b),Kh(_),w}finally{Le(Ae.DynamicComponentEnd),hu()}return new ju(this.componentType,_,!!g)}};function Ck(t,n,e,i){let r=t?["ng-version","21.2.20"]:ZT(n.selectors[0]),o=null,a=null,s=0;if(e)for(let f of e)s+=f[pg].requiredVars,f.create&&(f.targetIdx=0,(o??=[]).push(f)),f.update&&(f.targetIdx=0,(a??=[]).push(f));if(i)for(let f=0;f<i.length;f++){let g=i[f];if(typeof g!="function")for(let _ of g.bindings){s+=_[pg].requiredVars;let b=f+1;_.create&&(_.targetIdx=b,(o??=[]).push(_)),_.update&&(_.targetIdx=b,(a??=[]).push(_))}}let l=[n];if(i)for(let f of i){let g=typeof f=="function"?f:f.type,_=tu(g);l.push(_)}return jg(0,null,xk(o,a),1,s,l,null,null,null,[r],null)}function xk(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function S0(t){let n=t[pg].kind;return n==="input"||n==="twoWay"}var ju=class extends ZC{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=lu(e[ie],lt),this.location=ns(this._tNode,e),this.instance=ti(this._tNode.index,e)[rn],this.hostView=this.changeDetectorRef=new ro(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=Kg(i,r[ie],r,n,e);this.previousInputValues.set(n,e);let a=ti(i.index,r);e_(a,1)}get injector(){return new Jo(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function wk(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var ct=(()=>{class t{static __NG_ELEMENT_ID__=Ek}return t})();function Ek(){let t=Ht();return sx(t,re())}var hg=class t extends ct{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return ns(this._hostTNode,this._hostLView)}get injector(){return new Jo(this._hostTNode,this._hostLView)}get parentInjector(){let n=Mg(this._hostTNode,this._hostLView);if(G0(n)){let e=Au(n,this._hostLView),i=Tu(n),r=e[ie].data[i+8];return new Jo(r,e)}else return new Jo(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=T0(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-cn}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let a=ug(this._lContainer,n.ssrId),s=n.createEmbeddedViewImpl(e||{},o,a);return this.insertImpl(s,r,Nu(this._hostTNode,a)),s}createComponent(n,e,i,r,o,a,s){let l=n&&!IS(n),c;if(l)c=e;else{let q=e||{};c=q.index,i=q.injector,r=q.projectableNodes,o=q.environmentInjector||q.ngModuleRef,a=q.directives,s=q.bindings}let f=l?n:new ta(gr(n)),g=i||this.parentInjector;if(!o&&f.ngModule==null){let X=(l?g:this.parentInjector).get(Ne,null);X&&(o=X)}let _=gr(f.componentType??{}),b=ug(this._lContainer,_?.id??null),w=b?.firstChild??null,z=f.create(g,r,w,o,a,s);return this.insertImpl(z.hostView,c,Nu(this._hostTNode,b)),z}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(Ny(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=r[Wt],c=new t(l,l[bn],l[Wt]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return t_(a,r,o,i),n.attachToViewContainerRef(),ih(Uh(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=T0(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Fu(this._lContainer,e);i&&(El(Uh(this._lContainer),e),qg(i[ie],i))}detach(n){let e=this._adjustIndex(n,-1),i=Fu(this._lContainer,e);return i&&El(Uh(this._lContainer),e)!=null?new ro(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function T0(t){return t[Ml]}function Uh(t){return t[Ml]||(t[Ml]=[])}function sx(t,n){let e,i=n[t.index];return hi(i)?e=i:(e=WC(i,n,null,t),n[t.index]=e,Bg(n,e)),Mk(e,n,t,i),new hg(e,t,n)}function Dk(t,n){let e=t[$e],i=e.createComment(""),r=ei(n,t),o=e.parentNode(r);return Ru(e,o,i,e.nextSibling(r),!1),i}var Mk=Tk,Ik=()=>!1;function Sk(t,n,e){return Ik(t,n,e)}function Tk(t,n,e,i){if(t[no])return;let r;e.type&8?r=Jn(i):r=Dk(n,e),t[no]=r}var gg=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},_g=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)s_(n,e).matches!==null&&this.queries[e].setDirty()}},Vu=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=Nk(n):this.predicate=n}},vg=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},bg=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,Ak(e,o)),this.matchTNodeWithReadOption(n,e,Du(e,n,o,!1,!1))}else i===St?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Du(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===N||r===ct||r===St&&e.type&4)this.addMatch(e.index,-2);else{let o=Du(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function Ak(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function kk(t,n){return t.type&11?ns(t,n):t.type&4?Xu(t,n):null}function Ok(t,n,e,i){return e===-1?kk(n,t):e===-2?Rk(t,n,i):jl(t,t[ie],e,n)}function Rk(t,n,e){if(e===N)return ns(n,t);if(e===St)return Xu(n,t);if(e===ct)return sx(n,t)}function lx(t,n,e,i){let r=n[Yi].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let f=o[c];s.push(Ok(n,f,a[l+1],e.metadata.read))}}r.matches=s}return r.matches}function yg(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=lx(t,n,r,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)i.push(a[s/2]);else{let c=o[s+1],f=n[-l];for(let g=cn;g<f.length;g++){let _=f[g];_[eo]===_[Wt]&&yg(_[ie],_,c,i)}if(f[Zo]!==null){let g=f[Zo];for(let _=0;_<g.length;_++){let b=g[_];yg(b[ie],b,c,i)}}}}}return i}function a_(t,n){return t[Yi].queries[n].queryList}function cx(t,n,e){let i=new Bn((e&4)===4);return Ly(t,n,i,i.destroy),(n[Yi]??=new _g).queries.push(new gg(i))-1}function dx(t,n,e){let i=Ke();return i.firstCreatePass&&(mx(i,new Vu(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),cx(i,re(),n)}function ux(t,n,e,i){let r=Ke();if(r.firstCreatePass){let o=Ht();mx(r,new Vu(n,e,i),o.index),Fk(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return cx(r,re(),e)}function Nk(t){return t.split(",").map(n=>n.trim())}function mx(t,n,e){t.queries===null&&(t.queries=new vg),t.queries.track(new bg(n,e))}function Fk(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function s_(t,n){return t.queries.getByIndex(n)}function fx(t,n){let e=t[ie],i=s_(e,n);return i.crossesNgTemplate?yg(e,t,n,[]):lx(e,t,i,n)}function px(t,n,e){let i,r=rl(()=>{i._dirtyCounter();let o=Pk(i,t);if(n&&o===void 0)throw new O(-951,!1);return o});return i=r[Dt],i._dirtyCounter=ee(0),i._flatValue=void 0,r}function l_(t){return px(!0,!1,t)}function c_(t){return px(!0,!0,t)}function hx(t,n){let e=t[Dt];e._lView=re(),e._queryIndex=n,e._queryList=a_(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function Pk(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[ue]&4)return n?void 0:ln;let r=a_(e,i),o=fx(e,i);return r.reset(o,tC),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var Ki=class{},tm=class{};var Bu=class extends Ki{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Lu(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=eh(n);this._bootstrapComponents=wC(o.bootstrap),this._r3Injector=Rh(n,e,[{provide:Ki,useValue:this},{provide:Zl,useValue:this.componentFactoryResolver},...i],yl(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Hu=class extends tm{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Bu(this.moduleType,n,[])}};var Hl=class extends Ki{injector;componentFactoryResolver=new Lu(this);instance=null;constructor(n){super();let e=new $o([...n.providers,{provide:Ki,useValue:this},{provide:Zl,useValue:this.componentFactoryResolver}],n.parent||Ba(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Ql(t,n,e=null){return new Hl({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var Lk=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=ah(!1,e.type),r=i.length>0?Ql([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=x({token:t,providedIn:"environment",factory:()=>new t(K(Ne))})}return t})();function T(t){return zl(()=>{let n=gx(t),e=ne(C({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===Ig.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(Lk).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Ci.Emulated,styles:t.styles||ln,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&oa("NgStandalone"),_x(e);let i=t.dependencies;return e.directiveDefs=A0(i,jk),e.pipeDefs=A0(i,gy),e.id=Hk(e),e})}function jk(t){return gr(t)||tu(t)}function j(t){return zl(()=>({type:t.type,bootstrap:t.bootstrap||ln,declarations:t.declarations||ln,imports:t.imports||ln,exports:t.exports||ln,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function Vk(t,n){if(t==null)return mi;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,a,s,l;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,l=r[3]||null):(o=r,a=r,s=Wu.None,l=null),e[o]=[i,s,l],n[o]=a}return e}function Bk(t){if(t==null)return mi;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function S(t){return zl(()=>{let n=gx(t);return _x(n),n})}function d_(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function gx(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||mi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||ln,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:Vk(t.inputs,n),outputs:Bk(t.outputs),debugInfo:null}}function _x(t){t.features?.forEach(n=>n(t))}function A0(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function Hk(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function u_(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=Uk,e.hostDirectives=i?t.map(Cg):[t]):i?e.hostDirectives.unshift(...t.map(Cg)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function Uk(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let a=t[o];if(a.hostDirectives!==null){let s=n.length;i??=new Map,r??=new Map,vx(a,n,i),r.set(a,[s,n.length-1])}o===0&&gi(a)&&(e=!0,n.push(a))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return[n,i,r]}function vx(t,n,e){if(t.hostDirectives!==null)for(let i of t.hostDirectives)if(typeof i=="function"){let r=i();for(let o of r)k0(Cg(o),n,e)}else k0(i,n,e)}function k0(t,n,e){let i=tu(t.directive);zk(i.declaredInputs,t.inputs),vx(i,n,e),e.set(i,t),n.push(i)}function Cg(t){return typeof t=="function"?{directive:Bt(t),inputs:mi,outputs:mi}:{directive:Bt(t.directive),inputs:O0(t.inputs),outputs:O0(t.outputs)}}function O0(t){if(t===void 0||t.length===0)return mi;let n={};for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function zk(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function $k(t){return Object.getPrototypeOf(t.prototype).constructor}function pe(t){let n=$k(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,xl)?n[xl]:void 0,a=Object.hasOwn(n,wl)?n[wl]:void 0;if(gi(t))r=o??a;else{if(o)throw new O(903,!1);r=a}if(r){if(e){i.push(r);let l=t;l.inputs=zh(t.inputs),l.declaredInputs=zh(t.declaredInputs),l.outputs=zh(t.outputs);let c=r.hostBindings;c&&Zk(t,c);let f=r.viewQuery,g=r.contentQueries;if(f&&Wk(t,f),g&&Yk(t,g),qk(t,r),hy(t.outputs,r.outputs),gi(r)&&r.data.animation){let _=t.data;_.animation=(_.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let l=0;l<s.length;l++){let c=s[l];c&&c.ngInherit&&c(t),c===pe&&(e=!1)}}n=Object.getPrototypeOf(n)}Gk(i)}function qk(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function Gk(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Xa(r.hostAttrs,e=Xa(e,r.hostAttrs))}}function zh(t){return t===mi?{}:t===ln?[]:t}function Wk(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function Yk(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function Zk(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function bx(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=Xa(t.mergedAttrs,t.attrs);let f=t.tView=jg(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),f.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),Wa(t,!1);let l=Kk(e,n,t,i);gu()&&Gg(e,n,l,t),Ja(l,n);let c=WC(l,n,l,t);n[i+lt]=c,Bg(n,c),Sk(c,t,n)}function Qk(t,n,e,i,r,o,a,s,l,c,f){let g=e+lt,_;return n.firstCreatePass?(_=is(n,g,4,a||null,s||null),uu()&&KC(n,t,_,_i(n.consts,c),Yg),z0(n,_)):_=n.data[g],bx(_,t,n,e,i,r,o,l),qa(_)&&Ku(n,t,_),c!=null&&Yl(t,_,f),_}function nm(t,n,e,i,r,o,a,s,l,c,f){let g=e+lt,_;if(n.firstCreatePass){if(_=is(n,g,4,a||null,s||null),c!=null){let b=_i(n.consts,c);_.localNames=[];for(let w=0;w<b.length;w+=2)_.localNames.push(b[w],-1)}}else _=n.data[g];return bx(_,t,n,e,i,r,o,l),c!=null&&Yl(t,_,f),_}function A(t,n,e,i,r,o,a,s){let l=re(),c=Ke(),f=_i(c.consts,o);return Qk(l,c,t,n,e,i,r,f,void 0,a,s),A}function rs(t,n,e,i,r,o,a,s){let l=re(),c=Ke(),f=_i(c.consts,o);return nm(l,c,t,n,e,i,r,f,void 0,a,s),rs}var Kk=Xk;function Xk(t,n,e,i){return Ol(!0),n[$e].createComment("")}var im=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function Xi(t){return typeof t=="function"&&t[Dt]!==void 0}function m_(t){return Xi(t)&&typeof t.set=="function"}var f_=new y("");function Mr(t){return!!t&&typeof t.then=="function"}function rm(t){return!!t&&typeof t.subscribe=="function"}var yx=new y("");var p_=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(yx,{optional:!0})??[];injector=d(G);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Zt(this.injector,r);if(Mr(o))e.push(o);else if(rm(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),om=new y("");function Cx(){vp(()=>{let t="";throw new O(600,t)})}function xx(t){return t.isBoundToModule}var Jk=10;var Tn=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(Sn);afterRenderManager=d(Zu);zonelessEnabled=d(Rl);rootEffectScheduler=d(vu);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new E;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(Cr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe($(e=>!e))}constructor(){d(wi,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(Ne);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=G.NULL){return this._injector.get(P).run(()=>{Le(Ae.BootstrapComponentStart);let a=e instanceof Ju;if(!this._injector.get(p_).done){let w="";throw new O(405,w)}let l;a?l=e:l=this._injector.get(Zl).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=xx(l)?void 0:this._injector.get(Ki),f=i||l.selector,g=l.create(r,[],f,c),_=g.location.nativeElement,b=g.injector.get(f_,null);return b?.registerApplication(_),g.onDestroy(()=>{this.detachView(g.hostView),Ll(this.components,g),b?.unregisterApplication(_)}),this._loadComponent(g),Le(Ae.BootstrapComponentEnd,g),g})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Le(Ae.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Yu.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Le(Ae.ChangeDetectionEnd),new O(101,!1);let e=ce(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,ce(e),this.afterTick.next(),Le(Ae.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Tt,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<Jk;){Le(Ae.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Le(Ae.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Il(r))continue;let o=i&&!this.zonelessEnabled?0:1;zC(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Il(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Ll(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(om,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Ll(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new O(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Ll(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function J(t,n,e,i){let r=re(),o=Xo();if(ii(r,o,n)){let a=Ke(),s=Ya();MA(s,r,t,n,e,i)}return J}function ge(t,n,e,i,r,o,a,s){oa("NgControlFlow");let l=re(),c=Ke(),f=_i(c.consts,o);return nm(l,c,t,n,e,i,r,f,256,a,s),h_}function h_(t,n,e,i,r,o,a,s){oa("NgControlFlow");let l=re(),c=Ke(),f=_i(c.consts,o);return nm(l,c,t,n,e,i,r,f,512,a,s),h_}function _e(t,n){oa("NgControlFlow");let e=re(),i=Xo(),r=e[i]!==Qt?e[i]:-1,o=r!==-1?R0(e,lt+r):void 0,a=0;if(ii(e,i,t)){let s=ce(null);try{if(o!==void 0&&GA(o,a),t!==-1){let l=lt+t,c=R0(e,l),f=e1(e[ie],l),g=ek(c,f,e),_=Jg(e,f,n,{dehydratedView:g});t_(c,_,a,Nu(f,g))}}finally{ce(s)}}else if(o!==void 0){let s=qA(o,a);s!==void 0&&(s[rn]=n)}}function R0(t,n){return t[n]}function e1(t,n){return lu(t,n)}function v(t,n,e){let i=re(),r=Xo();if(ii(i,r,n)){let o=Ke(),a=Ya();PC(a,i,t,n,i[$e],e)}return v}function xg(t,n,e,i,r){Kg(n,t,e,r?"class":"style",i)}function m(t,n,e,i){let r=re(),o=r[ie],a=t+lt,s=o.firstCreatePass?n_(a,r,2,n,Yg,uu(),e,i):o.data[a];if(yr(s)){let l=r[pi].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(ox(c),()=>(N0(t,n,r,s,i),m))}}return N0(t,n,r,s,i),m}function N0(t,n,e,i,r){if(Zg(i,e,t,n,wx),qa(i)){let o=e[ie];Ku(o,e,i),Tg(o,i,e)}r!=null&&Yl(e,i)}function u(){let t=Ke(),n=Ht(),e=Qg(n);return t.firstCreatePass&&i_(t,e),wh(e)&&Eh(),Ch(),e.classesWithoutHost!=null&&RS(e)&&xg(t,e,re(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&NS(e)&&xg(t,e,re(),e.stylesWithoutHost,!1),u}function te(t,n,e,i){return m(t,n,e,i),u(),te}function dt(t,n,e,i){let r=re(),o=r[ie],a=t+lt,s=o.firstCreatePass?uk(a,o,2,n,e,i):o.data[a];return Zg(s,r,t,n,wx),i!=null&&Yl(r,s),dt}function At(){let t=Ht(),n=Qg(t);return wh(n)&&Eh(),Ch(),At}function An(t,n,e,i){return dt(t,n,e,i),At(),An}var wx=(t,n,e,i,r)=>(Ol(!0),_C(n[$e],i,Oh()));function Kl(t,n,e){let i=re(),r=i[ie],o=t+lt,a=r.firstCreatePass?n_(o,i,8,"ng-container",Yg,uu(),n,e):r.data[o];if(Zg(a,i,t,"ng-container",t1),qa(a)){let s=i[ie];Ku(s,i,a),Tg(s,a,i)}return e!=null&&Yl(i,a),Kl}function Xl(){let t=Ke(),n=Ht(),e=Qg(n);return t.firstCreatePass&&i_(t,e),Xl}function os(t,n,e){return Kl(t,n,e),Xl(),os}var t1=(t,n,e,i,r)=>(Ol(!0),AT(n[$e],""));function Oe(){return re()}function dn(t,n,e){let i=re(),r=Xo();if(ii(i,r,n)){let o=Ke(),a=Ya();LC(a,i,t,n,i[$e],e)}return dn}var Jl="en-US";var n1=Jl;function Ex(t){typeof t=="string"&&(n1=t.toLowerCase().replace(/_/g,"-"))}function D(t,n,e){let i=re(),r=Ke(),o=Ht();return Dx(r,i,i[$e],o,t,n,e),D}function as(t,n,e){let i=re(),r=Ke(),o=Ht();return(o.type&3||e)&&ix(o,r,i,e,i[$e],t,n,Iu(o,i,n)),as}function Dx(t,n,e,i,r,o,a){let s=!0,l=null;if((i.type&3||a)&&(l??=Iu(i,n,o),ix(i,t,n,a,e,r,o,l)&&(s=!1)),s){let c=i.outputs?.[r],f=i.hostDirectiveOutputs?.[r];if(f&&f.length)for(let g=0;g<f.length;g+=2){let _=f[g],b=f[g+1];l??=Iu(i,n,o),I0(i,n,_,b,r,l)}if(c&&c.length)for(let g of c)l??=Iu(i,n,o),I0(i,n,g,r,r,l)}}function I(t=1){return Qy(t)}function i1(t,n){let e=null,i=zT(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?DC(t,o,!0):GT(i,o))return r}return e}function Me(t){let n=re()[In][bn];if(!n.projection){let e=t?t.length:1,i=n.projection=xy(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?i1(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function V(t,n=0,e,i,r,o){let a=re(),s=Ke(),l=i?t+1:null;l!==null&&nm(a,s,l,i,r,o,null,e);let c=is(s,lt+t,16,null,e||null);c.projection===null&&(c.projection=n),Ih();let g=!a[Ha]||xh();a[In][bn].projection[c.projection]===null&&l!==null?r1(a,s,l):g&&!qu(c)&&hA(s,a,c)}function r1(t,n,e){let i=lt+e,r=n.data[i],o=t[i],a=ug(o,r.tView.ssrId),s=Jg(t,r,void 0,{dehydratedView:a});t_(o,s,0,Nu(r,a))}function ut(t,n,e,i){return ux(t,n,e,i),ut}function qe(t,n,e){return dx(t,n,e),qe}function H(t){let n=re(),e=Ke(),i=fu();Al(i+1);let r=s_(e,i);if(t.dirty&&Ry(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=fx(n,i);t.reset(o,tC),t.notifyOnChanges()}return!0}return!1}function U(){return a_(re(),fu())}function am(t,n,e,i,r){return hx(n,ux(t,e,i,r)),am}function sm(t,n,e,i){return hx(t,dx(n,e,i)),sm}function lm(t=1){Al(fu()+t)}function Be(t){let n=Hy();return ph(n,lt+t)}function xu(t,n){return t<<17|n<<2}function na(t){return t>>17&32767}function o1(t){return(t&2)==2}function a1(t,n){return t&131071|n<<17}function wg(t){return t|2}function ts(t){return(t&131068)>>2}function $h(t,n){return t&-131069|n<<2}function s1(t){return(t&1)===1}function Eg(t){return t|1}function l1(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=na(a),l=ts(a);t[i]=e;let c=!1,f;if(Array.isArray(e)){let g=e;f=g[1],(f===null||Va(g,f)>0)&&(c=!0)}else f=e;if(r)if(l!==0){let _=na(t[s+1]);t[i+1]=xu(_,s),_!==0&&(t[_+1]=$h(t[_+1],i)),t[s+1]=a1(t[s+1],i)}else t[i+1]=xu(s,0),s!==0&&(t[s+1]=$h(t[s+1],i)),s=i;else t[i+1]=xu(l,0),s===0?s=i:t[l+1]=$h(t[l+1],i),l=i;c&&(t[i+1]=wg(t[i+1])),F0(t,f,i,!0),F0(t,f,i,!1),c1(n,f,t,i,o),a=xu(s,l),o?n.classBindings=a:n.styleBindings=a}function c1(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Va(o,n)>=0&&(e[i+1]=Eg(e[i+1]))}function F0(t,n,e,i){let r=t[e+1],o=n===null,a=i?na(r):ts(r),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];d1(l,n)&&(s=!0,t[a+1]=i?Eg(c):wg(c)),a=i?na(c):ts(c)}s&&(t[e+1]=i?wg(r):Eg(r))}function d1(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Va(t,n)>=0:!1}var yi={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function u1(t){return t.substring(yi.key,yi.keyEnd)}function m1(t){return f1(t),Mx(t,Ix(t,0,yi.textEnd))}function Mx(t,n){let e=yi.textEnd;return e===n?-1:(n=yi.keyEnd=p1(t,yi.key=n,e),Ix(t,n,e))}function f1(t){yi.key=0,yi.keyEnd=0,yi.value=0,yi.valueEnd=0,yi.textEnd=t.length}function Ix(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function p1(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Hn(t,n,e){return Sx(t,n,e,!1),Hn}function R(t,n){return Sx(t,n,null,!0),R}function on(t){g1(x1,h1,t,!0)}function h1(t,n){for(let e=m1(n);e>=0;e=Mx(n,e))ou(t,u1(n),!0)}function Sx(t,n,e,i){let r=re(),o=Ke(),a=Tl(2);if(o.firstUpdatePass&&Ax(o,t,a,i),n!==Qt&&ii(r,a,n)){let s=o.data[vi()];kx(o,s,r,r[$e],t,r[a+1]=E1(n,e),i,a)}}function g1(t,n,e,i){let r=Ke(),o=Tl(2);r.firstUpdatePass&&Ax(r,null,o,i);let a=re();if(e!==Qt&&ii(a,o,e)){let s=r.data[vi()];if(Ox(s,i)&&!Tx(r,o)){let l=i?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=Jd(l,e||"")),xg(r,s,a,e,i)}else w1(r,s,a,a[$e],a[o+1],a[o+1]=C1(t,n,e),i,o)}}function Tx(t,n){return n>=t.expandoStartIndex}function Ax(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[vi()],a=Tx(t,e);Ox(o,i)&&n===null&&!a&&(n=!1),n=_1(r,o,n,i),l1(r,o,n,e,a,i)}}function _1(t,n,e,i){let r=Gy(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=qh(null,t,n,e,i),e=Ul(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=qh(r,t,n,e,i),o===null){let l=v1(t,n,i);l!==void 0&&Array.isArray(l)&&(l=qh(null,t,n,l[1],i),l=Ul(l,n.attrs,i),b1(t,n,i,l))}else o=y1(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function v1(t,n,e){let i=e?n.classBindings:n.styleBindings;if(ts(i)!==0)return t[na(i)]}function b1(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[na(r)]=i}function y1(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=Ul(i,a,e)}return Ul(i,n.attrs,e)}function qh(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=Ul(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function Ul(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),ou(t,a,e?!0:n[++o]))}return t===void 0?null:t}function C1(t,n,e){if(e==null||e==="")return ln;let i=[],r=xi(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function x1(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&ou(t,i,e)}function w1(t,n,e,i,r,o,a,s){r===Qt&&(r=ln);let l=0,c=0,f=0<r.length?r[0]:null,g=0<o.length?o[0]:null;for(;f!==null||g!==null;){let _=l<r.length?r[l+1]:void 0,b=c<o.length?o[c+1]:void 0,w=null,z;f===g?(l+=2,c+=2,_!==b&&(w=g,z=b)):g===null||f!==null&&f<g?(l+=2,w=f):(c+=2,w=g,z=b),w!==null&&kx(t,n,e,i,w,z,a,s),f=l<r.length?r[l]:null,g=c<o.length?o[c]:null}}function kx(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],f=s1(c)?P0(l,n,e,r,ts(c),a):void 0;if(!Uu(f)){Uu(o)||o1(c)&&(o=P0(l,null,e,r,s,a));let g=fh(vi(),e);_A(i,a,g,r,o)}}function P0(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let l=t[r],c=Array.isArray(l),f=c?l[1]:l,g=f===null,_=e[r+1];_===Qt&&(_=g?ln:void 0);let b=g?au(_,i):f===i?_:void 0;if(c&&!Uu(b)&&(b=au(l,i)),Uu(b)&&(s=b,a))return s;let w=t[r+1];r=a?na(w):ts(w)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=au(l,i))}return s}function Uu(t){return t!==void 0}function E1(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=yl(xi(t)))),t}function Ox(t,n){return(t.flags&(n?8:16))!==0}function p(t,n=""){let e=re(),i=Ke(),r=t+lt,o=i.firstCreatePass?is(i,r,1,n,null):i.data[r],a=D1(i,e,o,n);e[r]=a,gu()&&Gg(i,e,a,o),Wa(o,!1)}var D1=(t,n,e,i)=>(Ol(!0),ST(n[$e],i));function M1(t,n,e,i=""){return ii(t,Xo(),e)?n+_r(e)+i:Qt}function I1(t,n,e,i,r,o=""){let a=Th(),s=nx(t,a,e,r);return Tl(2),s?n+_r(e)+i+_r(r)+o:Qt}function S1(t,n,e,i,r,o,a,s=""){let l=Th(),c=mk(t,l,e,r,a);return Tl(3),c?n+_r(e)+i+_r(r)+o+_r(a)+s:Qt}function B(t){return Y("",t),B}function Y(t,n,e){let i=re(),r=M1(i,t,n,e);return r!==Qt&&g_(i,vi(),r),Y}function kt(t,n,e,i,r){let o=re(),a=I1(o,t,n,e,i,r);return a!==Qt&&g_(o,vi(),a),kt}function cm(t,n,e,i,r,o,a){let s=re(),l=S1(s,t,n,e,i,r,o,a);return l!==Qt&&g_(s,vi(),l),cm}function g_(t,n,e){let i=fh(n,t);TT(t[$e],i,e)}function Ji(t,n,e){m_(n)&&(n=n());let i=re(),r=Xo();if(ii(i,r,n)){let o=Ke(),a=Ya();PC(a,i,t,n,i[$e],e)}return Ji}function Ir(t,n){let e=m_(t);return e&&t.set(n),e}function er(t,n){let e=re(),i=Ke(),r=Ht();return Dx(i,e,e[$e],r,t,n),er}function ss(t,n,e){let i=Sl()+t,r=re();return r[i]===Qt?o_(r,i,n(e,r)):tx(r,i)}function L0(t,n,e){let i=Ke();i.firstCreatePass&&Rx(n,i.data,i.blueprint,gi(t),e)}function Rx(t,n,e,i,r){if(t=Bt(t),Array.isArray(t))for(let o=0;o<t.length;o++)Rx(t[o],n,e,i,r);else{let o=Ke(),a=re(),s=Ht(),l=zo(t)?t:Bt(t.provide),c=lh(t),f=s.providerIndexes&1048575,g=s.directiveStart,_=s.providerIndexes>>20;if(zo(t)||!t.multi){let b=new ea(c,r,M,null),w=Wh(l,n,r?f:f+_,g);w===-1?(Zh(Ou(s,a),o,l),Gh(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(b),a.push(b)):(e[w]=b,a[w]=b)}else{let b=Wh(l,n,f+_,g),w=Wh(l,n,f,f+_),z=b>=0&&e[b],q=w>=0&&e[w];if(r&&!q||!r&&!z){Zh(Ou(s,a),o,l);let X=k1(r?A1:T1,e.length,r,i,c,t);!r&&q&&(e[w].providerFactory=X),Gh(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(X),a.push(X)}else{let X=Nx(e[r?w:b],c,!r&&i);Gh(o,t,b>-1?b:w,X)}!r&&i&&q&&e[w].componentProviders++}}}function Gh(t,n,e,i){let r=zo(n),o=Sy(n);if(r||o){let l=(o?Bt(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let f=c.indexOf(e);f===-1?c.push(e,[i,l]):c[f+1].push(i,l)}else c.push(e,l)}}}function Nx(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Wh(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function T1(t,n,e,i,r){return Dg(this.multi,[])}function A1(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=jl(i,i[ie],this.providerFactory.index,r);a=l.slice(0,s),Dg(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],Dg(o,a);return a}function Dg(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function k1(t,n,e,i,r,o){let a=new ea(t,e,M,null);return a.multi=[],a.index=n,a.componentProviders=0,Nx(a,r,i&&!e),a}function we(t,n){return e=>{e.providersResolver=(i,r)=>L0(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>L0(i,r?r(n):n,!0))}}function __(t,n){let e=Sl()+t,i=re();return i[e]===Qt?o_(i,e,n()):tx(i,e)}function v_(t,n,e){return Fx(re(),Sl(),t,n,e)}function O1(t,n){let e=t[n];return e===Qt?void 0:e}function Fx(t,n,e,i,r,o){let a=n+e;return ii(t,a,r)?o_(t,a+1,o?i.call(o,r):i(r)):O1(t,a+1)}function Sr(t,n){let e=Ke(),i,r=t+lt;e.firstCreatePass?(i=R1(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=Yr(i.type,!0)),a,s=_n(M);try{let l=ku(!1),c=o();return ku(l),hh(e,re(),r,c),c}finally{_n(s)}}function R1(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function Tr(t,n,e){let i=t+lt,r=re(),o=ph(r,i);return N1(r,i)?Fx(r,Sl(),n,o.transform,e,o):o.transform(e)}function N1(t,n){return t[ie].data[n].pure}function ls(t,n){return Xu(t,n)}var zu=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},b_=(()=>{class t{compileModuleSync(e){return new Hu(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=eh(e),o=wC(r.declarations).reduce((a,s)=>{let l=gr(s);return l&&a.push(new ta(l)),a},[]);return new zu(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Px=(()=>{class t{applicationErrorHandler=d(Sn);appRef=d(Tn);taskService=d(Cr);ngZone=d(P);zonelessEnabled=d(Rl);tracing=d(wi,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new de;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(vl):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(jh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?e0:Nh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(vl+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Lx(){return[{provide:Wi,useExisting:Px},{provide:P,useClass:bl},{provide:Rl,useValue:!0}]}function F1(){return typeof $localize<"u"&&$localize.locale||Jl}var dm=new y("",{factory:()=>d(dm,{optional:!0,skipSelf:!0})||F1()});function Te(t){return dy(t)}function an(t,n){return rl(t,n?.equal)}var P1=t=>t;function y_(t,n){if(typeof t=="function"){let e=Pp(t,P1,n?.equal);return jx(e,n?.debugName)}else{let e=Pp(t.source,t.computation,t.equal);return jx(e,t.debugName)}}function jx(t,n){let e=t[Dt],i=t;return i.set=r=>ly(e,r),i.update=r=>cy(e,r),i.asReadonly=_u.bind(t),i}var Gx=Symbol("InputSignalNode#UNSET"),X1=ne(C({},ol),{transformFn:void 0,applyValueToInputSignal(t,n){Ao(t,n)}});function Wx(t,n){let e=Object.create(X1);e.value=t,e.transformFn=n?.transform;function i(){if(Hr(e),e.value===Gx){let r=null;throw new O(-950,r)}return e.value}return i[Dt]=e,i}var tr=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>$l(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function Vx(t,n){return Wx(t,n)}function J1(t){return Wx(Gx,t)}var Yx=(Vx.required=J1,Vx);function Bx(t,n){return l_(n)}function eO(t,n){return c_(n)}var tc=(Bx.required=eO,Bx);function Hx(t,n){return l_(n)}function tO(t,n){return c_(n)}var Zx=(Hx.required=tO,Hx);var x_=new y(""),nO=new y("");function ec(t){return!t.moduleRef}function iO(t){let n=ec(t)?t.r3Injector:t.moduleRef.injector,e=n.get(P);return e.run(()=>{ec(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Sn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),ec(t)){let o=()=>n.destroy(),a=t.platformInjector.get(x_);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(x_);a.add(o),t.moduleRef.onDestroy(()=>{Ll(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return oO(i,e,()=>{let o=n.get(Cr),a=o.add(),s=n.get(p_);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(dm,Jl);if(Ex(l||Jl),!n.get(nO,!0))return ec(t)?n.get(Tn):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(ec(t)){let f=n.get(Tn);return t.rootComponent!==void 0&&f.bootstrap(t.rootComponent),f}else return rO?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var rO;function oO(t,n,e){try{let i=e();return Mr(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var um=null;function aO(t=[],n){return G.create({name:n,providers:[{provide:Dl,useValue:"platform"},{provide:x_,useValue:new Set([()=>um=null])},...t]})}function sO(t=[]){if(um)return um;let n=aO(t);return um=n,Cx(),lO(n),n}function lO(t){let n=t.get($u,null);Zt(t,()=>{n?.forEach(e=>e())})}var cO=1e4;var K9=cO-1e3;var me=(()=>{class t{static __NG_ELEMENT_ID__=dO}return t})();function dO(t){return uO(Ht(),re(),(t&16)===16)}function uO(t,n,e){if(yr(t)&&!e){let i=ti(t.index,n);return new ro(i,i)}else if(t.type&175){let i=n[In];return new ro(i,n)}return null}var w_=class{supports(n){return r_(n)}create(n){return new E_(n)}},mO=(t,n)=>n,E_=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||mO}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let a=!i||e&&e.currentIndex<Ux(i,r,o)?e:i,s=Ux(a,r,o),l=a.currentIndex;if(a===i)r--,i=i._nextRemoved;else if(e=e._next,a.previousIndex==null)r++;else{o||(o=[]);let c=s-r,f=l-r;if(c!=f){for(let _=0;_<c;_++){let b=_<o.length?o[_]:o[_]=0,w=b+_;f<=w&&w<c&&(o[_]=b+1)}let g=a.previousIndex;o[g]=f-c}}s!==l&&n(a,s,l)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!r_(n))throw new O(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,a;if(Array.isArray(n)){this.length=n.length;for(let s=0;s<this.length;s++)o=n[s],a=this._trackByFn(s,o),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,o,a,s),i=!0):(i&&(e=this._verifyReinsertion(e,o,a,s)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,JC(n,s=>{a=this._trackByFn(r,s),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,s,a,r),i=!0):(i&&(e=this._verifyReinsertion(e,s,a,r)),Object.is(e.item,s)||this._addIdentityChange(e,s)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new D_(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new mm),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new mm),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},D_=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},M_=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},mm=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new M_,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function Ux(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function zx(){return new fm([new w_])}var fm=(()=>{class t{factories;static \u0275prov=x({token:t,providedIn:"root",factory:zx});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=d(t,{optional:!0,skipSelf:!0});return t.create(e,i||zx())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new O(901,!1)}}return t})();function Qx(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Le(Ae.BootstrapApplicationStart);try{let o=r?.injector??sO(i),a=[Lx(),n0,...e||[]],s=new Hl({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return iO({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Le(Ae.BootstrapApplicationEnd)}}function Z(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function Ei(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var C_=Symbol("NOT_SET"),Kx=new Set,fO=ne(C({},ol),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:C_,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==C_&&!Sa(this))return this.signal;try{for(let r of this.cleanup??Kx)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=ur(this),i;try{i=this.userFn.apply(null,n)}finally{Ur(this,e)}return(this.value===C_||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),I_=class extends Vl{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(yn),a),this.scheduler=r;for(let s of Ug){let l=e[s];if(l===void 0)continue;let c=Object.create(fO);c.sequence=this,c.phase=s,c.userFn=l,c.dirty=!0,c.signal=()=>(Hr(c),c.value),c.signal[Dt]=c,c.registerCleanupFn=f=>(c.cleanup??=new Set).add(f),this.nodes[s]=c,this.hooks[s]=f=>c.phaseFn(f)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??Kx)e()}finally{zr(n)}}};function Xx(t,n){let e=n?.injector??d(G),i=e.get(Wi),r=e.get(Zu),o=e.get(wi,null,{optional:!0});r.impl??=e.get(zg);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(Za,null,{optional:!0}),l=new I_(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function pm(t,n){let e=gr(t),i=n.elementInjector||Ba();return new ta(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var Jx=null;function ri(){return Jx}function S_(t){Jx??=t}var nc=class{},cs=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(ew),providedIn:"platform"})}return t})();var ew=(()=>{class t extends cs{_location;_history;_doc=d(W);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return ri().getBaseHref(this._doc)}onPopState(e){let i=ri().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=ri().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function iw(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function tw(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function ao(t){return t&&t[0]!=="?"?`?${t}`:t}var ds=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(hO),providedIn:"root"})}return t})(),pO=new y(""),hO=(()=>{class t extends ds{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(W).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return iw(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+ao(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+ao(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+ao(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(K(cs),K(pO,8))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var so=(()=>{class t{_subject=new E;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=vO(tw(nw(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+ao(i))}normalize(e){return t.stripTrailingSlash(_O(this._basePath,nw(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+ao(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+ao(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=ao;static joinWithSlash=iw;static stripTrailingSlash=tw;static \u0275fac=function(i){return new(i||t)(K(ds))};static \u0275prov=x({token:t,factory:()=>gO(),providedIn:"root"})}return t})();function gO(){return new so(K(ds))}function _O(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function nw(t){return t.replace(/\/index\.html$/,"")}function vO(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var hm=class{$implicit;ngForOf;index;count;constructor(n,e,i,r){this.$implicit=n,this.ngForOf=e,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},Ot=(()=>{class t{_viewContainer;_template;_differs;set ngForOf(e){this._ngForOf=e,this._ngForOfDirty=!0}set ngForTrackBy(e){this._trackByFn=e}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(e,i,r){this._viewContainer=e,this._template=i,this._differs=r}set ngForTemplate(e){e&&(this._template=e)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let e=this._ngForOf;!this._differ&&e&&(this._differ=this._differs.find(e).create(this.ngForTrackBy))}if(this._differ){let e=this._differ.diff(this._ngForOf);e&&this._applyChanges(e)}}_applyChanges(e){let i=this._viewContainer;e.forEachOperation((r,o,a)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new hm(r.item,this._ngForOf,-1,-1),a===null?void 0:a);else if(a==null)i.remove(o===null?void 0:o);else if(o!==null){let s=i.get(o);i.move(s,a),rw(s,r)}});for(let r=0,o=i.length;r<o;r++){let s=i.get(r).context;s.index=r,s.count=o,s.ngForOf=this._ngForOf}e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);rw(o,r)})}static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(M(ct),M(St),M(fm))};static \u0275dir=S({type:t,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return t})();function rw(t,n){t.context.$implicit=n.item}var Ut=(()=>{class t{_viewContainer;_context=new gm;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){ow(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){ow(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(M(ct),M(St))};static \u0275dir=S({type:t,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return t})(),gm=class{$implicit=null;ngIf=null};function ow(t,n){if(t&&!t.createEmbeddedView)throw new O(2020,!1)}var ic=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(G);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(M(ct))};static \u0275dir=S({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[je]})}return t})();function bO(t,n){return new O(2100,!1)}var T_=class{createSubscription(n,e,i){return Te(()=>n.subscribe({next:e,error:i}))}dispose(n){Te(()=>n.unsubscribe())}},A_=class{createSubscription(n,e,i){return n.then(r=>e?.(r),r=>i?.(r)),{unsubscribe:()=>{e=null,i=null}}}dispose(n){n.unsubscribe()}},yO=new A_,CO=new T_,rc=(()=>{class t{_ref;_latestValue=null;markForCheckOnValueUpdate=!0;_subscription=null;_obj=null;_strategy=null;applicationErrorHandler=d(Sn);constructor(e){this._ref=e}ngOnDestroy(){this._subscription&&this._dispose(),this._ref=null}transform(e){if(!this._obj){if(e)try{this.markForCheckOnValueUpdate=!1,this._subscribe(e)}finally{this.markForCheckOnValueUpdate=!0}return this._latestValue}return e!==this._obj?(this._dispose(),this.transform(e)):this._latestValue}_subscribe(e){this._obj=e,this._strategy=this._selectStrategy(e),this._subscription=this._strategy.createSubscription(e,i=>this._updateLatestValue(e,i),i=>this.applicationErrorHandler(i))}_selectStrategy(e){if(Mr(e))return yO;if(rm(e))return CO;throw bO(t,e)}_dispose(){this._strategy.dispose(this._subscription),this._latestValue=null,this._subscription=null,this._obj=null}_updateLatestValue(e,i){e===this._obj&&(this._latestValue=i,this.markForCheckOnValueUpdate&&this._ref?.markForCheck())}static \u0275fac=function(i){return new(i||t)(M(me,16))};static \u0275pipe=d_({name:"async",type:t,pure:!1})}return t})();var Rt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({})}return t})();function oc(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var aa=class{};var k_="browser";function aw(t){return t===k_}var ac=class{_doc;constructor(n){this._doc=n}manager},_m=(()=>{class t extends ac{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(K(W))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),ym=new y(""),F_=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof _m));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof _m);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new O(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(K(ym),K(P))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),O_="ng-app-id";function sw(t){for(let n of t)n.remove()}function lw(t,n){let e=n.createElement("style");return e.textContent=t,e}function xO(t,n,e,i){let r=t.head?.querySelectorAll(`style[${O_}="${n}"],link[${O_}="${n}"]`);if(r)for(let o of r)o.removeAttribute(O_),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function N_(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var P_=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,xO(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,lw);i?.forEach(r=>this.addUsage(r,this.external,N_))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(sw(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])sw(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,lw(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,N_(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(K(W),K(oo),K(ra,8),K(ia))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),R_={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},L_=/%COMP%/g;var dw="%COMP%",wO=`_nghost-${dw}`,EO=`_ngcontent-${dw}`,DO=!0,MO=new y("",{factory:()=>DO});function IO(t){return EO.replace(L_,t)}function SO(t){return wO.replace(L_,t)}function uw(t,n){return n.map(e=>e.replace(L_,t))}var j_=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,a,s,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.defaultRenderer=new sc(e,a,s,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof bm?r.applyToHost(e):r instanceof lc&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,f=this.removeStylesOnCompDestroy,g=this.tracingService;switch(i.encapsulation){case Ci.Emulated:o=new bm(l,c,i,this.appId,f,a,s,g);break;case Ci.ShadowDom:return new vm(l,e,i,a,s,this.nonce,g,c);case Ci.ExperimentalIsolatedShadowDom:return new vm(l,e,i,a,s,this.nonce,g);default:o=new lc(l,c,i,f,a,s,g);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(K(F_),K(P_),K(oo),K(MO),K(W),K(P),K(ra),K(wi,8))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),sc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(R_[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(cw(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(cw(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new O(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=R_[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=R_[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Qi.DashCase|Qi.Important)?n.style.setProperty(e,i,r&Qi.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Qi.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=ri().getGlobalEventTarget(this.doc,n),!n))throw new O(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function cw(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var vm=class extends sc{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,l){super(n,r,o,s),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=uw(i.id,c);for(let g of c){let _=document.createElement("style");a&&_.setAttribute("nonce",a),_.textContent=g,this.shadowRoot.appendChild(_)}let f=i.getExternalStyles?.();if(f)for(let g of f){let _=N_(g,r);a&&_.setAttribute("nonce",a),this.shadowRoot.appendChild(_)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},lc=class extends sc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,l){super(n,o,a,s),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?uw(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&es.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},bm=class extends lc{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,l){let c=r+"-"+i.id;super(n,e,i,o,a,s,l,c),this.contentAttr=IO(c),this.hostAttr=SO(c)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Cm=class t extends nc{supportsDOMEvents=!0;static makeCurrent(){S_(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=TO();return e==null?null:AO(e)}resetBaseElement(){cc=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return oc(document.cookie,n)}},cc=null;function TO(){return cc=cc||document.head.querySelector("base"),cc?cc.getAttribute("href"):null}function AO(t){return new URL(t,document.baseURI).pathname}var kO=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),mw=["alt","control","meta","shift"],OO={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},RO={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},fw=(()=>{class t extends ac{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>ri().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),mw.forEach(c=>{let f=i.indexOf(c);f>-1&&(i.splice(f,1),a+=c+".")}),a+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=a,l}static matchEventFullKeyCode(e,i){let r=OO[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),mw.forEach(a=>{if(a!==r){let s=RO[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(K(W))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})();async function V_(t,n,e){let i=C({rootComponent:t},NO(n,e));return Qx(i)}function NO(t,n){return{platformRef:n?.platformRef,appProviders:[...VO,...t?.providers??[]],platformProviders:jO}}function FO(){Cm.makeCurrent()}function PO(){return new vn}function LO(){return Sg(document),document}var jO=[{provide:ia,useValue:k_},{provide:$u,useValue:FO,multi:!0},{provide:W,useFactory:LO}];var VO=[{provide:Dl,useValue:"root"},{provide:vn,useFactory:PO},{provide:ym,useClass:_m,multi:!0},{provide:ym,useClass:fw,multi:!0},j_,P_,F_,{provide:Tt,useExisting:j_},{provide:aa,useClass:kO},[]];var lo=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init();for(let[e,i]of n.headers.entries())this.headers.set(e,i),this.normalizedNames.set(e,n.normalizedNames.get(e))}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=n.op==="a"?(this.headers.get(e)||[]).slice():[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(o===void 0)this.headers.delete(e),this.normalizedNames.delete(e);else{let a=Array.isArray(o)?o:[o],s=this.headers.get(e);if(!s)return;s=s.filter(l=>a.indexOf(l)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var wm=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Em=class{encodeKey(n){return pw(n)}encodeValue(n){return pw(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function BO(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[a,s]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(a)||[];l.push(s),e.set(a,l)}),e}var HO=/%(\d[a-f0-9])/gi,UO={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function pw(t){return encodeURIComponent(t).replace(HO,(n,e)=>UO[e]??n)}function xm(t){return`${t}`}var Ar=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Em,n.fromString){if(n.fromObject)throw new O(2805,!1);this.map=BO(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(xm):[xm(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){if(this.map===null&&(this.map=new Map),this.cloneFrom!==null){this.cloneFrom.init();for(let[n,e]of this.cloneFrom.map.entries())this.map.set(n,e);this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=n.op==="a"?(this.map.get(n.param)||[]).slice():[];e.push(xm(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=(this.map.get(n.param)||[]).slice(),r=i.indexOf(xm(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null}}};function zO(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function hw(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function gw(t){return typeof Blob<"u"&&t instanceof Blob}function _w(t){return typeof FormData<"u"&&t instanceof FormData}function $O(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var vw="Content-Type",bw="Accept",yw="text/plain",Cw="application/json",qO=`${Cw}, ${yw}, */*`,us=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(zO(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new O(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new lo,this.context??=new wm,!this.params)this.params=new Ar,this.urlWithParams=e;else{let a=this.params.toString();if(a.length===0)this.urlWithParams=e;else{let s=e.indexOf("?"),l=s===-1?"?":s<e.length-1?"&":"";this.urlWithParams=e+l+a}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||hw(this.body)||gw(this.body)||_w(this.body)||$O(this.body)?this.body:this.body instanceof Ar?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||_w(this.body)?null:gw(this.body)?this.body.type||null:hw(this.body)?null:typeof this.body=="string"?yw:this.body instanceof Ar?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?Cw:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,a=n.priority||this.priority,s=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,f=n.credentials||this.credentials,g=n.referrer??this.referrer,_=n.integrity||this.integrity,b=n.referrerPolicy||this.referrerPolicy,w=n.transferCache??this.transferCache,z=n.timeout??this.timeout,q=n.body!==void 0?n.body:this.body,X=n.withCredentials??this.withCredentials,Ze=n.reportProgress??this.reportProgress,Dn=n.headers||this.headers,qt=n.params||this.params,el=n.context??this.context;return n.setHeaders!==void 0&&(Dn=Object.keys(n.setHeaders).reduce((tl,Mo)=>tl.set(Mo,n.setHeaders[Mo]),Dn)),n.setParams&&(qt=Object.keys(n.setParams).reduce((tl,Mo)=>tl.set(Mo,n.setParams[Mo]),qt)),new t(e,i,q,{params:qt,headers:Dn,context:el,reportProgress:Ze,responseType:r,withCredentials:X,transferCache:w,keepalive:o,cache:s,priority:a,timeout:z,mode:l,redirect:c,credentials:f,referrer:g,integrity:_,referrerPolicy:b})}},sa=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(sa||{}),fs=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new lo,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Dm=class t extends fs{constructor(n={}){super(n)}type=sa.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},dc=class t extends fs{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=sa.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},ms=class extends fs{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},GO=200,WO=204;var YO=new y("");var ZO=/^\)\]\}',?\n/;var H_=(()=>{class t{xhrFactory;tracingService=d(wi,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new O(-2800,!1);let i=this.xhrFactory;return Q(null).pipe(We(()=>new ae(o=>{let a=i.build();if(a.open(e.method,e.urlWithParams),e.withCredentials&&(a.withCredentials=!0),e.headers.forEach((q,X)=>a.setRequestHeader(q,X.join(","))),e.headers.has(bw)||a.setRequestHeader(bw,qO),!e.headers.has(vw)){let q=e.detectContentTypeHeader();q!==null&&a.setRequestHeader(vw,q)}if(e.timeout&&(a.timeout=e.timeout),e.responseType){let q=e.responseType.toLowerCase();a.responseType=q!=="json"?q:"text"}let s=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let q=a.statusText||"OK",X=new lo(a.getAllResponseHeaders()),Ze=a.responseURL||e.url;return l=new Dm({headers:X,status:a.status,statusText:q,url:Ze}),l},f=this.maybePropagateTrace(()=>{let{headers:q,status:X,statusText:Ze,url:Dn}=c(),qt=null;X!==WO&&(qt=typeof a.response>"u"?a.responseText:a.response),X===0&&(X=qt?GO:0);let el=X>=200&&X<300;if(e.responseType==="json"&&typeof qt=="string"){let tl=qt;qt=qt.replace(ZO,"");try{qt=qt!==""?JSON.parse(qt):null}catch(Mo){qt=tl,el&&(el=!1,qt={error:Mo,text:qt})}}el?(o.next(new dc({body:qt,headers:q,status:X,statusText:Ze,url:Dn||void 0})),o.complete()):o.error(new ms({error:qt,headers:q,status:X,statusText:Ze,url:Dn||void 0}))}),g=this.maybePropagateTrace(q=>{let{url:X}=c(),Ze=new ms({error:q,status:a.status||0,statusText:a.statusText||"Unknown Error",url:X||void 0});o.error(Ze)}),_=g;e.timeout&&(_=this.maybePropagateTrace(q=>{let{url:X}=c(),Ze=new ms({error:new DOMException("Request timed out","TimeoutError"),status:a.status||0,statusText:a.statusText||"Request timeout",url:X||void 0});o.error(Ze)}));let b=!1,w=this.maybePropagateTrace(q=>{b||(o.next(c()),b=!0);let X={type:sa.DownloadProgress,loaded:q.loaded};q.lengthComputable&&(X.total=q.total),e.responseType==="text"&&a.responseText&&(X.partialText=a.responseText),o.next(X)}),z=this.maybePropagateTrace(q=>{let X={type:sa.UploadProgress,loaded:q.loaded};q.lengthComputable&&(X.total=q.total),o.next(X)});return a.addEventListener("load",f),a.addEventListener("error",g),a.addEventListener("timeout",_),a.addEventListener("abort",g),e.reportProgress&&(a.addEventListener("progress",w),s!==null&&a.upload&&a.upload.addEventListener("progress",z)),a.send(s),o.next({type:sa.Sent}),()=>{a.removeEventListener("error",g),a.removeEventListener("abort",g),a.removeEventListener("load",f),a.removeEventListener("timeout",_),e.reportProgress&&(a.removeEventListener("progress",w),s!==null&&a.upload&&a.upload.removeEventListener("progress",z)),a.readyState!==a.DONE&&a.abort()}})))}static \u0275fac=function(i){return new(i||t)(K(aa))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),QO=new y("",{factory:()=>!0}),KO="XSRF-TOKEN",XO=new y("",{factory:()=>KO}),JO="X-XSRF-TOKEN",eR=new y("",{factory:()=>JO}),tR=(()=>{class t{cookieName=d(XO);doc=d(W);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=oc(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),xw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=K(tR),r},providedIn:"root"})}return t})();function ww(t,n){if(!d(QO)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(cs).href,{origin:o}=new URL(r),{origin:a}=new URL(t.url,o);if(o!==a)return n(t)}catch{return n(t)}let e=d(xw).getToken(),i=d(eR);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}function nR(t,n){return n(t)}function iR(t,n,e){return(i,r)=>Zt(e,()=>n(i,o=>t(o,r)))}var Ew=new y("",{factory:()=>[ww]}),Dw=new y(""),Mw=new y("",{factory:()=>!0});var U_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=K(H_),r},providedIn:"root"})}return t})();var Mm=(()=>{class t{backend;injector;chain=null;pendingTasks=d(Nl);contributeToStability=d(Mw);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=this.injector.get(Im,null,{skipSelf:!0}),r=i!==null&&this.backend===i,o=this.injector.get(Dw,[],r?{self:!0}:void 0),a=Array.from(new Set([...this.injector.get(Ew),...o]));this.chain=a.reduceRight((s,l)=>iR(s,l,this.injector),nR)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Bo(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(K(U_),K(Ne))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Im=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=K(Mm),r},providedIn:"root"})}return t})();function B_(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var Je=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof us)o=e;else{let l;r.headers instanceof lo?l=r.headers:l=new lo(r.headers);let c;r.params&&(r.params instanceof Ar?c=r.params:c=new Ar({fromObject:r.params})),o=new us(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let a=Q(o).pipe(Vo(l=>this.handler.handle(l)));if(e instanceof us||r.observe==="events")return a;let s=a.pipe(oe(l=>l instanceof dc));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return s.pipe($(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new O(2806,!1);return l.body}));case"blob":return s.pipe($(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new O(2807,!1);return l.body}));case"text":return s.pipe($(l=>{if(l.body!==null&&typeof l.body!="string")throw new O(2808,!1);return l.body}));default:return s.pipe($(l=>l.body))}case"response":return s;default:throw new O(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Ar().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,B_(r,i))}post(e,i,r={}){return this.request("POST",e,B_(r,i))}put(e,i,r={}){return this.request("PUT",e,B_(r,i))}static \u0275fac=function(i){return new(i||t)(K(Im))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function z_(...t){let n=[Je,Mm,{provide:Im,useExisting:Mm},{provide:U_,useFactory:()=>d(YO,{optional:!0})??d(H_)},{provide:Ew,useValue:ww,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return vr(n)}var Iw=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(K(W))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var uc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=K(rR),r},providedIn:"root"})}return t})(),rR=(()=>{class t extends uc{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case Re.NONE:return i;case Re.HTML:return Er(i,"HTML")?xi(i):Fg(this._doc,String(i)).toString();case Re.STYLE:return Er(i,"Style")?xi(i):i;case Re.SCRIPT:if(Er(i,"Script"))return xi(i);throw new O(5200,!1);case Re.URL:return Er(i,"URL")?xi(i):Gl(String(i));case Re.RESOURCE_URL:if(Er(i,"ResourceURL"))return xi(i);throw new O(5201,!1);default:throw new O(5202,!1)}}bypassSecurityTrustHtml(e){return Ag(e)}bypassSecurityTrustStyle(e){return kg(e)}bypassSecurityTrustScript(e){return Og(e)}bypassSecurityTrustUrl(e){return Rg(e)}bypassSecurityTrustResourceUrl(e){return Ng(e)}static \u0275fac=function(i){return new(i||t)(K(W))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ve="primary",Mc=Symbol("RouteTitle"),Y_=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function ca(t){return new Y_(t)}function $_(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function Fw(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return $_(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),a=i.slice(r+1);if(o.length+a.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let s={};return!$_(o,t.slice(0,o.length),s)||!$_(a,t.slice(t.length-a.length),s)?null:{consumed:t,posParams:s}}function Nm(t){return new Promise((n,e)=>{t.pipe(fr()).subscribe({next:i=>n(i),error:i=>e(i)})})}function oR(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!ir(t[e],n[e]))return!1;return!0}function ir(t,n){let e=t?Z_(t):void 0,i=n?Z_(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!Pw(t[r],n[r]))return!1;return!0}function Z_(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function Pw(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function aR(t){return t.length>0?t[t.length-1]:null}function ma(t){return cl(t)?t:Mr(t)?Qe(Promise.resolve(t)):Q(t)}function Lw(t){return cl(t)?Nm(t):Promise.resolve(t)}var sR={exact:Vw,subset:Bw},jw={exact:lR,subset:cR,ignored:()=>!0},cv={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},_c={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function dv(t,n,e){let i=t instanceof Cn?t:n.parseUrl(t);return an(()=>Q_(n.lastSuccessfulNavigation()?.finalUrl??new Cn,i,C(C({},_c),e)))}function Q_(t,n,e){return sR[e.paths](t.root,n.root,e.matrixParams)&&jw[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function lR(t,n){return ir(t,n)}function Vw(t,n,e){if(!la(t.segments,n.segments)||!km(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!Vw(t.children[i],n.children[i],e))return!1;return!0}function cR(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>Pw(t[e],n[e]))}function Bw(t,n,e){return Hw(t,n,n.segments,e)}function Hw(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!la(r,e)||n.hasChildren()||!km(r,e,i))}else if(t.segments.length===e.length){if(!la(t.segments,e)||!km(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!Bw(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!la(t.segments,r)||!km(t.segments,r,i)||!t.children[ve]?!1:Hw(t.children[ve],n,o,i)}}function km(t,n,e){return n.every((i,r)=>jw[e](t[r].parameters,i.parameters))}var Cn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Fe([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=ca(this.queryParams),this._queryParamMap}toString(){return mR.serialize(this)}},Fe=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Om(this)}},co=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=ca(this.parameters),this._parameterMap}toString(){return zw(this)}};function dR(t,n){return la(t,n)&&t.every((e,i)=>ir(e.parameters,n[i].parameters))}function la(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function uR(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===ve&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==ve&&(e=e.concat(n(r,i)))}),e}var xs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>new uo,providedIn:"root"})}return t})(),uo=class{parse(n){let e=new X_(n);return new Cn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${mc(n.root,!0)}`,i=hR(n.queryParams),r=typeof n.fragment=="string"?`#${fR(n.fragment)}`:"";return`${e}${i}${r}`}},mR=new uo;function Om(t){return t.segments.map(n=>zw(n)).join("/")}function mc(t,n){if(!t.hasChildren())return Om(t);if(n){let e=t.children[ve]?mc(t.children[ve],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==ve&&i.push(`${r}:${mc(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=uR(t,(i,r)=>r===ve?[mc(t.children[ve],!1)]:[`${r}:${mc(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[ve]!=null?`${Om(t)}/${e[0]}`:`${Om(t)}/(${e.join("//")})`}}function Uw(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Tm(t){return Uw(t).replace(/%3B/gi,";")}function fR(t){return encodeURI(t)}function K_(t){return Uw(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Rm(t){return decodeURIComponent(t)}function Tw(t){return Rm(t.replace(/\+/g,"%20"))}function zw(t){return`${K_(t.path)}${pR(t.parameters)}`}function pR(t){return Object.entries(t).map(([n,e])=>`;${K_(n)}=${K_(e)}`).join("")}function hR(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${Tm(e)}=${Tm(r)}`).join("&"):`${Tm(e)}=${Tm(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var gR=/^[^\/()?;#]+/;function q_(t){let n=t.match(gR);return n?n[0]:""}var _R=/^[^\/()?;=#]+/;function vR(t){let n=t.match(_R);return n?n[0]:""}var bR=/^[^=?&#]+/;function yR(t){let n=t.match(bR);return n?n[0]:""}var CR=/^[^&#]+/;function xR(t){let n=t.match(CR);return n?n[0]:""}var X_=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Fe([],{}):new Fe([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new O(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[ve]=new Fe(e,i)),r}parseSegment(){let n=q_(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new O(4009,!1);return this.capture(n),new co(Rm(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=vR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=q_(this.remaining);r&&(i=r,this.capture(i))}n[Rm(e)]=Rm(i)}parseQueryParam(n){let e=yR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let a=xR(this.remaining);a&&(i=a,this.capture(i))}let r=Tw(e),o=Tw(i);if(n.hasOwnProperty(r)){let a=n[r];Array.isArray(a)||(a=[a],n[r]=a),a.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=q_(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new O(4010,!1);let a;r.indexOf(":")>-1?(a=r.slice(0,r.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=ve);let s=this.parseChildren(e+1);i[a??ve]=Object.keys(s).length===1&&s[ve]?s[ve]:new Fe([],s),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new O(4011,!1)}};function $w(t){return t.segments.length>0?new Fe([],{[ve]:t}):t}function qw(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=qw(r);if(i===ve&&o.segments.length===0&&o.hasChildren())for(let[a,s]of Object.entries(o.children))n[a]=s;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Fe(t.segments,n);return wR(e)}function wR(t){if(t.numberOfChildren===1&&t.children[ve]){let n=t.children[ve];return new Fe(t.segments.concat(n.segments),n.children)}return t}function mo(t){return t instanceof Cn}function Gw(t,n,e=null,i=null,r=new uo){let o=Ww(t);return Yw(o,n,e,i,r)}function Ww(t){let n;function e(o){let a={};for(let l of o.children){let c=e(l);a[l.outlet]=c}let s=new Fe(o.url,a);return o===t&&(n=s),s}let i=e(t.root),r=$w(i);return n??r}function Yw(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return G_(o,o,o,e,i,r);let a=ER(n);if(a.toRoot())return G_(o,o,new Fe([],{}),e,i,r);let s=DR(a,o,t),l=s.processChildren?pc(s.segmentGroup,s.index,a.commands):Qw(s.segmentGroup,s.index,a.commands);return G_(o,s.segmentGroup,l,e,i,r)}function Fm(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function vc(t){return typeof t=="object"&&t!=null&&t.outlets}function Aw(t,n,e){t||="\u0275";let i=new Cn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function G_(t,n,e,i,r,o){let a={};for(let[c,f]of Object.entries(i??{}))a[c]=Array.isArray(f)?f.map(g=>Aw(c,g,o)):Aw(c,f,o);let s;t===n?s=e:s=Zw(t,n,e);let l=$w(qw(s));return new Cn(l,a,r)}function Zw(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=Zw(o,n,e)}),new Fe(t.segments,i)}var Pm=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&Fm(i[0]))throw new O(4003,!1);let r=i.find(vc);if(r&&r!==aR(i))throw new O(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function ER(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new Pm(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,a)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let s={};return Object.entries(o.outlets).forEach(([l,c])=>{s[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:s}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:a===0?(o.split("/").forEach((s,l)=>{l==0&&s==="."||(l==0&&s===""?e=!0:s===".."?n++:s!=""&&r.push(s))}),r):[...r,o]},[]);return new Pm(e,n,i)}var hs=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function DR(t,n,e){if(t.isAbsolute)return new hs(n,!0,0);if(!e)return new hs(n,!1,NaN);if(e.parent===null)return new hs(e,!0,0);let i=Fm(t.commands[0])?0:1,r=e.segments.length-1+i;return MR(e,r,t.numberOfDoubleDots)}function MR(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new O(4005,!1);r=i.segments.length}return new hs(i,!1,r-o)}function IR(t){return vc(t[0])?t[0].outlets:{[ve]:t}}function Qw(t,n,e){if(t??=new Fe([],{}),t.segments.length===0&&t.hasChildren())return pc(t,n,e);let i=SR(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Fe(t.segments.slice(0,i.pathIndex),{});return o.children[ve]=new Fe(t.segments.slice(i.pathIndex),t.children),pc(o,0,r)}else return i.match&&r.length===0?new Fe(t.segments,{}):i.match&&!t.hasChildren()?J_(t,n,e):i.match?pc(t,0,r):J_(t,n,e)}function pc(t,n,e){if(e.length===0)return new Fe(t.segments,{});{let i=IR(e),r={};if(Object.keys(i).some(o=>o!==ve)&&t.children[ve]&&t.numberOfChildren===1&&t.children[ve].segments.length===0){let o=pc(t.children[ve],n,e);return new Fe(t.segments,o.children)}return Object.entries(i).forEach(([o,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(r[o]=Qw(t.children[o],n,a))}),Object.entries(t.children).forEach(([o,a])=>{i[o]===void 0&&(r[o]=a)}),new Fe(t.segments,r)}}function SR(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let a=t.segments[r],s=e[i];if(vc(s))break;let l=`${s}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!Ow(l,c,a))return o;i+=2}else{if(!Ow(l,{},a))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function J_(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(vc(o)){let l=TR(o.outlets);return new Fe(i,l)}if(r===0&&Fm(e[0])){let l=t.segments[n];i.push(new co(l.path,kw(e[0]))),r++;continue}let a=vc(o)?o.outlets[ve]:`${o}`,s=r<e.length-1?e[r+1]:null;a&&s&&Fm(s)?(i.push(new co(a,kw(s))),r+=2):(i.push(new co(a,{})),r++)}return new Fe(i,{})}function TR(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=J_(new Fe([],{}),0,i))}),n}function kw(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function Ow(t,n,e){return t==e.path&&ir(n,e.parameters)}var hc="imperative",Kt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Kt||{}),$n=class{id;url;constructor(n,e){this.id=n,this.url=e}},da=class extends $n{type=Kt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Mi=class extends $n{urlAfterRedirects;type=Kt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},un=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(un||{}),bc=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(bc||{}),oi=class extends $n{reason;code;type=Kt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function Kw(t){return t instanceof oi&&(t.code===un.Redirect||t.code===un.SupersededByNewNavigation)}var Or=class extends $n{reason;code;type=Kt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},ua=class extends $n{error;target;type=Kt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},yc=class extends $n{urlAfterRedirects;state;type=Kt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Lm=class extends $n{urlAfterRedirects;state;type=Kt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},jm=class extends $n{urlAfterRedirects;state;shouldActivate;type=Kt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Vm=class extends $n{urlAfterRedirects;state;type=Kt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Bm=class extends $n{urlAfterRedirects;state;type=Kt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Hm=class{route;type=Kt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Um=class{route;type=Kt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},zm=class{snapshot;type=Kt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},$m=class{snapshot;type=Kt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},qm=class{snapshot;type=Kt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Gm=class{snapshot;type=Kt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var _s=class{},Cc=class{},vs=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function AR(t){return!(t instanceof _s)&&!(t instanceof vs)&&!(t instanceof Cc)}var Wm=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new ws(this.rootInjector)}},ws=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Wm(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(K(Ne))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ym=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=ev(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=ev(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=tv(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return tv(n,this._root).map(e=>e.value)}};function ev(t,n){if(t===n.value)return n;for(let e of n.children){let i=ev(t,e);if(i)return i}return null}function tv(t,n){if(t===n.value)return[n];for(let e of n.children){let i=tv(t,e);if(i.length)return i.unshift(n),i}return[]}var zn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function ps(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var xc=class extends Ym{snapshot;constructor(n,e){super(n),this.snapshot=e,mv(this,n)}toString(){return this.snapshot.toString()}};function Xw(t,n){let e=kR(t,n),i=new Vt([new co("",{})]),r=new Vt({}),o=new Vt({}),a=new Vt({}),s=new Vt(""),l=new Ii(i,r,a,s,o,ve,t,e.root);return l.snapshot=e.root,new xc(new zn(l,[]),e)}function kR(t,n){let e={},i={},r={},a=new bs([],e,r,"",i,ve,t,null,{},n);return new wc("",new zn(a,[]))}var Ii=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,a,s,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=a,this.component=s,this._futureSnapshot=l,this.title=this.dataSubject?.pipe($(c=>c[Mc]))??Q(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe($(n=>ca(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe($(n=>ca(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function uv(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:C(C({},n.params),t.params),data:C(C({},n.data),t.data),resolve:C(C(C(C({},t.data),n.data),r?.data),t._resolvedData)}:i={params:C({},t.params),data:C({},t.data),resolve:C(C({},t.data),t._resolvedData??{})},r&&eE(r)&&(i.resolve[Mc]=r.title),i}var bs=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Mc]}constructor(n,e,i,r,o,a,s,l,c,f){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=a,this.component=s,this.routeConfig=l,this._resolve=c,this._environmentInjector=f}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=ca(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=ca(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},wc=class extends Ym{url;constructor(n,e){super(e),this.url=n,mv(this,e)}toString(){return Jw(this._root)}};function mv(t,n){n.value._routerState=t,n.children.forEach(e=>mv(t,e))}function Jw(t){let n=t.children.length>0?` { ${t.children.map(Jw).join(", ")} } `:"";return`${t.value}${n}`}function W_(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,ir(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),ir(n.params,e.params)||t.paramsSubject.next(e.params),oR(n.url,e.url)||t.urlSubject.next(e.url),ir(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function nv(t,n){let e=ir(t.params,n.params)&&dR(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||nv(t.parent,n.parent))}function eE(t){return typeof t.title=="string"||t.title===null}var tE=new y(""),fa=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=ve;activateEvents=new F;deactivateEvents=new F;attachEvents=new F;detachEvents=new F;routerOutletData=Yx();parentContexts=d(ws);location=d(ct);changeDetector=d(me);inputBinder=d(Xm,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new O(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new O(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new O(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new O(4013,!1);this._activatedRoute=e;let r=this.location,a=e.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,l=new iv(e,s,r.injector,this.routerOutletData);this.activated=r.createComponent(a,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[je]})}return t})(),iv=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Ii?this.route:n===ws?this.childContexts:n===tE?this.outletData:this.parent.get(n,e)}},Xm=new y("");var fv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&te(0,"router-outlet")},dependencies:[fa],encapsulation:2})}return t})();function pv(t){let n=t.children&&t.children.map(pv),e=n?ne(C({},t),{children:n}):C({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==ve&&(e.component=fv),e}function OR(t,n,e){let i=Ec(t,n._root,e?e._root:void 0);return new xc(i,n)}function Ec(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=RR(t,n,e);return new zn(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let a=o.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(s=>Ec(t,s)),a}}let i=NR(n.value),r=n.children.map(o=>Ec(t,o));return new zn(i,r)}}function RR(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return Ec(t,i,r);return Ec(t,i)})}function NR(t){return new Ii(new Vt(t.url),new Vt(t.params),new Vt(t.queryParams),new Vt(t.fragment),new Vt(t.data),t.outlet,t.component,t)}var ys=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},nE="ngNavigationCancelingError";function Zm(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=mo(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=iE(!1,un.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function iE(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[nE]=!0,e.cancellationCode=n,e}function FR(t){return rE(t)&&mo(t.url)}function rE(t){return!!t&&t[nE]}var rv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),W_(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=ps(e);n.children.forEach(o=>{let a=o.value.outlet;this.deactivateRoutes(o,r[a],i),delete r[a]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let a=i.getContext(r.outlet);a&&this.deactivateChildRoutes(n,e,a.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ps(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);if(i&&i.outlet){let a=i.outlet.detach(),s=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ps(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=ps(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Gm(o.value.snapshot))}),n.children.length&&this.forwardEvent(new $m(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(W_(r),r===o)if(r.component){let a=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,a.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let a=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let s=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),W_(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=r,a.outlet&&a.outlet.activateWith(r,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,i)}},Qm=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},gs=class{component;route;constructor(n,e){this.component=n,this.route=e}};function PR(t,n,e){let i=t._root,r=n?n._root:null;return fc(i,r,e,[i.value])}function LR(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Es(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Qp(t)?t:n.get(t):i}function fc(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=ps(n);return t.children.forEach(a=>{jR(a,o[a.value.outlet],e,i.concat([a.value]),r),delete o[a.value.outlet]}),Object.entries(o).forEach(([a,s])=>gc(s,e.getContext(a),r)),r}function jR(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,a=n?n.value:null,s=e?e.getContext(t.value.outlet):null;if(a&&o.routeConfig===a.routeConfig){let l=VR(a,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new Qm(i)):(o.data=a.data,o._resolvedData=a._resolvedData),o.component?fc(t,n,s?s.children:null,i,r):fc(t,n,e,i,r),l&&s&&s.outlet&&s.outlet.isActivated&&r.canDeactivateChecks.push(new gs(s.outlet.component,a))}else a&&gc(n,s,r),r.canActivateChecks.push(new Qm(i)),o.component?fc(t,null,s?s.children:null,i,r):fc(t,null,e,i,r);return r}function VR(t,n,e){if(typeof e=="function")return Zt(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!la(t.url,n.url);case"pathParamsOrQueryParamsChange":return!la(t.url,n.url)||!ir(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!nv(t,n)||!ir(t.queryParams,n.queryParams);default:return!nv(t,n)}}function gc(t,n,e){let i=ps(t),r=t.value;Object.entries(i).forEach(([o,a])=>{r.component?n?gc(a,n.children.getContext(o),e):gc(a,null,e):gc(a,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new gs(n.outlet.component,r)):e.canDeactivateChecks.push(new gs(null,r)):e.canDeactivateChecks.push(new gs(null,r))}function Ic(t){return typeof t=="function"}function BR(t){return typeof t=="boolean"}function HR(t){return t&&Ic(t.canLoad)}function UR(t){return t&&Ic(t.canActivate)}function zR(t){return t&&Ic(t.canActivateChild)}function $R(t){return t&&Ic(t.canDeactivate)}function qR(t){return t&&Ic(t.canMatch)}function oE(t){return t instanceof Lo||t?.name==="EmptyError"}var Am=Symbol("INITIAL_VALUE");function Cs(){return We(t=>dl(t.map(n=>n.pipe(ke(1),Ie(Am)))).pipe($(n=>{for(let e of n)if(e!==!0){if(e===Am)return Am;if(e===!1||GR(e))return e}return!0}),oe(n=>n!==Am),ke(1)))}function GR(t){return mo(t)||t instanceof ys}function aE(t){return t.aborted?Q(void 0).pipe(ke(1)):new ae(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function sE(t){return De(aE(t))}function WR(t){return Gt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?Q(ne(C({},n),{guardsResult:!0})):YR(o,e,i).pipe(Gt(a=>a&&BR(a)?ZR(e,r,t):Q(a)),$(a=>ne(C({},n),{guardsResult:a})))})}function YR(t,n,e){return Qe(t).pipe(Gt(i=>eN(i.component,i.route,e,n)),fr(i=>i!==!0,!0))}function ZR(t,n,e){return Qe(n).pipe(Vo(i=>qi(KR(i.route.parent,e),QR(i.route,e),JR(t,i.path),XR(t,i.route))),fr(i=>i!==!0,!0))}function QR(t,n){return t!==null&&n&&n(new qm(t)),Q(!0)}function KR(t,n){return t!==null&&n&&n(new zm(t)),Q(!0)}function XR(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return Q(!0);let i=e.map(r=>jn(()=>{let o=n._environmentInjector,a=Es(r,o),s=UR(a)?a.canActivate(n,t):Zt(o,()=>a(n,t));return ma(s).pipe(fr())}));return Q(i).pipe(Cs())}function JR(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>LR(o)).filter(o=>o!==null).map(o=>jn(()=>{let a=o.guards.map(s=>{let l=o.node._environmentInjector,c=Es(s,l),f=zR(c)?c.canActivateChild(e,t):Zt(l,()=>c(e,t));return ma(f).pipe(fr())});return Q(a).pipe(Cs())}));return Q(r).pipe(Cs())}function eN(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return Q(!0);let o=r.map(a=>{let s=n._environmentInjector,l=Es(a,s),c=$R(l)?l.canDeactivate(t,n,e,i):Zt(s,()=>l(t,n,e,i));return ma(c).pipe(fr())});return Q(o).pipe(Cs())}function tN(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return Q(!0);let a=o.map(s=>{let l=Es(s,t),c=HR(l)?l.canLoad(n,e):Zt(t,()=>l(n,e)),f=ma(c);return r?f.pipe(sE(r)):f});return Q(a).pipe(Cs(),lE(i))}function lE(t){return Ip(st(n=>{if(typeof n!="boolean")throw Zm(t,n)}),$(n=>n===!0))}function nN(t,n,e,i,r,o){let a=n.canMatch;if(!a||a.length===0)return Q(!0);let s=a.map(l=>{let c=Es(l,t),f=qR(c)?c.canMatch(n,e,r):Zt(t,()=>c(n,e,r));return ma(f).pipe(sE(o))});return Q(s).pipe(Cs(),lE(i))}var kr=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},Dc=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function iN(t){throw new O(4e3,!1)}function rN(t){throw iE(!1,un.GuardRejected)}var ov=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[ve])throw iN(`${n.redirectTo}`);r=r.children[ve]}}async applyRedirectCommands(n,e,i,r,o){let a=await oN(e,r,o);if(a instanceof Cn)throw new Dc(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,i);if(a[0]==="/")throw new Dc(s);return s}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new Cn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let s=o.substring(1);i[r]=e[s]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),a={};return Object.entries(e.children).forEach(([s,l])=>{a[s]=this.createSegmentGroup(n,l,i,r)}),new Fe(o,a)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new O(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function oN(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return Nm(ma(Zt(e,()=>i(n))))}function aN(t,n){return t.providers&&!t._injector&&(t._injector=Ql(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Di(t){return t.outlet||ve}function sN(t,n){let e=t.filter(i=>Di(i)===n);return e.push(...t.filter(i=>Di(i)!==n)),e}var av={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function cE(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function lN(t,n,e,i,r,o,a){let s=dE(t,n,e);if(!s.matched)return Q(s);let l=cE(o(s));return i=aN(n,i),nN(i,n,e,r,l,a).pipe($(c=>c===!0?s:C({},av)))}function dE(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?C({},av):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||Fw)(e,t,n);if(!r)return C({},av);let o={};Object.entries(r.posParams??{}).forEach(([s,l])=>{o[s]=l.path});let a=r.consumed.length>0?C(C({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:a,positionalParamSegments:r.posParams??{}}}function Rw(t,n,e,i,r){return e.length>0&&uN(t,e,i,r)?{segmentGroup:new Fe(n,dN(i,new Fe(e,t.children))),slicedSegments:[]}:e.length===0&&mN(t,e,i)?{segmentGroup:new Fe(t.segments,cN(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Fe(t.segments,t.children),slicedSegments:e}}function cN(t,n,e,i){let r={};for(let o of e)if(Jm(t,n,o)&&!i[Di(o)]){let a=new Fe([],{});r[Di(o)]=a}return C(C({},i),r)}function dN(t,n){let e={};e[ve]=n;for(let i of t)if(i.path===""&&Di(i)!==ve){let r=new Fe([],{});e[Di(i)]=r}return e}function uN(t,n,e,i){return e.some(r=>!Jm(t,n,r)||!(Di(r)!==ve)?!1:!(i!==void 0&&Di(r)===i))}function mN(t,n,e){return e.some(i=>Jm(t,n,i))}function Jm(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function fN(t,n,e){return n.length===0&&!t.children[e]}var sv=class{};async function pN(t,n,e,i,r,o,a="emptyOnly",s){return new lv(t,n,e,i,r,a,o,s).recognize()}var hN=31,lv=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,a,s,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=l,this.applyRedirects=new ov(this.urlSerializer,this.urlTree)}noMatchError(n){return new O(4002,`'${n.segmentGroup}'`)}async recognize(){let n=Rw(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new zn(i,e),o=new wc("",r),a=Gw(i,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(a),{state:o,tree:a}}async match(n){let e=new bs([],Object.freeze({}),Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),ve,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,ve,e),rootSnapshot:e}}catch(i){if(i instanceof Dc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof kr?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let a=await this.processSegment(n,e,i,i.segments,r,!0,o);return a instanceof zn?[a]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let a=[];for(let l of o){let c=i.children[l],f=sN(e,l),g=await this.processSegmentGroup(n,f,c,l,r);a.push(...g)}let s=uE(a);return gN(s),s}async processSegment(n,e,i,r,o,a,s){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,a,s)}catch(c){if(c instanceof kr||oE(c))continue;throw c}if(fN(i,r,o))return new sv;throw new kr(i)}async processSegmentAgainstRoute(n,e,i,r,o,a,s,l){if(Di(i)!==a&&(a===ve||!Jm(r,o,i)))throw new kr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,a,l);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,a,l);throw new kr(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,a,s){let{matched:l,parameters:c,consumedSegments:f,positionalParamSegments:g,remainingSegments:_}=dE(e,r,o);if(!l)throw new kr(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>hN&&(this.allowRedirects=!1));let b=this.createSnapshot(n,r,o,c,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let w=await this.applyRedirects.applyRedirectCommands(f,r.redirectTo,g,cE(b),n),z=await this.applyRedirects.lineralizeSegments(r,w);return this.processSegment(n,i,e,z.concat(_),a,!1,s)}createSnapshot(n,e,i,r,o){let a=new bs(i,r,Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,vN(e),Di(e),e.component??e._loadedComponent??null,e,bN(e),n),s=uv(a,o,this.paramsInheritanceStrategy);return a.params=Object.freeze(s.params),a.data=Object.freeze(s.data),a}async matchSegmentAgainstRoute(n,e,i,r,o,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=Dn=>this.createSnapshot(n,i,Dn.consumedSegments,Dn.parameters,a),l=await Nm(lN(e,i,r,n,this.urlSerializer,s,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new kr(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),f=i._loadedInjector??n,{parameters:g,consumedSegments:_,remainingSegments:b}=l,w=this.createSnapshot(n,i,_,g,a),{segmentGroup:z,slicedSegments:q}=Rw(e,_,b,c,o);if(q.length===0&&z.hasChildren()){let Dn=await this.processChildren(f,c,z,w);return new zn(w,Dn)}if(c.length===0&&q.length===0)return new zn(w,[]);let X=Di(i)===o,Ze=await this.processSegment(f,c,z,q,X?ve:o,!0,w);return new zn(w,Ze instanceof zn?[Ze]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Nm(tN(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw rN(e)}return{routes:[],injector:n}}};function gN(t){t.sort((n,e)=>n.value.outlet===ve?-1:e.value.outlet===ve?1:n.value.outlet.localeCompare(e.value.outlet))}function _N(t){let n=t.value.routeConfig;return n&&n.path===""}function uE(t){let n=[],e=new Set;for(let i of t){if(!_N(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=uE(i.children);n.push(new zn(i.value,r))}return n.filter(i=>!e.has(i))}function vN(t){return t.data||{}}function bN(t){return t.resolve||{}}function yN(t,n,e,i,r,o,a){return Gt(async s=>{let{state:l,tree:c}=await pN(t,n,e,i,s.extractedUrl,r,o,a);return ne(C({},s),{targetSnapshot:l,urlAfterRedirects:c})})}function CN(t){return Gt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return Q(n);let r=new Set(i.map(s=>s.route)),o=new Set;for(let s of r)if(!o.has(s))for(let l of mE(s))o.add(l);let a=0;return Qe(o).pipe(Vo(s=>r.has(s)?xN(s,e,t):(s.data=uv(s,s.parent,t).resolve,Q(void 0))),st(()=>a++),Hd(1),Gt(s=>a===o.size?Q(n):ot))})}function mE(t){let n=t.children.map(e=>mE(e)).flat();return[t,...n]}function xN(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!eE(i)&&(r[Mc]=i.title),jn(()=>(t.data=uv(t,t.parent,e).resolve,wN(r,t,n).pipe($(o=>(t._resolvedData=o,t.data=C(C({},t.data),o),null)))))}function wN(t,n,e){let i=Z_(t);if(i.length===0)return Q({});let r={};return Qe(i).pipe(Gt(o=>EN(t[o],n,e).pipe(fr(),st(a=>{if(a instanceof ys)throw Zm(new uo,a);r[o]=a}))),Hd(1),$(()=>r),Gr(o=>oE(o)?ot:ll(o)))}function EN(t,n,e){let i=n._environmentInjector,r=Es(t,i),o=r.resolve?r.resolve(n,e):Zt(i,()=>r(n,e));return ma(o)}function Nw(t){return We(n=>{let e=t(n);return e?Qe(e).pipe($(()=>n)):Q(n)})}var hv=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===ve);return i}getResolvedTitleForRoute(e){return e.data[Mc]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(fE),providedIn:"root"})}return t})(),fE=(()=>{class t extends hv{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(K(Iw))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ds=new y("",{factory:()=>({})}),Sc=new y(""),pE=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(b_);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await Lw(Zt(e,()=>i.loadComponent())),a=await _E(gE(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=a,a}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await hE(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function hE(t,n,e,i){let r=await Lw(Zt(e,()=>t.loadChildren())),o=await _E(gE(r)),a;o instanceof tm||Array.isArray(o)?a=o:a=await n.compileModuleAsync(o),i&&i(t);let s,l,c=!1,f;return Array.isArray(a)?(l=a,c=!0):(s=a.create(e).injector,f=a,l=s.get(Sc,[],{optional:!0,self:!0}).flat()),{routes:l.map(pv),injector:s,factory:f}}function DN(t){return t&&typeof t=="object"&&"default"in t}function gE(t){return DN(t)?t.default:t}async function _E(t){return t}var ef=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(MN),providedIn:"root"})}return t})(),MN=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),vE=new y("");var IN=()=>{},bE=new y(""),yE=(()=>{class t{currentNavigation=ee(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=ee(null);events=new E;transitionAbortWithErrorSubject=new E;configLoader=d(pE);environmentInjector=d(Ne);destroyRef=d(yn);urlSerializer=d(xs);rootContexts=d(ws);location=d(so);inputBindingEnabled=d(Xm,{optional:!0})!==null;titleStrategy=d(hv);options=d(Ds,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(ef);createViewTransition=d(vE,{optional:!0});navigationErrorHandler=d(bE,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Q(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new Hm(r)),i=r=>this.events.next(new Um(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Te(()=>{this.transitions?.next(ne(C({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Vt(null),this.transitions.pipe(oe(i=>i!==null),We(i=>{let r=!1,o=new AbortController,a=()=>!r&&this.currentTransition?.id===i.id;return Q(i).pipe(We(s=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",un.SupersededByNewNavigation),ot;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:s.id,initialUrl:s.rawUrl,extractedUrl:s.extractedUrl,targetBrowserUrl:typeof s.extras.browserUrl=="string"?this.urlSerializer.parse(s.extras.browserUrl):s.extras.browserUrl,trigger:s.source,extras:s.extras,previousNavigation:l?ne(C({},l),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:s.routesRecognizeHandler,beforeActivateHandler:s.beforeActivateHandler});let c=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=s.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!c&&f!=="reload")return this.events.next(new Or(s.id,this.urlSerializer.serialize(s.rawUrl),"",bc.IgnoredSameUrlNavigation)),s.resolve(!1),ot;if(this.urlHandlingStrategy.shouldProcessUrl(s.rawUrl))return Q(s).pipe(We(g=>(this.events.next(new da(g.id,this.urlSerializer.serialize(g.extractedUrl),g.source,g.restoredState)),g.id!==this.navigationId?ot:Promise.resolve(g))),yN(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),st(g=>{i.targetSnapshot=g.targetSnapshot,i.urlAfterRedirects=g.urlAfterRedirects,this.currentNavigation.update(_=>(_.finalUrl=g.urlAfterRedirects,_)),this.events.next(new Cc)}),We(g=>Qe(i.routesRecognizeHandler.deferredHandle??Q(void 0)).pipe($(()=>g))),st(()=>{let g=new yc(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(g)}));if(c&&this.urlHandlingStrategy.shouldProcessUrl(s.currentRawUrl)){let{id:g,extractedUrl:_,source:b,restoredState:w,extras:z}=s,q=new da(g,this.urlSerializer.serialize(_),b,w);this.events.next(q);let X=Xw(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=ne(C({},s),{targetSnapshot:X,urlAfterRedirects:_,extras:ne(C({},z),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Ze=>(Ze.finalUrl=_,Ze)),Q(i)}else return this.events.next(new Or(s.id,this.urlSerializer.serialize(s.extractedUrl),"",bc.IgnoredByUrlHandlingStrategy)),s.resolve(!1),ot}),$(s=>{let l=new Lm(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);return this.events.next(l),this.currentTransition=i=ne(C({},s),{guards:PR(s.targetSnapshot,s.currentSnapshot,this.rootContexts)}),i}),WR(s=>this.events.next(s)),We(s=>{if(i.guardsResult=s.guardsResult,s.guardsResult&&typeof s.guardsResult!="boolean")throw Zm(this.urlSerializer,s.guardsResult);let l=new jm(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot,!!s.guardsResult);if(this.events.next(l),!a())return ot;if(!s.guardsResult)return this.cancelNavigationTransition(s,"",un.GuardRejected),ot;if(s.guards.canActivateChecks.length===0)return Q(s);let c=new Vm(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);if(this.events.next(c),!a())return ot;let f=!1;return Q(s).pipe(CN(this.paramsInheritanceStrategy),st({next:()=>{f=!0;let g=new Bm(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(g)},complete:()=>{f||this.cancelNavigationTransition(s,"",un.NoDataFromResolver)}}))}),Nw(s=>{let l=f=>{let g=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let _=f._environmentInjector;g.push(this.configLoader.loadComponent(_,f.routeConfig).then(b=>{f.component=b}))}for(let _ of f.children)g.push(...l(_));return g},c=l(s.targetSnapshot.root);return c.length===0?Q(s):Qe(Promise.all(c).then(()=>s))}),Nw(()=>this.afterPreactivation()),We(()=>{let{currentSnapshot:s,targetSnapshot:l}=i,c=this.createViewTransition?.(this.environmentInjector,s.root,l.root);return c?Qe(c).pipe($(()=>i)):Q(i)}),ke(1),We(s=>{let l=OR(e.routeReuseStrategy,s.targetSnapshot,s.currentRouterState);this.currentTransition=i=s=ne(C({},s),{targetRouterState:l}),this.currentNavigation.update(f=>(f.targetRouterState=l,f)),this.events.next(new _s);let c=i.beforeActivateHandler.deferredHandle;return c?Qe(c.then(()=>s)):Q(s)}),st(s=>{new rv(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),a()&&(r=!0,this.currentNavigation.update(l=>(l.abort=IN,l)),this.lastSuccessfulNavigation.set(Te(this.currentNavigation)),this.events.next(new Mi(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects))),this.titleStrategy?.updateTitle(s.targetRouterState.snapshot),s.resolve(!0))}),De(aE(o.signal).pipe(oe(()=>!r&&!i.targetRouterState),st(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",un.Aborted)}))),st({complete:()=>{r=!0}}),De(this.transitionAbortWithErrorSubject.pipe(st(s=>{throw s}))),Bo(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",un.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Gr(s=>{if(r=!0,this.destroyed)return i.resolve(!1),ot;if(rE(s))this.events.next(new oi(i.id,this.urlSerializer.serialize(i.extractedUrl),s.message,s.cancellationCode)),FR(s)?this.events.next(new vs(s.url,s.navigationBehaviorOptions)):i.resolve(!1);else{let l=new ua(i.id,this.urlSerializer.serialize(i.extractedUrl),s,i.targetSnapshot??void 0);try{let c=Zt(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(c instanceof ys){let{message:f,cancellationCode:g}=Zm(this.urlSerializer,c);this.events.next(new oi(i.id,this.urlSerializer.serialize(i.extractedUrl),f,g)),this.events.next(new vs(c.redirectTo,c.navigationBehaviorOptions))}else throw this.events.next(l),s}catch(c){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(c)}}return ot}))}))}cancelNavigationTransition(e,i,r){let o=new oi(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Te(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function SN(t){return t!==hc}var CE=new y("");var xE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(TN),providedIn:"root"})}return t})(),Km=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},TN=(()=>{class t extends Km{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),tf=(()=>{class t{urlSerializer=d(xs);options=d(Ds,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(so);urlHandlingStrategy=d(ef);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Cn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,a=r??o;return a instanceof Cn?this.urlSerializer.serialize(a):a}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=Xw(null,d(Ne));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(AN),providedIn:"root"})}return t})(),AN=(()=>{class t extends tf{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof da?this.updateStateMemento():e instanceof Or?this.commitTransition(i):e instanceof yc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof _s?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof oi&&!Kw(e)?this.restoreHistory(i):e instanceof ua?this.restoreHistory(i,!0):e instanceof Mi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:a,state:s}=r;if(this.location.isCurrentPathEqualTo(e)||a){let l=this.browserPageId,c=C(C({},s),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=C(C({},s),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?C({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):C({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function gv(t,n){t.events.pipe(oe(e=>e instanceof Mi||e instanceof oi||e instanceof ua||e instanceof Or),$(e=>e instanceof Mi||e instanceof Or?0:(e instanceof oi?e.code===un.Redirect||e.code===un.SupersededByNewNavigation:!1)?2:1),oe(e=>e!==2),ke(1)).subscribe(()=>{n()})}var xn=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(im);stateManager=d(tf);options=d(Ds,{optional:!0})||{};pendingTasks=d(Cr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(yE);urlSerializer=d(xs);location=d(so);urlHandlingStrategy=d(ef);injector=d(Ne);_events=new E;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(xE);injectorCleanup=d(CE,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(Sc,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(Xm,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new de;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Te(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof oi&&i.code!==un.Redirect&&i.code!==un.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Mi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof vs){let a=i.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=C({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||SN(r.source)},a);this.scheduleNavigation(s,hc,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}AR(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),hc,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let a=r?.navigationId?r:null,s=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=ne(C({},o),{browserUrl:e})),r){let c=C({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(s);this.scheduleNavigation(l,i,a,o).catch(c=>{this.disposed||this.injector.get(Sn)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Te(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(pv),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:a,queryParamsHandling:s,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:a,f=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":f=C(C({},this.currentUrlTree.queryParams),o);break;case"preserve":f=this.currentUrlTree.queryParams;break;default:f=o||null}f!==null&&(f=this.removeEmptyProps(f));let g;try{let _=r?r.snapshot:this.routerState.snapshot.root;g=Ww(_)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),g=this.currentUrlTree.root}return Yw(g,e,f,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=mo(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,hc,null,i)}navigate(e,i={skipLocationChange:!1}){return kN(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(hr(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=C({},cv):i===!1?r=C({},_c):r=C(C({},_c),i),mo(e))return Q_(this.currentUrlTree,e,r);let o=this.parseUrl(e);return Q_(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,a){if(this.disposed)return Promise.resolve(!1);let s,l,c;a?(s=a.resolve,l=a.reject,c=a.promise):c=new Promise((g,_)=>{s=g,l=_});let f=this.pendingTasks.add();return gv(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(f))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:s,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function kN(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new O(4008,!1)}var RN=(()=>{class t{router=d(xn);stateManager=d(tf);fragment=ee("");queryParams=ee({});path=ee("");serializer=d(xs);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof Mi&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new Cn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),mn=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new tr("href"),{optional:!0});reactiveHref=y_(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Te(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return Te(this._target)}_target=ee(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return Te(this._queryParams)}_queryParams=ee(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return Te(this._fragment)}_fragment=ee(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return Te(this._queryParamsHandling)}_queryParamsHandling=ee(void 0);set state(e){this._state.set(e)}get state(){return Te(this._state)}_state=ee(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return Te(this._info)}_info=ee(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return Te(this._relativeTo)}_relativeTo=ee(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return Te(this._preserveFragment)}_preserveFragment=ee(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return Te(this._skipLocationChange)}_skipLocationChange=ee(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return Te(this._replaceUrl)}_replaceUrl=ee(!1);isAnchorElement;onChanges=new E;applicationErrorHandler=d(Sn);options=d(Ds,{optional:!0});reactiveRouterState=d(RN);constructor(e,i,r,o,a,s){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=a,this.locationStrategy=s;let l=a.nativeElement.tagName?.toLowerCase();this.isAnchorElement=l==="a"||l==="area"||!!(typeof customElements=="object"&&customElements.get(l)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=ee(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(mo(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,a){let s=this._urlTree();if(s===null||this.isAnchorElement&&(e!==0||i||r||o||a||typeof this.target=="string"&&this.target!="_self"))return!0;let l={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(s,l)?.catch(c=>{this.applicationErrorHandler(c)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=an(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:mo(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return Te(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(M(xn),M(Ii),$l("tabindex"),M(Se),M(N),M(ds))};static \u0275dir=S({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&D("click",function(a){return r.onClick(a.button,a.ctrlKey,a.shiftKey,a.altKey,a.metaKey)}),i&2&&J("href",r.reactiveHref(),Pg)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",Z],skipLocationChange:[2,"skipLocationChange","skipLocationChange",Z],replaceUrl:[2,"replaceUrl","replaceUrl",Z],routerLink:"routerLink"},features:[je]})}return t})(),_v=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new F;link=d(mn,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(a=>{a instanceof Mi&&this.update()})}ngAfterContentInit(){Q(this.links.changes,Q(null)).pipe(qr()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=Qe(e).pipe(qr()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=NN(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?C({},cv):C({},_c);return r=>{let o=r.urlTree;return o?Te(dv(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(M(xn),M(N),M(Se),M(me))};static \u0275dir=S({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&ut(o,mn,5),i&2){let a;H(a=U())&&(r.links=a)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[je]})}return t})();function NN(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var FN=new y("");function vv(t,...n){return vr([{provide:Sc,multi:!0,useValue:t},[],{provide:Ii,useFactory:PN},{provide:om,multi:!0,useFactory:LN},n.map(e=>e.\u0275providers)])}function PN(){return d(xn).routerState.root}function LN(){let t=d(G);return n=>{let e=t.get(Tn);if(n!==e.components[0])return;let i=t.get(xn),r=t.get(jN);t.get(VN)===1&&i.initialNavigation(),t.get(BN,null,{optional:!0})?.setUpPreloading(),t.get(FN,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var jN=new y("",{factory:()=>new E}),VN=new y("",{factory:()=>1});var BN=new y("");var AE=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(M(Se),M(N))};static \u0275dir=S({type:t})}return t})(),UN=(()=>{class t extends AE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,features:[pe]})}return t})(),As=new y("");var zN={provide:As,useExisting:Yt(()=>pt),multi:!0};function $N(){let t=ri()?ri().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var qN=new y(""),pt=(()=>{class t extends AE{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!$N())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(M(Se),M(N),M(qN,8))};static \u0275dir=S({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&D("input",function(a){return r._handleInput(a.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(a){return r._compositionEnd(a.target.value)})},standalone:!1,features:[we([zN]),pe]})}return t})();function xv(t){return t==null||wv(t)===0}function wv(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var po=new y(""),Pc=new y(""),GN=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,fe=class{static min(n){return WN(n)}static max(n){return YN(n)}static required(n){return kE(n)}static requiredTrue(n){return ZN(n)}static email(n){return OE(n)}static minLength(n){return RE(n)}static maxLength(n){return QN(n)}static pattern(n){return KN(n)}static nullValidator(n){return rf()}static compose(n){return VE(n)}static composeAsync(n){return BE(n)}};function WN(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function YN(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function kE(t){return xv(t.value)?{required:!0}:null}function ZN(t){return t.value===!0?null:{required:!0}}function OE(t){return xv(t.value)||GN.test(t.value)?null:{email:!0}}function RE(t){return n=>{let e=n.value?.length??wv(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function QN(t){return n=>{let e=n.value?.length??wv(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function KN(t){if(!t)return rf;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(xv(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function rf(t){return null}function NE(t){return t!=null}function FE(t){return Mr(t)?Qe(t):t}function PE(t){let n={};return t.forEach(e=>{n=e!=null?C(C({},n),e):n}),Object.keys(n).length===0?null:n}function LE(t,n){return n.map(e=>e(t))}function XN(t){return!t.validate}function jE(t){return t.map(n=>XN(n)?n:e=>n.validate(e))}function VE(t){if(!t)return null;let n=t.filter(NE);return n.length==0?null:function(e){return PE(LE(e,n))}}function Ev(t){return t!=null?VE(jE(t)):null}function BE(t){if(!t)return null;let n=t.filter(NE);return n.length==0?null:function(e){let i=LE(e,n).map(FE);return jo(i).pipe($(PE))}}function Dv(t){return t!=null?BE(jE(t)):null}function wE(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function HE(t){return t._rawValidators}function UE(t){return t._rawAsyncValidators}function bv(t){return t?Array.isArray(t)?t:[t]:[]}function of(t,n){return Array.isArray(t)?t.includes(n):t===n}function EE(t,n){let e=bv(n);return bv(t).forEach(r=>{of(e,r)||e.push(r)}),e}function DE(t,n){return bv(n).filter(e=>!of(t,e))}var af=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Ev(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Dv(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Rr=class extends af{name;get formDirective(){return null}get path(){return null}},qn=class extends af{_parent=null;name=null;valueAccessor=null},sf=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Nt=(()=>{class t extends sf{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(M(qn,2))};static \u0275dir=S({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&R("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[pe]})}return t})(),Xt=(()=>{class t extends sf{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(M(Rr,10))};static \u0275dir=S({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&R("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[pe]})}return t})();var Tc="VALID",nf="INVALID",Ms="PENDING",Ac="DISABLED",fo=class{},lf=class extends fo{value;source;constructor(n,e){super(),this.value=n,this.source=e}},Oc=class extends fo{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},Rc=class extends fo{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Is=class extends fo{status;source;constructor(n,e){super(),this.status=n,this.source=e}},cf=class extends fo{source;constructor(n){super(),this.source=n}},Nc=class extends fo{source;constructor(n){super(),this.source=n}};function Mv(t){return(ff(t)?t.validators:t)||null}function JN(t){return Array.isArray(t)?Ev(t):t||null}function Iv(t,n){return(ff(n)?n.asyncValidators:t)||null}function eF(t){return Array.isArray(t)?Dv(t):t||null}function ff(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function zE(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new O(1e3,"");if(!i[e])throw new O(1001,"")}function $E(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new O(-1002,"")})}var Ss=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Te(this.statusReactive)}set status(n){Te(()=>this.statusReactive.set(n))}_status=an(()=>this.statusReactive());statusReactive=ee(void 0);get valid(){return this.status===Tc}get invalid(){return this.status===nf}get pending(){return this.status===Ms}get disabled(){return this.status===Ac}get enabled(){return this.status!==Ac}errors;get pristine(){return Te(this.pristineReactive)}set pristine(n){Te(()=>this.pristineReactive.set(n))}_pristine=an(()=>this.pristineReactive());pristineReactive=ee(!0);get dirty(){return!this.pristine}get touched(){return Te(this.touchedReactive)}set touched(n){Te(()=>this.touchedReactive.set(n))}_touched=an(()=>this.touchedReactive());touchedReactive=ee(!1);get untouched(){return!this.touched}_events=new E;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(EE(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(EE(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(DE(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(DE(n,this._rawAsyncValidators))}hasValidator(n){return of(this._rawValidators,n)}hasAsyncValidator(n){return of(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(ne(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Rc(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new Rc(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(ne(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Oc(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new Oc(!0,i))}markAsPending(n={}){this.status=Ms;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Is(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(ne(C({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Ac,this.errors=null,this._forEachChild(r=>{r.disable(ne(C({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new lf(this.value,i)),this._events.next(new Is(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(ne(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Tc,this._forEachChild(i=>{i.enable(ne(C({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(ne(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Tc||this.status===Ms)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new lf(this.value,e)),this._events.next(new Is(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(ne(C({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Ac:Tc}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Ms,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=FE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Is(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new F,this.statusChanges=new F}_calculateStatus(){return this._allControlsDisabled()?Ac:this.errors?nf:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Ms)?Ms:this._anyControlsHaveStatus(nf)?nf:Tc}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new Oc(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new Rc(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){ff(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=JN(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=eF(this._rawAsyncValidators)}},Ts=class extends Ss{constructor(n,e,i){super(Mv(e),Iv(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){$E(this,!0,n),Object.keys(n).forEach(i=>{zE(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,ne(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Nc(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var yv=class extends Ts{};var ks=new y("",{factory:()=>pf}),pf="always";function qE(t,n){return[...n.path,t]}function Fc(t,n,e=pf){Sv(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),nF(t,n),rF(t,n),iF(t,n),tF(t,n)}function df(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),mf(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function uf(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function tF(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function Sv(t,n){let e=HE(t);n.validator!==null?t.setValidators(wE(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=UE(t);n.asyncValidator!==null?t.setAsyncValidators(wE(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();uf(n._rawValidators,r),uf(n._rawAsyncValidators,r)}function mf(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=HE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=UE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return uf(n._rawValidators,i),uf(n._rawAsyncValidators,i),e}function nF(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&GE(t,n)})}function iF(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&GE(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function GE(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function rF(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function WE(t,n){t==null,Sv(t,n)}function oF(t,n){return mf(t,n)}function Tv(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function aF(t){return Object.getPrototypeOf(t.constructor)===UN}function YE(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function Av(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===pt?e=o:aF(o)?i=o:r=o}),r||i||e||null}function sF(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var lF={provide:Rr,useExisting:Yt(()=>Nr)},kc=Promise.resolve(),Nr=(()=>{class t extends Rr{callSetDisabledState;get submitted(){return Te(this.submittedReactive)}_submitted=an(()=>this.submittedReactive());submittedReactive=ee(!1);_directives=new Set;form;ngSubmit=new F;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Ts({},Ev(e),Dv(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){kc.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),Fc(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){kc.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){kc.then(()=>{let i=this._findContainer(e.path),r=new Ts({});WE(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){kc.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){kc.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),YE(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new cf(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(M(po,10),M(Pc,10),M(ks,8))};static \u0275dir=S({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&D("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[we([lF]),pe]})}return t})();function ME(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function IE(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var pa=class extends Ss{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(Mv(e),Iv(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),ff(e)&&(e.nonNullable||e.initialValueIsDefault)&&(IE(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Nc(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){ME(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){ME(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){IE(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var cF=t=>t instanceof pa;var dF={provide:qn,useExisting:Yt(()=>Lc)},SE=Promise.resolve(),Lc=(()=>{class t extends qn{_changeDetectorRef;callSetDisabledState;control=new pa;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new F;constructor(e,i,r,o,a,s){super(),this._changeDetectorRef=a,this.callSetDisabledState=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=Av(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),Tv(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){Fc(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){SE.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&Z(i);SE.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?qE(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(M(Rr,9),M(po,10),M(Pc,10),M(As,10),M(me,8),M(ks,8))};static \u0275dir=S({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[we([dF]),pe,je]})}return t})();var Jt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var Cv=class extends Ss{constructor(n,e,i){super(Mv(e),Iv(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){$E(this,!1,n),n.forEach((i,r)=>{zE(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],ne(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Nc(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var uF=(()=>{class t extends Rr{callSetDisabledState;get submitted(){return Te(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=an(()=>this._submittedReactive());_submittedReactive=ee(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(mf(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return Fc(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){df(e.control||null,e,!1),sF(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,YE(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new cf(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(df(i||null,e),cF(r)&&(Fc(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);WE(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&oF(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){Sv(this.form,this),this._oldForm&&mf(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(M(po,10),M(Pc,10),M(ks,8))};static \u0275dir=S({type:t,features:[pe,je]})}return t})();var kv=new y(""),mF={provide:qn,useExisting:Yt(()=>jc)},jc=(()=>{class t extends qn{_ngModelWarningConfig;callSetDisabledState;viewModel;form;set isDisabled(e){}model;update=new F;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,a){super(),this._ngModelWarningConfig=o,this.callSetDisabledState=a,this._setValidators(e),this._setAsyncValidators(i),this.valueAccessor=Av(this,r)}ngOnChanges(e){if(this._isControlChanged(e)){let i=e.form.previousValue;i&&df(i,this,!1),Fc(this.form,this,this.callSetDisabledState),this.form.updateValueAndValidity({emitEvent:!1})}Tv(e,this.viewModel)&&(this.form.setValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.form&&df(this.form,this,!1)}get path(){return[]}get control(){return this.form}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_isControlChanged(e){return e.hasOwnProperty("form")}static \u0275fac=function(i){return new(i||t)(M(po,10),M(Pc,10),M(As,10),M(kv,8),M(ks,8))};static \u0275dir=S({type:t,selectors:[["","formControl",""]],inputs:{form:[0,"formControl","form"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},exportAs:["ngForm"],standalone:!1,features:[we([mF]),pe,je]})}return t})();var fF={provide:qn,useExisting:Yt(()=>wn)},wn=(()=>{class t extends qn{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new F;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,a){super(),this._ngModelWarningConfig=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=Av(this,o)}ngOnChanges(e){this._added||this._setUpControl(),Tv(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return qE(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||t)(M(Rr,13),M(po,10),M(Pc,10),M(As,10),M(kv,8))};static \u0275dir=S({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[we([fF]),pe,je]})}return t})();var pF={provide:Rr,useExisting:Yt(()=>zt)},zt=(()=>{class t extends uF{form=null;ngSubmit=new F;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&D("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[we([pF]),pe]})}return t})();function hF(t){return typeof t=="number"?t:parseInt(t,10)}var Ov=(()=>{class t{_validator=rf;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):rf,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,features:[je]})}return t})();var gF={provide:po,useExisting:Yt(()=>Vc),multi:!0};var Vc=(()=>{class t extends Ov{required;inputName="required";normalizeInput=Z;createValidator=e=>kE;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&J("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[we([gF]),pe]})}return t})();var _F={provide:po,useExisting:Yt(()=>Bc),multi:!0},Bc=(()=>{class t extends Ov{email;inputName="email";normalizeInput=Z;createValidator=e=>OE;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","email","","formControlName",""],["","email","","formControl",""],["","email","","ngModel",""]],inputs:{email:"email"},standalone:!1,features:[we([_F]),pe]})}return t})(),vF={provide:po,useExisting:Yt(()=>Rv),multi:!0},Rv=(()=>{class t extends Ov{minlength;inputName="minlength";normalizeInput=e=>hF(e);createValidator=e=>RE(e);static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","minlength","","formControlName",""],["","minlength","","formControl",""],["","minlength","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&J("minlength",r._enabled?r.minlength:null)},inputs:{minlength:"minlength"},standalone:!1,features:[we([vF]),pe]})}return t})();var ZE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({})}return t})();function TE(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var kn=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return TE(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new Ts(r,o)}record(e,i=null){let r=this._reduceControls(e);return new yv(r,i)}control(e,i,r){let o={};return this.useNonNullable?(TE(i)?o=i:(o.validators=i,o.asyncValidators=r),new pa(e,ne(C({},o),{nonNullable:!0}))):new pa(e,i,r)}array(e,i,r){let o=e.map(a=>this._createControl(a));return new Cv(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof pa)return e;if(e instanceof Ss)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var hf=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:ks,useValue:e.callSetDisabledState??pf}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[ZE]})}return t})(),fn=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:kv,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:ks,useValue:e.callSetDisabledState??pf}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[ZE]})}return t})();function ha(t){return t.buttons===0||t.detail===0}function ga(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var Nv;function QE(){if(Nv==null){let t=typeof document<"u"?document.head:null;Nv=!!(t&&(t.createShadowRoot||t.attachShadow))}return Nv}function Fv(t){if(QE()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function ho(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function $t(t){return t.composedPath?t.composedPath()[0]:t.target}var Pv;try{Pv=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Pv=!1}var Ee=(()=>{class t{_platformId=d(ia);isBrowser=this._platformId?aw(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Pv)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Hc;function KE(){if(Hc==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Hc=!0}))}finally{Hc=Hc||!1}return Hc}function Os(t){return KE()?t:!!t.capture}function pn(t,n=0){return XE(t)?Number(t):arguments.length===2?n:0}function XE(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function En(t){return t instanceof N?t.nativeElement:t}var JE=new y("cdk-input-modality-detector-options"),eD={ignoreKeys:[18,17,224,91,16]},tD=650,Lv={passive:!0,capture:!0},nD=(()=>{class t{_platform=d(Ee);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Vt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=$t(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<tD||(this._modality.next(ha(e)?"keyboard":"mouse"),this._mostRecentTarget=$t(e))};_onTouchstart=e=>{if(ga(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=$t(e)};constructor(){let e=d(P),i=d(W),r=d(JE,{optional:!0});if(this._options=C(C({},eD),r),this.modalityDetected=this._modality.pipe(ml(1)),this.modalityChanged=this.modalityDetected.pipe(Pa()),this._platform.isBrowser){let o=d(Tt).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Lv),o.listen(i,"mousedown",this._onMousedown,Lv),o.listen(i,"touchstart",this._onTouchstart,Lv)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Uc=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Uc||{}),iD=new y("cdk-focus-monitor-default-options"),gf=Os({passive:!0,capture:!0}),On=(()=>{class t{_ngZone=d(P);_platform=d(Ee);_inputModalityDetector=d(nD);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(W);_stopInputModalityDetector=new E;constructor(){let e=d(iD,{optional:!0});this._detectionMode=e?.detectionMode||Uc.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=$t(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=En(e);if(!this._platform.isBrowser||r.nodeType!==1)return Q();let o=Fv(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new E,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=En(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=En(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Uc.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Uc.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?tD:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=$t(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,gf),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,gf)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(De(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,gf),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,gf),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _f=new WeakMap,ht=(()=>{class t{_appRef;_injector=d(G);_environmentInjector=d(Ne);load(e){let i=this._appRef=this._appRef||this._injector.get(Tn),r=_f.get(i);r||(r={loaders:new Set,refs:[]},_f.set(i,r),i.onDestroy(()=>{_f.get(i)?.refs.forEach(o=>o.destroy()),_f.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(pm(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Rs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})(),vf;function bF(){if(vf===void 0&&(vf=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(vf=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return vf}function _a(t){return bF()?.createHTML(t)||t}function rD(t,n,e){let i=e.sanitize(Re.HTML,n);t.innerHTML=_a(i||"")}function Fr(t){return Array.isArray(t)?t:[t]}var oD=new Set,va,Ns=(()=>{class t{_platform=d(Ee);_nonce=d(ra,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):CF}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&yF(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function yF(t,n){if(!oD.has(t))try{va||(va=document.createElement("style"),n&&va.setAttribute("nonce",n),va.setAttribute("type","text/css"),document.head.appendChild(va)),va.sheet&&(va.sheet.insertRule(`@media ${t} {body{ }}`,0),oD.add(t))}catch(e){console.error(e)}}function CF(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var zc=(()=>{class t{_mediaMatcher=d(Ns);_zone=d(P);_queries=new Map;_destroySubject=new E;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return aD(Fr(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=aD(Fr(e)).map(a=>this._registerQuery(a).observable),o=dl(r);return o=qi(o.pipe(ke(1)),o.pipe(ml(1),ui(0))),o.pipe($(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ae(a=>{let s=l=>this._zone.run(()=>a.next(l));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(Ie(i),$(({matches:a})=>({query:e,matches:a})),De(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function aD(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}function xF(t){if(t.type==="characterData"&&t.target instanceof Comment)return!0;if(t.type==="childList"){for(let n=0;n<t.addedNodes.length;n++)if(!(t.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<t.removedNodes.length;n++)if(!(t.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var sD=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),lD=(()=>{class t{_mutationObserverFactory=d(sD);_observedElements=new Map;_ngZone=d(P);constructor(){}ngOnDestroy(){this._observedElements.forEach((e,i)=>this._cleanupObserver(i))}observe(e){let i=En(e);return new ae(r=>{let a=this._observeElement(i).pipe($(s=>s.filter(l=>!xF(l))),oe(s=>!!s.length)).subscribe(s=>{this._ngZone.run(()=>{r.next(s)})});return()=>{a.unsubscribe(),this._unobserveElement(i)}})}_observeElement(e){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(e))this._observedElements.get(e).count++;else{let i=new E,r=this._mutationObserverFactory.create(o=>i.next(o));r&&r.observe(e,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(e,{observer:r,stream:i,count:1})}return this._observedElements.get(e).stream})}_unobserveElement(e){this._observedElements.has(e)&&(this._observedElements.get(e).count--,this._observedElements.get(e).count||this._cleanupObserver(e))}_cleanupObserver(e){if(this._observedElements.has(e)){let{observer:i,stream:r}=this._observedElements.get(e);i&&i.disconnect(),r.complete(),this._observedElements.delete(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),cD=(()=>{class t{_contentObserver=d(lD);_elementRef=d(N);event=new F;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(e){this._debounce=pn(e),this._subscribe()}_debounce;_currentSubscription=null;constructor(){}ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let e=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?e.pipe(ui(this.debounce)):e).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",Z],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return t})(),Fs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({providers:[sD]})}return t})();var Ps=(()=>{class t{_platform=d(Ee);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return EF(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=wF(OF(e));if(i&&(dD(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=dD(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!AF(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return kF(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function wF(t){try{return t.frameElement}catch{return null}}function EF(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function DF(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function MF(t){return SF(t)&&t.type=="hidden"}function IF(t){return TF(t)&&t.hasAttribute("href")}function SF(t){return t.nodeName.toLowerCase()=="input"}function TF(t){return t.nodeName.toLowerCase()=="a"}function fD(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function dD(t){if(!fD(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function AF(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function kF(t){return MF(t)?!1:DF(t)||IF(t)||t.hasAttribute("contenteditable")||fD(t)}function OF(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var bf=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?ft(n,{injector:this._injector}):setTimeout(n)}},$c=(()=>{class t{_checker=d(Ps);_ngZone=d(P);_document=d(W);_injector=d(G);constructor(){d(ht).load(Rs)}create(e,i=!1){return new bf(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var pD=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),hD=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),RF=0,Vv=(()=>{class t{_ngZone=d(P);_defaultOptions=d(hD,{optional:!0});_liveElement;_document=d(W);_sanitizer=d(uc);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(pD,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,a;return i.length===1&&typeof i[0]=="number"?a=i[0]:[o,a]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),a==null&&r&&(a=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:rD(this._liveElement,e,this._sanitizer),typeof a=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),a)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${RF++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var go=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(go||{}),uD="cdk-high-contrast-black-on-white",mD="cdk-high-contrast-white-on-black",jv="cdk-high-contrast-active",gD=(()=>{class t{_platform=d(Ee);_hasCheckedHighContrastMode=!1;_document=d(W);_breakpointSubscription;constructor(){this._breakpointSubscription=d(zc).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return go.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return go.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return go.BLACK_ON_WHITE}return go.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(jv,uD,mD),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===go.BLACK_ON_WHITE?e.add(jv,uD):i===go.WHITE_ON_BLACK&&e.add(jv,mD)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),qc=(()=>{class t{constructor(){d(gD)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Fs]})}return t})();var NF=200,yf=class{_letterKeyStream=new E;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new E;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:NF;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(st(e=>this._pressedLetters.push(e)),ui(n),oe(()=>this._pressedLetters.length>0),$(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function Ye(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Ls=class{_items;_activeItemIndex=ee(-1);_activeItem=ee(null);_wrap=!1;_typeaheadSubscription=de.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Bn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Xi(n)&&(this._effectRef=xr(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new E;change=new E;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new yf(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,a=this._getItemsArray().length;this._setActiveItemByIndex(o<a?o:a-1,-1);break}else return;default:(r||Ye(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Xi(this._items)?this._items():this._items instanceof Bn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var ya=class extends Ls{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Ca=class extends Ls{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var Bv={},ze=class t{_appId=d(oo);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),Bv.hasOwnProperty(n)||(Bv[n]=0),`${n}${e?t._infix+"-":""}${Bv[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};var yD=" ";function js(t,n,e){let i=xf(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(yD)))}function vo(t,n,e){let i=xf(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(yD)):t.removeAttribute(n)}function xf(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var CD="cdk-describedby-message",Cf="cdk-describedby-host",Uv=0,xD=(()=>{class t{_platform=d(Ee);_document=d(W);_messageRegistry=new Map;_messagesContainer=null;_id=`${Uv++}`;constructor(){d(ht).load(Rs),this._id=d(oo)+"-"+Uv++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=Hv(i,r);typeof i!="string"?(bD(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=Hv(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let a=this._messageRegistry.get(o);a&&a.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${Cf}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(Cf);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");bD(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(Hv(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=xf(e,"aria-describedby").filter(r=>r.indexOf(CD)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);js(e,"aria-describedby",r.messageElement.id),e.setAttribute(Cf,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,vo(e,"aria-describedby",r.messageElement.id),e.removeAttribute(Cf)}_isElementDescribedByMessage(e,i){let r=xf(e,"aria-describedby"),o=this._messageRegistry.get(i),a=o&&o.messageElement.id;return!!a&&r.indexOf(a)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Hv(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function bD(t,n){t.id||(t.id=`${CD}-${n}-${Uv++}`)}var Si=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(Si||{}),wf,xa;function Ef(){if(xa==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return xa=!1,xa;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)xa=!0;else{let t=Element.prototype.scrollTo;t?xa=!/\{\s*\[native code\]\s*\}/.test(t.toString()):xa=!1}}return xa}function Vs(){if(typeof document!="object"||!document)return Si.NORMAL;if(wf==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),wf=Si.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,wf=t.scrollLeft===0?Si.NEGATED:Si.INVERTED),t.remove()}return wf}function zv(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Bs,wD=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function $v(){if(Bs)return Bs;if(typeof document!="object"||!document)return Bs=new Set(wD),Bs;let t=document.createElement("input");return Bs=new Set(wD.filter(n=>(t.setAttribute("type",n),t.type===n))),Bs}var ED={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var FF=new y("MATERIAL_ANIMATIONS"),DD=null;function qv(){return d(FF,{optional:!0})?.animationsDisabled||d(ql,{optional:!0})==="NoopAnimations"?"di-disabled":(DD??=d(Ns).matchMedia("(prefers-reduced-motion)").matches,DD?"reduced-motion":"enabled")}function He(){return qv()!=="enabled"}function Mt(t){return t==null?"":typeof t=="string"?t:`${t}px`}function Xe(t){return t!=null&&`${t}`!="false"}var ai=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(ai||{}),Gv=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=ai.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},MD=Os({passive:!0,capture:!0}),Wv=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,MD)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,MD)))}_delegateEventHandler=n=>{let e=$t(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Yc={enterDuration:225,exitDuration:150},PF=800,ID=Os({passive:!0,capture:!0}),SD=["mousedown","touchstart"],TD=["mouseup","mouseleave","touchend","touchcancel"],LF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),wa=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Wv;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=En(i)),o&&o.get(ht).load(LF)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=C(C({},Yc),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||jF(n,e,r),s=n-r.left,l=e-r.top,c=o.enterDuration,f=document.createElement("div");f.classList.add("mat-ripple-element"),f.style.left=`${s-a}px`,f.style.top=`${l-a}px`,f.style.height=`${a*2}px`,f.style.width=`${a*2}px`,i.color!=null&&(f.style.backgroundColor=i.color),f.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(f);let g=window.getComputedStyle(f),_=g.transitionProperty,b=g.transitionDuration,w=_==="none"||b==="0s"||b==="0s, 0s"||r.width===0&&r.height===0,z=new Gv(this,f,i,w);f.style.transform="scale3d(1, 1, 1)",z.state=ai.FADING_IN,i.persistent||(this._mostRecentTransientRipple=z);let q=null;return!w&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let X=()=>{q&&(q.fallbackTimer=null),clearTimeout(Dn),this._finishRippleTransition(z)},Ze=()=>this._destroyRipple(z),Dn=setTimeout(Ze,c+100);f.addEventListener("transitionend",X),f.addEventListener("transitioncancel",Ze),q={onTransitionEnd:X,onTransitionCancel:Ze,fallbackTimer:Dn}}),this._activeRipples.set(z,q),(w||!c)&&this._finishRippleTransition(z),z}fadeOutRipple(n){if(n.state===ai.FADING_OUT||n.state===ai.HIDDEN)return;let e=n.element,i=C(C({},Yc),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=ai.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=En(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,SD.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{TD.forEach(e=>{this._triggerElement.addEventListener(e,this,ID)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===ai.FADING_IN?this._startFadeOutTransition(n):n.state===ai.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=ai.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=ai.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=ha(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+PF;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!ga(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===ai.VISIBLE||n.config.terminateOnPointerUp&&n.state===ai.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(SD.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(TD.forEach(e=>n.removeEventListener(e,this,ID)),this._pointerUpEventsRegistered=!1))}};function jF(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Zc=new y("mat-ripple-global-options"),Df=(()=>{class t{_elementRef=d(N);_animationsDisabled=He();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(P),i=d(Ee),r=d(Zc,{optional:!0}),o=d(G);this._globalOptions=r||{},this._rippleRenderer=new wa(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:C(C(C({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,C(C({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,C(C({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var AD=(()=>{class t{_animationsDisabled=He();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&R("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return t})();var or=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var VF=["text"],BF=[[["mat-icon"]],"*"],HF=["mat-icon","*"];function UF(t,n){if(t&1&&te(0,"mat-pseudo-checkbox",1),t&2){let e=I();v("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function zF(t,n){if(t&1&&te(0,"mat-pseudo-checkbox",3),t&2){let e=I();v("disabled",e.disabled)}}function $F(t,n){if(t&1&&(m(0,"span",4),p(1),u()),t&2){let e=I();h(),Y("(",e.group.label,")")}}var Kc=new y("MAT_OPTION_PARENT_COMPONENT"),Xc=new y("MatOptgroup");var Qc=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},Rn=(()=>{class t{_element=d(N);_changeDetectorRef=d(me);_parent=d(Kc,{optional:!0});group=d(Xc,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(ze).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=ee(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new F;_text;_stateChanges=new E;constructor(){let e=d(ht);e.load(or),e.load(Rs),this._signalDisableRipple=!!this._parent&&Xi(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!Ye(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new Qc(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&qe(VF,7),i&2){let o;H(o=U())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&D("click",function(){return r._selectViaInteraction()})("keydown",function(a){return r._handleKeydown(a)}),i&2&&(dn("id",r.id),J("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),R("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",Z]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:HF,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Me(BF),ge(0,UF,1,2,"mat-pseudo-checkbox",1),V(1),m(2,"span",2,0),V(4,1),u(),ge(5,zF,1,1,"mat-pseudo-checkbox",3),ge(6,$F,2,1,"span",4),te(7,"div",5)),i&2&&(_e(r.multiple?0:-1),h(5),_e(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),h(),_e(r.group&&r.group._inert?6:-1),h(),v("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[AD,Df],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})();function Mf(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let a=0;a<t+1;a++)i[a].group&&i[a].group===r[o]&&o++;return o}return 0}function If(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var qF=new y("cdk-dir-doc",{providedIn:"root",factory:()=>d(W)}),GF=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function kD(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?GF.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var It=(()=>{class t{get value(){return this.valueSignal()}valueSignal=ee("ltr");change=new F;constructor(){let e=d(qF,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(kD(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ce=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({})}return t})();var WF=20,sr=(()=>{class t{_ngZone=d(P);_platform=d(Ee);_renderer=d(Tt).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new E;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=WF){return this._platform.isBrowser?new ae(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Bd(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Q()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(oe(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=En(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Pr=(()=>{class t{elementRef=d(N);scrollDispatcher=d(sr);ngZone=d(P);dir=d(It,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new E;_renderer=d(Se);_cleanupScroll;_elementScrolled=new E;constructor(){}ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&Vs()!=Si.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),Vs()==Si.INVERTED?e.left=e.right:Vs()==Si.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;Ef()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let a=this.dir&&this.dir.value=="rtl";return e=="start"?e=a?r:i:e=="end"&&(e=a?i:r),a&&Vs()==Si.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:a&&Vs()==Si.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),YF=20,Gn=(()=>{class t{_platform=d(Ee);_listeners;_viewportSize=null;_change=new E;_document=d(W);constructor(){let e=d(P),i=d(Tt).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=YF){return e>0?this._change.pipe(Bd(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Nn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({})}return t})(),Yv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Ce,Nn,Ce,Nn]})}return t})();var Jc=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},yo=class extends Jc{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},Wn=class extends Jc{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Zv=class extends Jc{element;constructor(n){super(),this.element=n instanceof N?n.nativeElement:n}},Hs=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof yo)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Wn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Zv)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},ed=class extends Hs{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Ki,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||G.NULL,o=r.get(Ne,i.injector);e=pm(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var Ea=(()=>{class t extends Hs{_moduleRef=d(Ki,{optional:!0});_document=d(W);_viewContainerRef=d(ct);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new F;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[pe]})}return t})(),Lr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({})}return t})();var OD=Ef();function $s(t){return new Sf(t.get(Gn),t.get(W))}var Sf=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Mt(-this._previousScrollPosition.left),n.style.top=Mt(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),OD&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),OD&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function VD(t,n){return new Tf(t.get(sr),t.get(P),t.get(Gn),n)}var Tf=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(oe(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var td=class{enable(){}disable(){}attach(){}};function Qv(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,a=t.left>e.right;return i||r||o||a})}function RD(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,a=t.right>e.right;return i||r||o||a})}function Ai(t,n){return new Af(t.get(sr),t.get(Gn),t.get(P),n)}var Af=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Qv(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},BD=(()=>{class t{_injector=d(G);constructor(){}noop=()=>new td;close=e=>VD(this._injector,e);block=()=>$s(this._injector);reposition=e=>Ai(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ti=class{positionStrategy;scrollStrategy=new td;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var kf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var HD=(()=>{class t{_attachedOverlays=[];_document=d(W);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),UD=(()=>{class t extends HD{_ngZone=d(P);_renderer=d(Tt).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),zD=(()=>{class t extends HD{_platform=d(Ee);_ngZone=d(P);_renderer=d(Tt).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=$t(e)};_clickListener=e=>{let i=$t(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if(ND(s.overlayElement,i)||ND(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ND(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var $D=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Nf=(()=>{class t{_platform=d(Ee);_containerElement;_document=d(W);_styleLoader=d(ht);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||zv()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),zv()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load($D)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Kv=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Xv(t){return t&&t.nodeType===1}var Us=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new E;_attachments=new E;_detachments=new E;_positionStrategy;_scrollStrategy;_locationChanges=de.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new E;_outsidePointerEvents=new E;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,l,c,f=!1,g,_){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=f,this._injector=g,this._renderer=_,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=ft(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=C(C({},this._config),n),this._updateElementSize()}setDirection(n){this._config=ne(C({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Mt(this._config.width),n.height=Mt(this._config.height),n.minWidth=Mt(this._config.minWidth),n.minHeight=Mt(this._config.minHeight),n.maxWidth=Mt(this._config.maxWidth),n.maxHeight=Mt(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Xv(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Kv(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Fr(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=ft(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},FD="cdk-overlay-connected-position-bounding-box",QF=/([A-Za-z%]+)$/;function jr(t,n){return new Of(n,t.get(Gn),t.get(W),t.get(Ee),t.get(Nf))}var Of=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new E;_resizeSubscription=de.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(FD),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],a;for(let s of this._preferredPositions){let l=this._getOriginPoint(n,r,s),c=this._getOverlayPoint(l,e,s),f=this._getOverlayFit(c,e,i,s);if(f.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,l);return}if(this._canFitWithFlexibleDimensions(f,c,i)){o.push({position:s,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,s)});continue}(!a||a.overlayFit.visibleArea<f.visibleArea)&&(a={overlayFit:f,overlayPoint:c,originPoint:l,position:s,overlayRect:e})}if(o.length){let s=null,l=-1;for(let c of o){let f=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);f>l&&(l=f,s=c)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(a.position,a.originPoint);return}this._applyPosition(a.position,a.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Da(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(FD),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof N?this._origin.nativeElement:Xv(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let a=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;r=i.originX=="start"?a:s}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=LD(e),{x:a,y:s}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(a+=l),c&&(s+=c);let f=0-a,g=a+o.width-i.width,_=0-s,b=s+o.height-i.height,w=this._subtractOverflows(o.width,f,g),z=this._subtractOverflows(o.height,_,b),q=w*z;return{visibleArea:q,isCompletelyWithinViewport:o.width*o.height===q,fitsInViewportVertically:z===o.height,fitsInViewportHorizontally:w==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,a=PD(this._overlayRef.getConfig().minHeight),s=PD(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||a!=null&&a<=r,c=n.fitsInViewportHorizontally||s!=null&&s<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=LD(e),o=this._viewportRect,a=Math.max(n.x+r.width-o.width,0),s=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),f=0,g=0;return r.width<=o.width?f=c||-a:f=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?g=l||-s:g=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:f,y:g},{x:n.x+f,y:n.y+g}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!KF(this._lastScrollVisibility,i)){let r=new kf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,a,s;if(e.overlayY==="top")a=n.y,o=i.height-a+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-s+this._getViewportMarginTop();else{let b=Math.min(i.bottom-n.y+i.top,n.y),w=this._lastBoundingBoxSize.height;o=b*2,a=n.y-b,o>w&&!this._isInitialRender&&!this._growAfterOpen&&(a=n.y-w/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,f,g,_;if(c)_=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),f=n.x-this._getViewportMarginStart();else if(l)g=n.x,f=i.right-n.x-this._getViewportMarginEnd();else{let b=Math.min(i.right-n.x+i.left,n.x),w=this._lastBoundingBoxSize.width;f=b*2,g=n.x-b,f>w&&!this._isInitialRender&&!this._growAfterOpen&&(g=n.x-w/2)}return{top:a,left:g,bottom:s,right:_,width:f,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,a=this._overlayRef.getConfig().maxWidth;r.width=Mt(i.width),r.height=Mt(i.height),r.top=Mt(i.top)||"auto",r.bottom=Mt(i.bottom)||"auto",r.left=Mt(i.left)||"auto",r.right=Mt(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Mt(o)),a&&(r.maxWidth=Mt(a))}this._lastBoundingBoxSize=i,Da(this._boundingBox.style,r)}_resetBoundingBoxStyles(){Da(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Da(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,a=this._overlayRef.getConfig();if(r){let f=this._viewportRuler.getViewportScrollPosition();Da(i,this._getExactOverlayY(e,n,f)),Da(i,this._getExactOverlayX(e,n,f))}else i.position="static";let s="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(s+=`translateX(${l}px) `),c&&(s+=`translateY(${c}px)`),i.transform=s.trim(),a.maxHeight&&(r?i.maxHeight=Mt(a.maxHeight):o&&(i.maxHeight="")),a.maxWidth&&(r?i.maxWidth=Mt(a.maxWidth):o&&(i.maxWidth="")),Da(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let a=this._document.documentElement.clientHeight;r.bottom=`${a-(o.y+this._overlayRect.height)}px`}else r.top=Mt(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let a;if(this._isRtl()?a=n.overlayX==="end"?"left":"right":a=n.overlayX==="end"?"right":"left",a==="right"){let s=this._document.documentElement.clientWidth;r.right=`${s-(o.x+this._overlayRect.width)}px`}else r.left=Mt(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:RD(n,i),isOriginOutsideView:Qv(n,i),isOverlayClipped:RD(e,i),isOverlayOutsideView:Qv(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Fr(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof N)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function Da(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function PD(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(QF);return!e||e==="px"?parseFloat(n):null}return t||null}function LD(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function KF(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var jD="cdk-global-overlay-wrapper";function qs(t){return new Rf}var Rf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(jD),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,l=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),f=this._xPosition,g=this._xOffset,_=this._overlayRef.getConfig().direction==="rtl",b="",w="",z="";l?z="flex-start":f==="center"?(z="center",_?w=g:b=g):_?f==="left"||f==="end"?(z="flex-end",b=g):(f==="right"||f==="start")&&(z="flex-start",w=g):f==="left"||f==="start"?(z="flex-start",b=g):(f==="right"||f==="end")&&(z="flex-end",w=g),n.position=this._cssPosition,n.marginLeft=l?"0":b,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":w,e.justifyContent=z,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(jD),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},qD=(()=>{class t{_injector=d(G);constructor(){}global(){return qs()}flexibleConnectedTo(e){return jr(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),nd=new y("OVERLAY_DEFAULT_CONFIG");function ki(t,n){t.get(ht).load($D);let e=t.get(Nf),i=t.get(W),r=t.get(ze),o=t.get(Tn),a=t.get(It),s=t.get(Se,null,{optional:!0})||t.get(Tt).createRenderer(null,null),l=new Ti(n),c=t.get(nd,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,"showPopover"in i.body?l.usePopover=n?.usePopover??c:l.usePopover=!1;let f=i.createElement("div"),g=i.createElement("div");f.id=r.getId("cdk-overlay-"),f.classList.add("cdk-overlay-pane"),g.appendChild(f),l.usePopover&&(g.setAttribute("popover","manual"),g.classList.add("cdk-overlay-popover"));let _=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return Xv(_)?_.after(g):_?.type==="parent"?_.element.appendChild(g):e.getContainerElement().appendChild(g),new Us(new ed(f,o,t),g,f,l,t.get(P),t.get(UD),i,t.get(so),t.get(zD),n?.disableAnimations??t.get(ql,null,{optional:!0})==="NoopAnimations",t.get(Ne),s)}var GD=(()=>{class t{scrollStrategies=d(BD);_positionBuilder=d(qD);_injector=d(G);constructor(){}create(e){return ki(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),XF=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],JF=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(G);return()=>Ai(t)}}),zs=(()=>{class t{elementRef=d(N);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),WD=new y("cdk-connected-overlay-default-config"),Ff=(()=>{class t{_dir=d(It,{optional:!0});_injector=d(G);_overlayRef;_templatePortal;_backdropSubscription=de.EMPTY;_attachSubscription=de.EMPTY;_detachSubscription=de.EMPTY;_positionSubscription=de.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(JF);_ngZone=d(P);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new F;positionChange=new F;attach=new F;detach=new F;overlayKeydown=new F;overlayOutsideClick=new F;constructor(){let e=d(St),i=d(ct),r=d(WD,{optional:!0}),o=d(nd,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Wn(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=XF);let e=this._overlayRef=ki(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!Ye(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=$t(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Ti({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=jr(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof zs?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof zs?this.origin.elementRef.nativeElement:this.origin instanceof N?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Np(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",Z],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",Z],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",Z],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",Z],push:[2,"cdkConnectedOverlayPush","push",Z],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",Z],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",Z],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[je]})}return t})(),Yn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({providers:[GD],imports:[Ce,Lr,Yv,Yv]})}return t})();var Jv=class{_box;_destroyed=new E;_resizeSubject=new E;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ae(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(oe(e=>e.some(i=>i.target===n)),zd({bufferSize:1,refCount:!0}),De(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},YD=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(P);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Jv(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var eP=["notch"],tP=["matFormFieldNotchedOutline",""],nP=["*"],ZD=["iconPrefixContainer"],QD=["textPrefixContainer"],KD=["iconSuffixContainer"],XD=["textSuffixContainer"],iP=["textField"],rP=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],oP=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function aP(t,n){t&1&&te(0,"span",21)}function sP(t,n){if(t&1&&(m(0,"label",20),V(1,1),ge(2,aP,1,0,"span",21),u()),t&2){let e=I(2);v("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),J("for",e._control.disableAutomaticLabeling?null:e._control.id),h(2),_e(!e.hideRequiredMarker&&e._control.required?2:-1)}}function lP(t,n){if(t&1&&ge(0,sP,3,5,"label",20),t&2){let e=I();_e(e._hasFloatingLabel()?0:-1)}}function cP(t,n){t&1&&te(0,"div",7)}function dP(t,n){}function uP(t,n){if(t&1&&A(0,dP,0,0,"ng-template",13),t&2){I(2);let e=Be(1);v("ngTemplateOutlet",e)}}function mP(t,n){if(t&1&&(m(0,"div",9),ge(1,uP,1,1,null,13),u()),t&2){let e=I();v("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),h(),_e(e._forceDisplayInfixLabel()?-1:1)}}function fP(t,n){t&1&&(m(0,"div",10,2),V(2,2),u())}function pP(t,n){t&1&&(m(0,"div",11,3),V(2,3),u())}function hP(t,n){}function gP(t,n){if(t&1&&A(0,hP,0,0,"ng-template",13),t&2){I();let e=Be(1);v("ngTemplateOutlet",e)}}function _P(t,n){t&1&&(m(0,"div",14,4),V(2,4),u())}function vP(t,n){t&1&&(m(0,"div",15,5),V(2,5),u())}function bP(t,n){t&1&&te(0,"div",16)}function yP(t,n){t&1&&(m(0,"div",18),V(1,6),u())}function CP(t,n){if(t&1&&(m(0,"mat-hint",22),p(1),u()),t&2){let e=I(2);v("id",e._hintLabelId),h(),B(e.hintLabel)}}function xP(t,n){if(t&1&&(m(0,"div",19),ge(1,CP,2,2,"mat-hint",22),V(2,7),te(3,"div",23),V(4,8),u()),t&2){let e=I();h(),_e(e.hintLabel?1:-1)}}var et=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-label"]]})}return t})(),oM=new y("MatError"),Ft=(()=>{class t{id=d(ze).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&dn("id",r.id)},inputs:{id:"id"},features:[we([{provide:oM,useExisting:t}])]})}return t})(),Lf=(()=>{class t{align="start";id=d(ze).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(dn("id",r.id),J("align",null),R("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),wP=new y("MatPrefix");var aM=new y("MatSuffix"),hn=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[we([{provide:aM,useExisting:t}])]})}return t})(),sM=new y("FloatingLabelParent"),JD=(()=>{class t{_elementRef=d(N);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(YD);_ngZone=d(P);_parent=d(sM);_resizeSubscription=new de;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return EP(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&R("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function EP(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var eM="mdc-line-ripple--active",Pf="mdc-line-ripple--deactivating",tM=(()=>{class t{_elementRef=d(N);_cleanupTransitionEnd;constructor(){let e=d(P),i=d(Se);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Pf),e.add(eM)}deactivate(){this._elementRef.nativeElement.classList.add(Pf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Pf);e.propertyName==="opacity"&&r&&i.remove(eM,Pf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),nM=(()=>{class t{_elementRef=d(N);_ngZone=d(P);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&qe(eP,5),i&2){let o;H(o=U())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&R("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:tP,ngContentSelectors:nP,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Me(),An(0,"div",1),dt(1,"div",2,0),V(3),At(),An(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),id=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t})}return t})();var Ma=new y("MatFormField"),DP=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),iM="fill",MP="auto",rM="fixed",IP="translateY(-50%)",mt=(()=>{class t{_elementRef=d(N);_changeDetectorRef=d(me);_platform=d(Ee);_idGenerator=d(ze);_ngZone=d(P);_defaults=d(DP,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=tc("iconPrefixContainer");_textPrefixContainerSignal=tc("textPrefixContainer");_iconSuffixContainerSignal=tc("iconSuffixContainer");_textSuffixContainerSignal=tc("textSuffixContainer");_prefixSuffixContainers=an(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Zx(et);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Xe(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||MP}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||iM;this._appearanceSignal.set(i)}_appearanceSignal=ee(iM);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||rM}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||rM}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new E;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=He();constructor(){let e=this._defaults,i=d(It);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),xr(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=an(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(Ie([void 0,void 0]),$(()=>[i.errorState,i.userAriaDescribedBy]),Ud(),oe(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(De(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),at(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){Xx({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=an(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,f=this._currentDirection==="rtl"?"-1":"1",g=`${a+s}px`,b=`calc(${f} * (${g} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,w=`var(--mat-mdc-form-field-label-transform, ${IP} translateX(${b}))`,z=a+s+l+c;return[w,z]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(am(o,r._labelChild,et,5),ut(o,id,5)(o,wP,5)(o,aM,5)(o,oM,5)(o,Lf,5)),i&2){lm();let a;H(a=U())&&(r._formFieldControl=a.first),H(a=U())&&(r._prefixChildren=a),H(a=U())&&(r._suffixChildren=a),H(a=U())&&(r._errorChildren=a),H(a=U())&&(r._hintChildren=a)}},viewQuery:function(i,r){if(i&1&&(sm(r._iconPrefixContainerSignal,ZD,5)(r._textPrefixContainerSignal,QD,5)(r._iconSuffixContainerSignal,KD,5)(r._textSuffixContainerSignal,XD,5),qe(iP,5)(ZD,5)(QD,5)(KD,5)(XD,5)(JD,5)(nM,5)(tM,5)),i&2){lm(4);let o;H(o=U())&&(r._textField=o.first),H(o=U())&&(r._iconPrefixContainer=o.first),H(o=U())&&(r._textPrefixContainer=o.first),H(o=U())&&(r._iconSuffixContainer=o.first),H(o=U())&&(r._textSuffixContainer=o.first),H(o=U())&&(r._floatingLabel=o.first),H(o=U())&&(r._notchedOutline=o.first),H(o=U())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&R("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[we([{provide:Ma,useExisting:t},{provide:sM,useExisting:t}])],ngContentSelectors:oP,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Me(rP),A(0,lP,1,1,"ng-template",null,0,ls),m(2,"div",6,1),D("click",function(a){return r._control.onContainerClick(a)}),ge(4,cP,1,0,"div",7),m(5,"div",8),ge(6,mP,2,2,"div",9),ge(7,fP,3,0,"div",10),ge(8,pP,3,0,"div",11),m(9,"div",12),ge(10,gP,1,1,null,13),V(11),u(),ge(12,_P,3,0,"div",14),ge(13,vP,3,0,"div",15),u(),ge(14,bP,1,0,"div",16),u(),m(15,"div",17),ge(16,yP,2,0,"div",18)(17,xP,5,1,"div",19),u()),i&2){let o;h(2),R("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),h(2),_e(!r._hasOutline()&&!r._control.disabled?4:-1),h(2),_e(r._hasOutline()?6:-1),h(),_e(r._hasIconPrefix?7:-1),h(),_e(r._hasTextPrefix?8:-1),h(2),_e(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),h(2),_e(r._hasTextSuffix?12:-1),h(),_e(r._hasIconSuffix?13:-1),h(),_e(r._hasOutline()?-1:14),h(),R("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let a=r._getSubscriptMessageType();h(),_e((o=a)==="error"?16:o==="hint"?17:-1)}},dependencies:[JD,nM,ic,tM,Lf],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return t})();var Co=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Ce]})}return t})();var jf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Ce]})}return t})();var Gs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Co,jf,Rn,Ce]})}return t})();var SP=["panel"],TP=["*"];function AP(t,n){if(t&1&&(dt(0,"div",1,0),V(2),At()),t&2){let e=n.id,i=I();on(i._classList),R("mat-mdc-autocomplete-visible",i.showPanel)("mat-mdc-autocomplete-hidden",!i.showPanel)("mat-autocomplete-panel-animations-enabled",!i._animationsDisabled)("mat-primary",i._color==="primary")("mat-accent",i._color==="accent")("mat-warn",i._color==="warn"),dn("id",i.id),J("aria-label",i.ariaLabel||null)("aria-labelledby",i._getPanelAriaLabelledby(e))}}var eb=class{source;option;constructor(n,e){this.source=n,this.option=e}},lM=new y("mat-autocomplete-default-options",{providedIn:"root",factory:()=>({autoActiveFirstOption:!1,autoSelectActiveOption:!1,hideSingleSelectionIndicator:!1,requireSelection:!1,hasBackdrop:!1})}),Vf=(()=>{class t{_changeDetectorRef=d(me);_elementRef=d(N);_defaults=d(lM);_animationsDisabled=He();_activeOptionChanges=de.EMPTY;_keyManager;showPanel=!1;get isOpen(){return this._isOpen&&this.showPanel}_isOpen=!1;_latestOpeningTrigger;_setColor(e){this._color=e,this._changeDetectorRef.markForCheck()}_color;template;panel;options;optionGroups;ariaLabel;ariaLabelledby;displayWith=null;autoActiveFirstOption;autoSelectActiveOption;requireSelection;panelWidth;disableRipple=!1;optionSelected=new F;opened=new F;closed=new F;optionActivated=new F;set classList(e){this._classList=e,this._elementRef.nativeElement.className=""}_classList;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator;_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}id=d(ze).getId("mat-autocomplete-");inertGroups;constructor(){let e=d(Ee);this.inertGroups=e?.SAFARI||!1,this.autoActiveFirstOption=!!this._defaults.autoActiveFirstOption,this.autoSelectActiveOption=!!this._defaults.autoSelectActiveOption,this.requireSelection=!!this._defaults.requireSelection,this._hideSingleSelectionIndicator=this._defaults.hideSingleSelectionIndicator??!1}ngAfterContentInit(){this._keyManager=new ya(this.options).withWrap().skipPredicate(this._skipPredicate),this._activeOptionChanges=this._keyManager.change.subscribe(e=>{this.isOpen&&this.optionActivated.emit({source:this,option:this.options.toArray()[e]||null})}),this._setVisibility()}ngOnDestroy(){this._keyManager?.destroy(),this._activeOptionChanges.unsubscribe()}_setScrollTop(e){this.panel&&(this.panel.nativeElement.scrollTop=e)}_getScrollTop(){return this.panel?this.panel.nativeElement.scrollTop:0}_setVisibility(){this.showPanel=!!this.options?.length,this._changeDetectorRef.markForCheck()}_emitSelectEvent(e){let i=new eb(this,e);this.optionSelected.emit(i)}_getPanelAriaLabelledby(e){if(this.ariaLabel)return null;let i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_skipPredicate(){return!1}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-autocomplete"]],contentQueries:function(i,r,o){if(i&1&&ut(o,Rn,5)(o,Xc,5),i&2){let a;H(a=U())&&(r.options=a),H(a=U())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&qe(St,7)(SP,5),i&2){let o;H(o=U())&&(r.template=o.first),H(o=U())&&(r.panel=o.first)}},hostAttrs:[1,"mat-mdc-autocomplete"],inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],displayWith:"displayWith",autoActiveFirstOption:[2,"autoActiveFirstOption","autoActiveFirstOption",Z],autoSelectActiveOption:[2,"autoSelectActiveOption","autoSelectActiveOption",Z],requireSelection:[2,"requireSelection","requireSelection",Z],panelWidth:"panelWidth",disableRipple:[2,"disableRipple","disableRipple",Z],classList:[0,"class","classList"],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",Z]},outputs:{optionSelected:"optionSelected",opened:"opened",closed:"closed",optionActivated:"optionActivated"},exportAs:["matAutocomplete"],features:[we([{provide:Kc,useExisting:t}])],ngContentSelectors:TP,decls:1,vars:0,consts:[["panel",""],["role","listbox",1,"mat-mdc-autocomplete-panel","mdc-menu-surface","mdc-menu-surface--open",3,"id"]],template:function(i,r){i&1&&(Me(),rs(0,AP,3,17,"ng-template"))},styles:[`div.mat-mdc-autocomplete-panel {
  width: 100%;
  max-height: 256px;
  visibility: hidden;
  transform-origin: center top;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  position: relative;
  border-radius: var(--mat-autocomplete-container-shape, var(--mat-sys-corner-extra-small));
  box-shadow: var(--mat-autocomplete-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  background-color: var(--mat-autocomplete-background-color, var(--mat-sys-surface-container));
}
@media (forced-colors: active) {
  div.mat-mdc-autocomplete-panel {
    outline: solid 1px;
  }
}
.cdk-overlay-pane:not(.mat-mdc-autocomplete-panel-above) div.mat-mdc-autocomplete-panel {
  border-top-left-radius: 0;
  border-top-right-radius: 0;
}
.mat-mdc-autocomplete-panel-above div.mat-mdc-autocomplete-panel {
  border-bottom-left-radius: 0;
  border-bottom-right-radius: 0;
  transform-origin: center bottom;
}
div.mat-mdc-autocomplete-panel.mat-mdc-autocomplete-visible {
  visibility: visible;
}

div.mat-mdc-autocomplete-panel.mat-mdc-autocomplete-hidden,
.cdk-overlay-pane:has(> .mat-mdc-autocomplete-hidden) {
  visibility: hidden;
  pointer-events: none;
}

@keyframes _mat-autocomplete-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
.mat-autocomplete-panel-animations-enabled {
  animation: _mat-autocomplete-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}

mat-autocomplete {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return t})();var kP={provide:As,useExisting:Yt(()=>rd),multi:!0};var OP=new y("mat-autocomplete-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(G);return()=>Ai(t)}}),rd=(()=>{class t{_environmentInjector=d(Ne);_element=d(N);_injector=d(G);_viewContainerRef=d(ct);_zone=d(P);_changeDetectorRef=d(me);_dir=d(It,{optional:!0});_formField=d(Ma,{optional:!0,host:!0});_viewportRuler=d(Gn);_scrollStrategy=d(OP);_renderer=d(Se);_animationsDisabled=He();_defaults=d(lM,{optional:!0});_overlayRef=null;_portal;_componentDestroyed=!1;_initialized=new E;_keydownSubscription;_outsideClickSubscription;_cleanupWindowBlur;_previousValue=null;_valueOnAttach=null;_valueOnLastKeydown=null;_positionStrategy;_manuallyFloatingLabel=!1;_closingActionsSubscription;_viewportSubscription=de.EMPTY;_breakpointObserver=d(zc);_handsetLandscapeSubscription=de.EMPTY;_canOpenOnNextFocus=!0;_valueBeforeAutoSelection;_pendingAutoselectedOption=null;_closeKeyEventStream=new E;_overlayPanelClass=Fr(this._defaults?.overlayPanelClass||[]);_windowBlurHandler=()=>{this._canOpenOnNextFocus=this.panelOpen||!this._hasFocus()};_onChange=()=>{};_onTouched=()=>{};autocomplete;position="auto";connectedTo;autocompleteAttribute="off";autocompleteDisabled=!1;constructor(){}_aboveClass="mat-mdc-autocomplete-panel-above";ngAfterViewInit(){this._initialized.next(),this._initialized.complete(),this._cleanupWindowBlur=this._renderer.listen("window","blur",this._windowBlurHandler)}ngOnChanges(e){e.position&&this._positionStrategy&&(this._setStrategyPositions(this._positionStrategy),this.panelOpen&&this._overlayRef.updatePosition())}ngOnDestroy(){this._cleanupWindowBlur?.(),this._handsetLandscapeSubscription.unsubscribe(),this._viewportSubscription.unsubscribe(),this._componentDestroyed=!0,this._destroyPanel(),this._closeKeyEventStream.complete(),this._clearFromModal()}get panelOpen(){return this._overlayAttached&&this.autocomplete.showPanel}_overlayAttached=!1;openPanel(){this._openPanelInternal()}closePanel(){this._resetLabel(),this._overlayAttached&&(this.panelOpen&&this._zone.run(()=>{this.autocomplete.closed.emit()}),this.autocomplete._latestOpeningTrigger===this&&(this.autocomplete._isOpen=!1,this.autocomplete._latestOpeningTrigger=null),this._overlayAttached=!1,this._pendingAutoselectedOption=null,this._overlayRef&&this._overlayRef.hasAttached()&&(this._overlayRef.detach(),this._closingActionsSubscription.unsubscribe()),this._updatePanelState(),this._componentDestroyed||this._changeDetectorRef.detectChanges(),this._trackedModal&&vo(this._trackedModal,"aria-owns",this.autocomplete.id))}updatePosition(){this._overlayAttached&&this._overlayRef.updatePosition()}get panelClosingActions(){return at(this.optionSelections,this.autocomplete._keyManager.tabOut.pipe(oe(()=>this._overlayAttached)),this._closeKeyEventStream,this._getOutsideClickStream(),this._overlayRef?this._overlayRef.detachments().pipe(oe(()=>this._overlayAttached)):Q()).pipe($(e=>e instanceof Qc?e:null))}optionSelections=jn(()=>{let e=this.autocomplete?this.autocomplete.options:null;return e?e.changes.pipe(Ie(e),We(()=>at(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(We(()=>this.optionSelections))});get activeOption(){return this.autocomplete&&this.autocomplete._keyManager?this.autocomplete._keyManager.activeItem:null}_getOutsideClickStream(){return new ae(e=>{let i=o=>{let a=$t(o),s=this._formField?this._formField.getConnectedOverlayOrigin().nativeElement:null,l=this.connectedTo?this.connectedTo.elementRef.nativeElement:null;this._overlayAttached&&a!==this._element.nativeElement&&!this._hasFocus()&&(!s||!s.contains(a))&&(!l||!l.contains(a))&&this._overlayRef&&!this._overlayRef.overlayElement.contains(a)&&e.next(o)},r=[this._renderer.listen("document","click",i),this._renderer.listen("document","auxclick",i),this._renderer.listen("document","touchend",i)];return()=>{r.forEach(o=>o())}})}writeValue(e){Promise.resolve(null).then(()=>this._assignOptionValue(e))}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this._element.nativeElement.disabled=e}_handleKeydown(e){let i=e,r=i.keyCode,o=Ye(i);if(r===27&&!o&&i.preventDefault(),this._valueOnLastKeydown=this._element.nativeElement.value,this.activeOption&&r===13&&this.panelOpen&&!o)this.activeOption._selectViaInteraction(),this._resetActiveItem(),i.preventDefault();else if(this.autocomplete){let a=this.autocomplete._keyManager.activeItem,s=r===38||r===40;r===9||s&&!o&&this.panelOpen?this.autocomplete._keyManager.onKeydown(i):s&&this._canOpen()&&this._openPanelInternal(this._valueOnLastKeydown),(s||this.autocomplete._keyManager.activeItem!==a)&&(this._scrollToOption(this.autocomplete._keyManager.activeItemIndex||0),this.autocomplete.autoSelectActiveOption&&this.activeOption&&(this._pendingAutoselectedOption||(this._valueBeforeAutoSelection=this._valueOnLastKeydown),this._pendingAutoselectedOption=this.activeOption,this._assignOptionValue(this.activeOption.value)))}}_handleInput(e){let i=e.target,r=i.value;if(i.type==="number"&&(r=r==""?null:parseFloat(r)),this._previousValue!==r){if(this._previousValue=r,this._pendingAutoselectedOption=null,(!this.autocomplete||!this.autocomplete.requireSelection)&&this._onChange(r),!r)this._clearPreviousSelectedOption(null,!1);else if(this.panelOpen&&!this.autocomplete.requireSelection){let o=this.autocomplete.options?.find(a=>a.selected);if(o){let a=this._getDisplayValue(o.value);r!==a&&o.deselect(!1)}}if(this._canOpen()&&this._hasFocus()){let o=this._valueOnLastKeydown??this._element.nativeElement.value;this._valueOnLastKeydown=null,this._openPanelInternal(o)}}}_handleFocus(){this._canOpenOnNextFocus?this._canOpen()&&(this._previousValue=this._element.nativeElement.value,this._attachOverlay(this._previousValue),this._floatLabel(!0)):this._canOpenOnNextFocus=!0}_handleClick(){this._canOpen()&&!this.panelOpen&&this._openPanelInternal()}_hasFocus(){return ho()===this._element.nativeElement}_floatLabel(e=!1){this._formField&&this._formField.floatLabel==="auto"&&(e?this._formField._animateAndLockLabel():this._formField.floatLabel="always",this._manuallyFloatingLabel=!0)}_resetLabel(){this._manuallyFloatingLabel&&(this._formField&&(this._formField.floatLabel="auto"),this._manuallyFloatingLabel=!1)}_subscribeToClosingActions(){let e=new ae(r=>{ft(()=>{r.next()},{injector:this._environmentInjector})}),i=this.autocomplete.options?.changes.pipe(st(()=>this._positionStrategy.reapplyLastPosition()),Op(0))??Q();return at(e,i).pipe(We(()=>this._zone.run(()=>{let r=this.panelOpen;return this._resetActiveItem(),this._updatePanelState(),this._changeDetectorRef.detectChanges(),this.panelOpen&&this._overlayRef.updatePosition(),r!==this.panelOpen&&(this.panelOpen?this._emitOpened():this.autocomplete.closed.emit()),this.panelClosingActions})),ke(1)).subscribe(r=>this._setValueAndClose(r))}_emitOpened(){this.autocomplete.opened.emit()}_destroyPanel(){this._overlayRef&&(this.closePanel(),this._overlayRef.dispose(),this._overlayRef=null)}_getDisplayValue(e){let i=this.autocomplete;return i&&i.displayWith?i.displayWith(e):e}_assignOptionValue(e){let i=this._getDisplayValue(e);e==null&&this._clearPreviousSelectedOption(null,!1),this._updateNativeInputValue(i??"")}_updateNativeInputValue(e){this._formField?this._formField._control.value=e:this._element.nativeElement.value=e,this._previousValue=e}_setValueAndClose(e){let i=this.autocomplete,r=e?e.source:this._pendingAutoselectedOption;r?(this._clearPreviousSelectedOption(r),this._assignOptionValue(r.value),this._onChange(r.value),i._emitSelectEvent(r),this._element.nativeElement.focus()):i.requireSelection&&this._element.nativeElement.value!==this._valueOnAttach&&(this._clearPreviousSelectedOption(null),this._assignOptionValue(null),this._onChange(null)),this.closePanel()}_clearPreviousSelectedOption(e,i){this.autocomplete?.options?.forEach(r=>{r!==e&&r.selected&&r.deselect(i)})}_openPanelInternal(e=this._element.nativeElement.value){if(this._attachOverlay(e),this._floatLabel(),this._trackedModal){let i=this.autocomplete.id;js(this._trackedModal,"aria-owns",i)}}_attachOverlay(e){if(!this.autocomplete)return;let i=this._overlayRef;i?(this._positionStrategy.setOrigin(this._getConnectedElement()),i.updateSize({width:this._getPanelWidth()})):(this._portal=new Wn(this.autocomplete.template,this._viewContainerRef,{id:this._formField?.getLabelId()}),i=ki(this._injector,this._getOverlayConfig()),this._overlayRef=i,this._viewportSubscription=this._viewportRuler.change().subscribe(()=>{this.panelOpen&&i&&i.updateSize({width:this._getPanelWidth()})}),this._handsetLandscapeSubscription=this._breakpointObserver.observe(ED.HandsetLandscape).subscribe(o=>{o.matches?this._positionStrategy.withFlexibleDimensions(!0).withGrowAfterOpen(!0).withViewportMargin(8):this._positionStrategy.withFlexibleDimensions(!1).withGrowAfterOpen(!1).withViewportMargin(0)})),i&&!i.hasAttached()&&(i.attach(this._portal),this._valueOnAttach=e,this._valueOnLastKeydown=null,this._closingActionsSubscription=this._subscribeToClosingActions());let r=this.panelOpen;this.autocomplete._isOpen=this._overlayAttached=!0,this.autocomplete._latestOpeningTrigger=this,this.autocomplete._setColor(this._formField?.color),this._updatePanelState(),this._applyModalPanelOwnership(),this.panelOpen&&r!==this.panelOpen&&this._emitOpened()}_handlePanelKeydown=e=>{(e.keyCode===27&&!Ye(e)||e.keyCode===38&&Ye(e,"altKey"))&&(this._pendingAutoselectedOption&&(this._updateNativeInputValue(this._valueBeforeAutoSelection??""),this._pendingAutoselectedOption=null),this._closeKeyEventStream.next(),this._resetActiveItem(),e.stopPropagation(),e.preventDefault())};_updatePanelState(){if(this.autocomplete._setVisibility(),this.panelOpen){let e=this._overlayRef;this._keydownSubscription||(this._keydownSubscription=e.keydownEvents().subscribe(this._handlePanelKeydown)),this._outsideClickSubscription||(this._outsideClickSubscription=e.outsidePointerEvents().subscribe())}else this._keydownSubscription?.unsubscribe(),this._outsideClickSubscription?.unsubscribe(),this._keydownSubscription=this._outsideClickSubscription=void 0}_getOverlayConfig(){return new Ti({positionStrategy:this._getOverlayPosition(),scrollStrategy:this._scrollStrategy(),width:this._getPanelWidth(),direction:this._dir??void 0,hasBackdrop:this._defaults?.hasBackdrop,backdropClass:this._defaults?.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:this._overlayPanelClass,disableAnimations:this._animationsDisabled})}_getOverlayPosition(){let e=jr(this._injector,this._getConnectedElement()).withFlexibleDimensions(!1).withPush(!1).withPopoverLocation("inline");return this._setStrategyPositions(e),this._positionStrategy=e,e}_setStrategyPositions(e){let i=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],r=this._aboveClass,o=[{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:r},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:r}],a;this.position==="above"?a=o:this.position==="below"?a=i:a=[...i,...o],e.withPositions(a)}_getConnectedElement(){return this.connectedTo?this.connectedTo.elementRef:this._formField?this._formField.getConnectedOverlayOrigin():this._element}_getPanelWidth(){return this.autocomplete.panelWidth||this._getHostWidth()}_getHostWidth(){return this._getConnectedElement().nativeElement.getBoundingClientRect().width}_resetActiveItem(){let e=this.autocomplete;if(e.autoActiveFirstOption){let i=-1;for(let r=0;r<e.options.length;r++)if(!e.options.get(r).disabled){i=r;break}e._keyManager.setActiveItem(i)}else e._keyManager.setActiveItem(-1)}_canOpen(){let e=this._element.nativeElement;return!e.readOnly&&!e.disabled&&!this.autocompleteDisabled}_scrollToOption(e){let i=this.autocomplete,r=Mf(e,i.options,i.optionGroups);if(e===0&&r===1)i._setScrollTop(0);else if(i.panel){let o=i.options.toArray()[e];if(o){let a=o._getHostElement(),s=If(a.offsetTop,a.offsetHeight,i._getScrollTop(),i.panel.nativeElement.offsetHeight);i._setScrollTop(s)}}}_trackedModal=null;_applyModalPanelOwnership(){let e=this._element.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=this.autocomplete.id;this._trackedModal&&vo(this._trackedModal,"aria-owns",i),js(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(this._trackedModal){let e=this.autocomplete.id;vo(this._trackedModal,"aria-owns",e),this._trackedModal=null}}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["input","matAutocomplete",""],["textarea","matAutocomplete",""]],hostAttrs:[1,"mat-mdc-autocomplete-trigger"],hostVars:7,hostBindings:function(i,r){i&1&&D("focusin",function(){return r._handleFocus()})("blur",function(){return r._onTouched()})("input",function(a){return r._handleInput(a)})("keydown",function(a){return r._handleKeydown(a)})("click",function(){return r._handleClick()}),i&2&&J("autocomplete",r.autocompleteAttribute)("role",r.autocompleteDisabled?null:"combobox")("aria-autocomplete",r.autocompleteDisabled?null:"list")("aria-activedescendant",r.panelOpen&&r.activeOption?r.activeOption.id:null)("aria-expanded",r.autocompleteDisabled?null:r.panelOpen.toString())("aria-controls",r.autocompleteDisabled||!r.panelOpen||r.autocomplete==null?null:r.autocomplete.id)("aria-haspopup",r.autocompleteDisabled?null:"listbox")},inputs:{autocomplete:[0,"matAutocomplete","autocomplete"],position:[0,"matAutocompletePosition","position"],connectedTo:[0,"matAutocompleteConnectedTo","connectedTo"],autocompleteAttribute:[0,"autocomplete","autocompleteAttribute"],autocompleteDisabled:[2,"matAutocompleteDisabled","autocompleteDisabled",Z]},exportAs:["matAutocompleteTrigger"],features:[we([kP]),je]})}return t})(),gt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Yn,Gs,Nn,Gs,Ce]})}return t})();var RP=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2,changeDetection:0})}return t})(),NP={passive:!0},cM=(()=>{class t{_platform=d(Ee);_ngZone=d(P);_renderer=d(Tt).createRenderer(null,null);_styleLoader=d(ht);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return ot;this._styleLoader.load(RP);let i=En(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new E,a="cdk-text-field-autofilled",s=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(a)?(i.classList.add(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(a)&&(i.classList.remove(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",s,NP)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=En(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var dM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({})}return t})();var uM=new y("MAT_INPUT_VALUE_ACCESSOR");var Bf=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ws=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var Ge=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Fs,mt,Ce]})}return t})();var PP=["button","checkbox","file","hidden","image","radio","range","reset","submit"],LP=new y("MAT_INPUT_CONFIG"),Pt=(()=>{class t{_elementRef=d(N);_platform=d(Ee);ngControl=d(qn,{optional:!0,self:!0});_autofillMonitor=d(cM);_ngZone=d(P);_formField=d(Ma,{optional:!0});_renderer=d(Se);_uid=d(ze).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(LP,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new E;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=Xe(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(fe.required)??!1}set required(e){this._required=Xe(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&$v().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=Xe(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>$v().has(e));constructor(){let e=d(Nr,{optional:!0}),i=d(zt,{optional:!0}),r=d(Bf),o=d(uM,{optional:!0,self:!0}),a=this._elementRef.nativeElement,s=a.nodeName.toLowerCase();o?Xi(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Ws(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=s==="select",this._isTextarea=s==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&xr(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){PP.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&D("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(dn("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),J("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),R("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",Z]},exportAs:["matInput"],features:[we([{provide:id,useExisting:t}]),je]})}return t})(),_t=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Ge,Ge,dM,Ce]})}return t})();var jP={capture:!0},VP=["focus","mousedown","mouseenter","touchstart"],tb="mat-ripple-loader-uninitialized",nb="mat-ripple-loader-class-name",mM="mat-ripple-loader-centered",Hf="mat-ripple-loader-disabled",fM=(()=>{class t{_document=d(W);_animationsDisabled=He();_globalRippleOptions=d(Zc,{optional:!0});_platform=d(Ee);_ngZone=d(P);_injector=d(G);_eventCleanups;_hosts=new Map;constructor(){let e=d(Tt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>VP.map(i=>e.listen(this._document,i,this._onInteraction,jP)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(tb,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(nb))&&e.setAttribute(nb,i.className||""),i.centered&&e.setAttribute(mM,""),i.disabled&&e.setAttribute(Hf,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Hf,""):e.removeAttribute(Hf)}_onInteraction=e=>{let i=$t(e);if(i instanceof HTMLElement){let r=i.closest(`[${tb}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(nb)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Yc.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??Yc.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Hf),rippleConfig:{centered:e.hasAttribute(mM),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new wa(s,this._ngZone,i,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(tb)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var BP=["mat-icon-button",""],HP=["*"],UP=new y("MAT_BUTTON_CONFIG");function pM(t){return t==null?void 0:Ei(t)}var ib=(()=>{class t{_elementRef=d(N);_ngZone=d(P);_animationsDisabled=He();_config=d(UP,{optional:!0});_focusMonitor=d(On);_cleanupClick;_renderer=d(Se);_rippleLoader=d(fM);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(ht).load(or);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(J("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),on(r.color?"mat-"+r.color:""),R("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",Z],disabled:[2,"disabled","disabled",Z],ariaDisabled:[2,"aria-disabled","ariaDisabled",Z],disabledInteractive:[2,"disabledInteractive","disabledInteractive",Z],tabIndex:[2,"tabIndex","tabIndex",pM],_tabindex:[2,"tabindex","_tabindex",pM]}})}return t})(),Zn=(()=>{class t extends ib{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[pe],attrs:BP,ngContentSelectors:HP,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Me(),An(0,"span",0),V(1),An(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var zP=["matButton",""],$P=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],qP=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var hM=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),vt=(()=>{class t extends ib{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=GP(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?hM.get(this._appearance):null,o=hM.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[pe],attrs:zP,ngContentSelectors:qP,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Me($P),An(0,"span",0),V(1),dt(2,"span",1),V(3,1),At(),V(4,2),An(5,"span",2)(6,"span",3)),i&2&&R("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();function GP(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var bt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Co,Ce]})}return t})();var WP=["*"];var YP=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],ZP=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],QP=new y("MAT_CARD_CONFIG"),Lt=(()=>{class t{appearance;constructor(){let e=d(QP,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&R("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:WP,decls:1,vars:0,template:function(i,r){i&1&&(Me(),V(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return t})(),en=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var jt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})(),tn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-subtitle"],["","mat-card-subtitle",""],["","matCardSubtitle",""]],hostAttrs:[1,"mat-mdc-card-subtitle"]})}return t})(),gn=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})(),nn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:ZP,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(Me(YP),V(0),dt(1,"div",0),V(2,1),At(),V(3,2))},encapsulation:2,changeDetection:0})}return t})();var Pn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-card-avatar",""],["","matCardAvatar",""]],hostAttrs:[1,"mat-mdc-card-avatar"]})}return t})();var tt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Ce]})}return t})();var gM=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=Xe(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=Xe(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(J("aria-orientation",r.vertical?"vertical":"horizontal"),R("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2,changeDetection:0})}return t})(),nt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Ce]})}return t})();function _M(t){return Error(`Unable to find icon with the name "${t}"`)}function KP(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function vM(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function bM(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Vr=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},CM=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Vr(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let a=this._sanitizer.sanitize(Re.HTML,r);if(!a)throw bM(r);let s=_a(a);return this._addSvgIconConfig(e,i,new Vr("",s,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Vr(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(Re.HTML,i);if(!o)throw bM(i);let a=_a(o);return this._addSvgIconSetConfig(e,new Vr("",a,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(Re.RESOURCE_URL,e);if(!i)throw vM(e);let r=this._cachedIconsByUrl.get(i);return r?Q(Uf(r)):this._loadSvgIconFromConfig(new Vr(e,null)).pipe(st(o=>this._cachedIconsByUrl.set(i,o)),$(o=>Uf(o)))}getNamedSvgIcon(e,i=""){let r=yM(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let a=this._iconSetConfigs.get(i);return a?this._getSvgFromIconSetConfigs(e,a):ll(_M(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?Q(Uf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe($(i=>Uf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return Q(r);let o=i.filter(a=>!a.svgText).map(a=>this._loadSvgIconSetFromConfig(a).pipe(Gr(s=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(Re.RESOURCE_URL,a.url)} failed: ${s.message}`;return this._errorHandler.handleError(new Error(c)),Q(null)})));return jo(o).pipe($(()=>{let a=this._extractIconWithNameFromAnySet(e,i);if(!a)throw _M(e);return a}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let a=this._svgElementFromConfig(o),s=this._extractSvgIconFromSet(a,e,o.options);if(s)return s}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(st(i=>e.svgText=i),$(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?Q(null):this._fetchIcon(e).pipe(st(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let a=o.cloneNode(!0);if(a.removeAttribute("id"),a.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(a,r);if(a.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(a),r);let s=this._svgElementFromString(_a("<svg></svg>"));return s.appendChild(a),this._setSvgAttributes(s,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(_a("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:a,value:s}=r[o];a!=="id"&&i.setAttribute(a,s)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw KP();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let a=this._sanitizer.sanitize(Re.RESOURCE_URL,i);if(!a)throw vM(i);let s=this._inProgressUrlFetches.get(a);if(s)return s;let l=this._httpClient.get(a,{responseType:"text",withCredentials:o}).pipe($(c=>_a(c)),Bo(()=>this._inProgressUrlFetches.delete(a)),ul());return this._inProgressUrlFetches.set(a,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(yM(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return XP(o)?new Vr(o.url,null,o.options):new Vr(o,null)}}static \u0275fac=function(i){return new(i||t)(K(Je,8),K(uc),K(W,8),K(vn))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Uf(t){return t.cloneNode(!0)}function yM(t,n){return t+":"+n}function XP(t){return!!(t.url&&t.options)}var JP=["*"],eL=new y("MAT_ICON_DEFAULT_OPTIONS"),tL=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(W),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),xM=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],nL=xM.map(t=>`[${t}]`).join(", "),iL=/^url\(['"]?#(.*?)['"]?\)$/,it=(()=>{class t{_elementRef=d(N);_iconRegistry=d(CM);_location=d(tL);_errorHandler=d(vn);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=de.EMPTY;constructor(){let e=d(new tr("aria-hidden"),{optional:!0}),i=d(eL,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(a=>{o.setAttribute(a.name,`url('${e}#${a.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(nL),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)xM.forEach(a=>{let s=i[o],l=s.getAttribute(a),c=l?l.match(iL):null;if(c){let f=r.get(s);f||(f=[],r.set(s,f)),f.push({name:a,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(ke(1)).subscribe(o=>this._setSvgElement(o),o=>{let a=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(a))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(J("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),on(r.color?"mat-"+r.color:""),R("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",Z],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:JP,decls:1,vars:0,template:function(i,r){i&1&&(Me(),V(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),rt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Ce]})}return t})();var od=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new E;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var ad=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var wM=["*"],EM=`.mdc-list {
  margin: 0;
  padding: 8px 0;
  list-style-type: none;
}
.mdc-list:focus {
  outline: none;
}

.mdc-list-item {
  display: flex;
  position: relative;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  align-items: stretch;
  cursor: pointer;
  padding-left: 16px;
  padding-right: 16px;
  background-color: var(--mat-list-list-item-container-color, transparent);
  border-radius: var(--mat-list-list-item-container-shape, var(--mat-sys-corner-none));
}
.mdc-list-item.mdc-list-item--selected {
  background-color: var(--mat-list-list-item-selected-container-color);
}
.mdc-list-item:focus {
  outline: 0;
}
.mdc-list-item.mdc-list-item--disabled {
  cursor: auto;
}
.mdc-list-item.mdc-list-item--with-one-line {
  height: var(--mat-list-list-item-one-line-container-height, 48px);
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__start {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-two-lines {
  height: var(--mat-list-list-item-two-line-container-height, 64px);
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-three-lines {
  height: var(--mat-list-list-item-three-line-container-height, 88px);
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--selected::before, .mdc-list-item.mdc-list-item--selected:focus::before, .mdc-list-item:not(.mdc-list-item--selected):focus::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  content: "";
  pointer-events: none;
}

a.mdc-list-item {
  color: inherit;
  text-decoration: none;
}

.mdc-list-item__start {
  fill: currentColor;
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-leading-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-leading-icon-size, 24px);
  height: var(--mat-list-list-item-leading-icon-size, 24px);
  margin-left: 16px;
  margin-right: 32px;
}
[dir=rtl] .mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-left: 32px;
  margin-right: 16px;
}
.mdc-list-item--with-leading-icon:hover .mdc-list-item__start {
  color: var(--mat-list-list-item-hover-leading-icon-color);
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start {
  width: var(--mat-list-list-item-leading-avatar-size, 40px);
  height: var(--mat-list-list-item-leading-avatar-size, 40px);
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start, [dir=rtl] .mdc-list-item--with-leading-avatar .mdc-list-item__start {
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}

.mdc-list-item__end {
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  font-family: var(--mat-list-list-item-trailing-supporting-text-font, var(--mat-sys-label-small-font));
  line-height: var(--mat-list-list-item-trailing-supporting-text-line-height, var(--mat-sys-label-small-line-height));
  font-size: var(--mat-list-list-item-trailing-supporting-text-size, var(--mat-sys-label-small-size));
  font-weight: var(--mat-list-list-item-trailing-supporting-text-weight, var(--mat-sys-label-small-weight));
  letter-spacing: var(--mat-list-list-item-trailing-supporting-text-tracking, var(--mat-sys-label-small-tracking));
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-trailing-icon-size, 24px);
  height: var(--mat-list-list-item-trailing-icon-size, 24px);
}
.mdc-list-item--with-trailing-icon:hover .mdc-list-item__end {
  color: var(--mat-list-list-item-hover-trailing-icon-color);
}
.mdc-list-item.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-supporting-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-list-item--selected.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-selected-trailing-icon-color, var(--mat-sys-primary));
}

.mdc-list-item__content {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  align-self: center;
  flex: 1;
  pointer-events: none;
}
.mdc-list-item--with-two-lines .mdc-list-item__content, .mdc-list-item--with-three-lines .mdc-list-item__content {
  align-self: stretch;
}

.mdc-list-item__primary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  color: var(--mat-list-list-item-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-list-list-item-label-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-list-list-item-label-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-list-list-item-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-list-list-item-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-list-list-item-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-list-item:hover .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item:focus .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-focus-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text, .mdc-list-item--with-three-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}

.mdc-list-item__secondary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  display: block;
  margin-top: 0;
  color: var(--mat-list-list-item-supporting-text-color, var(--mat-sys-on-surface-variant));
  font-family: var(--mat-list-list-item-supporting-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-list-list-item-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-list-list-item-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-list-list-item-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-list-list-item-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}
.mdc-list-item__secondary-text::before {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-three-lines .mdc-list-item__secondary-text {
  white-space: normal;
  line-height: 20px;
}
.mdc-list-item--with-overline .mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: auto;
}

.mdc-list-item--with-leading-radio.mdc-list-item,
.mdc-list-item--with-leading-checkbox.mdc-list-item,
.mdc-list-item--with-leading-icon.mdc-list-item,
.mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
[dir=rtl] .mdc-list-item--with-leading-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-checkbox.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-icon.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  display: block;
  margin-top: 0;
  line-height: normal;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-trailing-icon.mdc-list-item, [dir=rtl] .mdc-list-item--with-trailing-icon.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
}

.mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  -webkit-user-select: none;
  user-select: none;
  margin-left: 28px;
  margin-right: 16px;
}
[dir=rtl] .mdc-list-item--with-trailing-meta .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 28px;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end {
  display: block;
  line-height: normal;
  align-self: flex-start;
  margin-top: 0;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end::before, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-leading-radio .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 8px;
  margin-right: 24px;
}
[dir=rtl] .mdc-list-item--with-leading-radio .mdc-list-item__start,
[dir=rtl] .mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 24px;
  margin-right: 8px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-item--with-trailing-radio.mdc-list-item,
.mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-left: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, [dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-right: 0;
}
.mdc-list-item--with-trailing-radio .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 24px;
  margin-right: 8px;
}
[dir=rtl] .mdc-list-item--with-trailing-radio .mdc-list-item__end,
[dir=rtl] .mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 8px;
  margin-right: 24px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-three-lines .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-group__subheader {
  margin: 0.75rem 16px;
}

.mdc-list-item--disabled .mdc-list-item__start,
.mdc-list-item--disabled .mdc-list-item__content,
.mdc-list-item--disabled .mdc-list-item__end {
  opacity: 1;
}
.mdc-list-item--disabled .mdc-list-item__primary-text,
.mdc-list-item--disabled .mdc-list-item__secondary-text {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}
.mdc-list-item--disabled.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-disabled-leading-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-leading-icon-opacity, 0.38);
}
.mdc-list-item--disabled.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-disabled-trailing-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-trailing-icon-opacity, 0.38);
}

.mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing, [dir=rtl] .mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing {
  padding-left: 0;
  padding-right: 0;
}

.mdc-list-item.mdc-list-item--disabled .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-disabled-label-text-color, var(--mat-sys-on-surface));
}

.mdc-list-item:hover::before {
  background-color: var(--mat-list-list-item-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}

.mdc-list-item.mdc-list-item--disabled::before {
  background-color: var(--mat-list-list-item-disabled-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item:focus::before {
  background-color: var(--mat-list-list-item-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item--disabled .mdc-radio,
.mdc-list-item--disabled .mdc-checkbox {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}

.mdc-list-item--with-leading-avatar .mat-mdc-list-item-avatar {
  border-radius: var(--mat-list-list-item-leading-avatar-shape, var(--mat-sys-corner-full));
  background-color: var(--mat-list-list-item-leading-avatar-color, var(--mat-sys-primary-container));
}

.mat-mdc-list-item-icon {
  font-size: var(--mat-list-list-item-leading-icon-size, 24px);
}

@media (forced-colors: active) {
  a.mdc-list-item--activated::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  a.mdc-list-item--activated [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-list-base {
  display: block;
}
.mat-mdc-list-base .mdc-list-item__start,
.mat-mdc-list-base .mdc-list-item__end,
.mat-mdc-list-base .mdc-list-item__content {
  pointer-events: auto;
}

.mat-mdc-list-item,
.mat-mdc-list-option {
  width: 100%;
  box-sizing: border-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-list-item:not(.mat-mdc-list-item-interactive),
.mat-mdc-list-option:not(.mat-mdc-list-item-interactive) {
  cursor: default;
}
.mat-mdc-list-item .mat-divider-inset,
.mat-mdc-list-option .mat-divider-inset {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
}
.mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
.mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-left: 72px;
}
[dir=rtl] .mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
[dir=rtl] .mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-right: 72px;
}

.mat-mdc-list-item-interactive::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  content: "";
  opacity: 0;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-list-item > .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-list-item:focus-visible > .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-line.mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: normal;
}
.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-unscoped-content.mdc-list-item__secondary-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

mat-action-list button {
  background: none;
  color: inherit;
  border: none;
  font: inherit;
  outline: inherit;
  -webkit-tap-highlight-color: transparent;
  text-align: start;
}
mat-action-list button::-moz-focus-inner {
  border: 0;
}

.mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-inline-start: var(--mat-list-list-item-leading-icon-start-space, 16px);
  margin-inline-end: var(--mat-list-list-item-leading-icon-end-space, 16px);
}

.mat-mdc-nav-list .mat-mdc-list-item {
  border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
  --mat-focus-indicator-border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
}
.mat-mdc-nav-list .mat-mdc-list-item.mdc-list-item--activated {
  background-color: var(--mat-list-active-indicator-color, var(--mat-sys-secondary-container));
}
`,rL=["unscopedContent"],oL=["text"],aL=[[["","matListItemAvatar",""],["","matListItemIcon",""]],[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["","matListItemMeta",""]],[["mat-divider"]]],sL=["[matListItemAvatar],[matListItemIcon]","[matListItemTitle]","[matListItemLine]","*","[matListItemMeta]","mat-divider"];var lL=new y("ListOption"),Qn=(()=>{class t{_elementRef=d(N);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return t})(),ld=(()=>{class t{_elementRef=d(N);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return t})(),Li=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matListItemMeta",""]],hostAttrs:[1,"mat-mdc-list-item-meta","mdc-list-item__end"]})}return t})(),DM=(()=>{class t{_listOption=d(lL,{optional:!0});constructor(){}_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,hostVars:4,hostBindings:function(i,r){i&2&&R("mdc-list-item__start",r._isAlignedAtStart())("mdc-list-item__end",!r._isAlignedAtStart())}})}return t})(),cL=(()=>{class t extends DM{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[pe]})}return t})(),Kn=(()=>{class t extends DM{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[pe]})}return t})(),dL=new y("MAT_LIST_CONFIG"),sd=(()=>{class t{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=Xe(e)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(Xe(e))}_disabled=ee(!1);_defaultOptions=d(dL,{optional:!0});static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,hostVars:1,hostBindings:function(i,r){i&2&&J("aria-disabled",r.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return t})(),uL=(()=>{class t{_elementRef=d(N);_ngZone=d(P);_listBase=d(sd,{optional:!0});_platform=d(Ee);_hostElement;_isButtonElement;_noopAnimations=He();_avatars;_icons;set lines(e){this._explicitLines=pn(e,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(e){this._disableRipple=Xe(e)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(e){this._disabled.set(Xe(e))}_disabled=ee(!1);_subscriptions=new de;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){d(ht).load(or);let e=d(Zc,{optional:!0});this.rippleConfig=e||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new wa(this,this._ngZone,this._hostElement,this._platform,d(G)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(at(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(e){if(!this._lines||!this._titles||!this._unscopedContent)return;e&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),r=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let o=this._titles.length===0&&i===1;r.classList.toggle("mdc-list-item__primary-text",o),r.classList.toggle("mdc-list-item__secondary-text",!o)}else r.classList.remove("mdc-list-item__primary-text"),r.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let e=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(e+=1),e}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(e=>e.nodeType!==e.COMMENT_NODE).some(e=>!!(e.textContent&&e.textContent.trim()))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,contentQueries:function(i,r,o){if(i&1&&ut(o,cL,4)(o,Kn,4),i&2){let a;H(a=U())&&(r._avatars=a),H(a=U())&&(r._icons=a)}},hostVars:4,hostBindings:function(i,r){i&2&&(J("aria-disabled",r.disabled)("disabled",r._isButtonElement&&r.disabled||null),R("mdc-list-item--disabled",r.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return t})();var lr=(()=>{class t extends sd{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-list"]],hostAttrs:[1,"mat-mdc-list","mat-mdc-list-base","mdc-list"],exportAs:["matList"],features:[we([{provide:sd,useExisting:t}]),pe],ngContentSelectors:wM,decls:1,vars:0,template:function(i,r){i&1&&(Me(),V(0))},styles:[EM],encapsulation:2,changeDetection:0})}return t})(),ci=(()=>{class t extends uL{_lines;_titles;_meta;_unscopedContent;_itemText;get activated(){return this._activated}set activated(e){this._activated=Xe(e)}_activated=!1;_getAriaCurrent(){return this._hostElement.nodeName==="A"&&this._activated?"page":null}_hasBothLeadingAndTrailing(){return this._meta.length!==0&&(this._avatars.length!==0||this._icons.length!==0)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-list-item"],["a","mat-list-item",""],["button","mat-list-item",""]],contentQueries:function(i,r,o){if(i&1&&ut(o,ld,5)(o,Qn,5)(o,Li,5),i&2){let a;H(a=U())&&(r._lines=a),H(a=U())&&(r._titles=a),H(a=U())&&(r._meta=a)}},viewQuery:function(i,r){if(i&1&&qe(rL,5)(oL,5),i&2){let o;H(o=U())&&(r._unscopedContent=o.first),H(o=U())&&(r._itemText=o.first)}},hostAttrs:[1,"mat-mdc-list-item","mdc-list-item"],hostVars:13,hostBindings:function(i,r){i&2&&(J("aria-current",r._getAriaCurrent()),R("mdc-list-item--activated",r.activated)("mdc-list-item--with-leading-avatar",r._avatars.length!==0)("mdc-list-item--with-leading-icon",r._icons.length!==0)("mdc-list-item--with-trailing-meta",r._meta.length!==0)("mat-mdc-list-item-both-leading-and-trailing",r._hasBothLeadingAndTrailing())("_mat-animation-noopable",r._noopAnimations))},inputs:{activated:"activated"},exportAs:["matListItem"],features:[pe],ngContentSelectors:sL,decls:10,vars:0,consts:[["unscopedContent",""],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mat-focus-indicator"]],template:function(i,r){i&1&&(Me(aL),V(0),m(1,"span",1),V(2,1),V(3,2),m(4,"span",2,0),D("cdkObserveContent",function(){return r._updateItemLines(!0)}),V(6,3),u()(),V(7,4),V(8,5),te(9,"div",3))},dependencies:[cD],encapsulation:2,changeDetection:0})}return t})();var MM=(()=>{class t extends sd{_isNonInteractive=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-nav-list"]],hostAttrs:["role","navigation",1,"mat-mdc-nav-list","mat-mdc-list-base","mdc-list"],exportAs:["matNavList"],features:[we([{provide:sd,useExisting:t}]),pe],ngContentSelectors:wM,decls:1,vars:0,template:function(i,r){i&1&&(Me(),V(0))},styles:[EM],encapsulation:2,changeDetection:0})}return t})();var yt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Fs,Co,jf,Ce,nt]})}return t})();var mL=["mat-menu-item",""],fL=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],pL=["mat-icon, [matMenuItemIcon]","*"];function hL(t,n){t&1&&(ni(),m(0,"svg",2),te(1,"polygon",3),u())}var gL=["*"];function _L(t,n){if(t&1){let e=Oe();dt(0,"div",0),as("click",function(){se(e);let r=I();return le(r.closed.emit("click"))})("animationstart",function(r){se(e);let o=I();return le(o._onAnimationStart(r.animationName))})("animationend",function(r){se(e);let o=I();return le(o._onAnimationDone(r.animationName))})("animationcancel",function(r){se(e);let o=I();return le(o._onAnimationDone(r.animationName))}),dt(1,"div",1),V(2),At()()}if(t&2){let e=I();on(e._classList),R("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),dn("id",e.panelId),J("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var ob=new y("MAT_MENU_PANEL"),cd=(()=>{class t{_elementRef=d(N);_document=d(W);_focusMonitor=d(On);_parentMenu=d(ob,{optional:!0});_changeDetectorRef=d(me);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new E;_focused=new E;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(ht).load(or),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&D("click",function(a){return r._checkDisabled(a)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(J("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),R("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",Z],disableRipple:[2,"disableRipple","disableRipple",Z]},exportAs:["matMenuItem"],attrs:mL,ngContentSelectors:pL,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(Me(fL),V(0),m(1,"span",0),V(2,1),u(),te(3,"div",1),ge(4,hL,2,0,":svg:svg",2)),i&2&&(h(3),v("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),h(),_e(r._triggersSubmenu?4:-1))},dependencies:[Df],encapsulation:2,changeDetection:0})}return t})();var vL=new y("MatMenuContent");var bL=new y("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),rb="_mat-menu-enter",zf="_mat-menu-exit",Zs=(()=>{class t{_elementRef=d(N);_changeDetectorRef=d(me);_injector=d(G);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=He();_allItems;_directDescendantItems=new Bn;_classList={};_panelAnimationState="void";_animationDone=new E;_isAnimating=ee(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;set panelClass(e){let i=this._previousPanelClass,r=C({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass;get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new F;close=this.closed;panelId=d(ze).getId("mat-menu-panel-");constructor(){let e=d(bL);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new Ca(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(Ie(this._directDescendantItems),We(e=>at(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(Ie(this._directDescendantItems),We(i=>at(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:Ye(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=ft(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=ne(C({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===zf;(i||e===rb)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===rb||e===zf)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(zf),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?rb:zf)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(Ie(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&ut(o,vL,5)(o,cd,5)(o,cd,4),i&2){let a;H(a=U())&&(r.lazyContent=a.first),H(a=U())&&(r._allItems=a),H(a=U())&&(r.items=a)}},viewQuery:function(i,r){if(i&1&&qe(St,5),i&2){let o;H(o=U())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&J("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",Z],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:Z(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[we([{provide:ob,useExisting:t}])],ngContentSelectors:gL,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(Me(),rs(0,_L,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--mat-menu-item-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-menu-item-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-menu-item-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-menu-item-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-menu-item-label-text-weight, var(--mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--mat-menu-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-menu-container-color, var(--mat-sys-surface-container));
  box-shadow: var(--mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--mat-menu-divider-color, var(--mat-sys-surface-variant));
  margin-bottom: var(--mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--mat-menu-item-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--mat-menu-item-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--mat-menu-item-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--mat-menu-item-spacing, 12px);
  height: var(--mat-menu-item-icon-size, 24px);
  width: var(--mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),yL=new y("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(G);return()=>Ai(t)}});var Ys=new WeakMap,CL=(()=>{class t{_canHaveBackdrop;_element=d(N);_viewContainerRef=d(ct);_menuItemInstance=d(cd,{optional:!0,self:!0});_dir=d(It,{optional:!0});_focusMonitor=d(On);_ngZone=d(P);_injector=d(G);_scrollStrategy=d(yL);_changeDetectorRef=d(me);_animationsDisabled=He();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=de.EMPTY;_menuCloseSubscription=de.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e&&(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(ob,{optional:!0});this._parentMaterialMenu=i instanceof Zs?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Ys.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Ys.get(i);Ys.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),a=o.getConfig(),s=a.positionStrategy;this._setPosition(i,s),this._canHaveBackdrop?a.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:a.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof Zs&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(De(i.close)).subscribe(()=>{s.withLockedPosition(!1).reapplyLastPosition(),s.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof Zs&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(ke(1)).subscribe(()=>{i.detach(),Ys.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Ys.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=ki(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof Zs&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new Ti({positionStrategy:jr(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",a=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,a)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[a,s]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[l,c]=[a,s],[f,g]=[r,o],_=0;if(this._triggersSubmenu()){if(g=r=e.xPosition==="before"?"start":"end",o=f=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let b=this._parentMaterialMenu.items.first;this._parentInnerPadding=b?b._getHostElement().offsetTop:0}_=a==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(l=a==="top"?"bottom":"top",c=s==="top"?"bottom":"top");i.withPositions([{originX:r,originY:l,overlayX:f,overlayY:a,offsetY:_},{originX:o,originY:l,overlayX:g,overlayY:a,offsetY:_},{originX:r,originY:c,overlayX:f,overlayY:s,offsetY:-_},{originX:o,originY:c,overlayX:g,overlayY:s,offsetY:-_}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:Q(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(oe(a=>this._menuOpen&&a!==this._menuItemInstance)):Q();return at(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new Wn(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Ys.get(e)===this}_triggerIsAriaDisabled(){return Z(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){em()};static \u0275dir=S({type:t})}return t})(),IM=(()=>{class t extends CL{_cleanupTouchstart;_hoverSubscription=de.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new F;onMenuOpen=this.menuOpened;menuClosed=new F;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(Se);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{ga(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){ha(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&D("click",function(a){return r._handleClick(a)})("mousedown",function(a){return r._handleMousedown(a)})("keydown",function(a){return r._handleKeydown(a)}),i&2&&J("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu==null?null:r.menu.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[pe]})}return t})();var Ct=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Co,Yn,Ce,Nn]})}return t})();var xL=["trigger"],wL=["panel"],EL=[[["mat-select-trigger"]],"*"],DL=["mat-select-trigger","*"];function ML(t,n){if(t&1&&(m(0,"span",4),p(1),u()),t&2){let e=I();h(),B(e.placeholder)}}function IL(t,n){t&1&&V(0)}function SL(t,n){if(t&1&&(m(0,"span",11),p(1),u()),t&2){let e=I(2);h(),B(e.triggerValue)}}function TL(t,n){if(t&1&&(m(0,"span",5),ge(1,IL,1,0)(2,SL,2,1,"span",11),u()),t&2){let e=I();h(),_e(e.customTrigger?1:2)}}function AL(t,n){if(t&1){let e=Oe();m(0,"div",12,1),D("keydown",function(r){se(e);let o=I();return le(o._handleKeydown(r))}),V(2,1),u()}if(t&2){let e=I();on(e.panelClass),R("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),J("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var kL=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(G);return()=>Ai(t)}}),OL=new y("MAT_SELECT_CONFIG"),RL=new y("MatSelectTrigger"),ab=class{source;value;constructor(n,e){this.source=n,this.value=e}},xo=(()=>{class t{_viewportRuler=d(Gn);_changeDetectorRef=d(me);_elementRef=d(N);_dir=d(It,{optional:!0});_idGenerator=d(ze);_renderer=d(Se);_parentFormField=d(Ma,{optional:!0});ngControl=d(qn,{self:!0,optional:!0});_liveAnnouncer=d(Vv);_defaultOptions=d(OL,{optional:!0});_animationsDisabled=He();_popoverLocation;_initialized=new E;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=Mf(e,this.options,this.optionGroups),a=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=If(a.offsetTop,a.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new ab(this,e)}_scrollStrategyFactory=d(kL);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new E;_errorStateTracker;stateChanges=new E;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=ee(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(fe.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=jn(()=>{let e=this.options;return e?e.changes.pipe(Ie(e),We(()=>at(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(We(()=>this.optionSelectionChanges))});openedChange=new F;_openedStream=this.openedChange.pipe(oe(e=>e),$(()=>{}));_closedStream=this.openedChange.pipe(oe(e=>!e),$(()=>{}));selectionChange=new F;valueChange=new F;constructor(){let e=d(Bf),i=d(Nr,{optional:!0}),r=d(zt,{optional:!0}),o=d(new tr("tabindex"),{optional:!0}),a=d(nd,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Ws(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=a?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new od(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(De(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(De(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(Ie(null),De(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(ke(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&vo(this._trackedModal,"aria-owns",i),js(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;vo(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,a=this._keyManager;if(!a.isTyping()&&o&&!Ye(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let s=this.selected;a.onKeydown(e);let l=this.selected;l&&s!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,a=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!a&&(r===13||r===32)&&i.activeItem&&!Ye(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!a&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let s=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(s?l.select():l.deselect())})}else{let s=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==s&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!Ye(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof zs?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new ya(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=at(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(De(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),at(...this.options.map(i=>i._stateChanges)).pipe(De(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=$t(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&ut(o,RL,5)(o,Rn,5)(o,Xc,5),i&2){let a;H(a=U())&&(r.customTrigger=a.first),H(a=U())&&(r.options=a),H(a=U())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&qe(xL,5)(wL,5)(Ff,5),i&2){let o;H(o=U())&&(r.trigger=o.first),H(o=U())&&(r.panel=o.first),H(o=U())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&D("keydown",function(a){return r._handleKeydown(a)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(J("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),R("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",Z],disableRipple:[2,"disableRipple","disableRipple",Z],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Ei(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",Z],placeholder:"placeholder",required:[2,"required","required",Z],multiple:[2,"multiple","multiple",Z],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",Z],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",Ei],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",Z]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[we([{provide:id,useExisting:t},{provide:Kc,useExisting:t}]),je],ngContentSelectors:DL,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Me(EL),m(0,"div",2,0),D("click",function(){return r.open()}),m(3,"div",3),ge(4,ML,2,1,"span",4)(5,TL,3,1,"span",5),u(),m(6,"div",6)(7,"div",7),ni(),m(8,"svg",8),te(9,"path",9),u()()()(),A(10,AL,3,16,"ng-template",10),D("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(a){return r._handleOverlayKeydown(a)})),i&2){let o=Be(1);h(3),J("id",r._valueId),h(),_e(r.empty?4:5),h(6),v("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[zs,Ff],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return t})();var xt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Yn,Gs,Ce,Nn,Ge,Gs]})}return t})();var Gf=["*"],NL=["content"],FL=[[["mat-drawer"]],[["mat-drawer-content"]],"*"],PL=["mat-drawer","mat-drawer-content","*"];function LL(t,n){if(t&1){let e=Oe();m(0,"div",1),D("click",function(){se(e);let r=I();return le(r._onBackdropClicked())}),u()}if(t&2){let e=I();R("mat-drawer-shown",e._isShowingBackdrop())}}function jL(t,n){t&1&&(m(0,"mat-drawer-content"),V(1,2),u())}var VL=[[["mat-sidenav"]],[["mat-sidenav-content"]],"*"],BL=["mat-sidenav","mat-sidenav-content","*"];function HL(t,n){if(t&1){let e=Oe();m(0,"div",1),D("click",function(){se(e);let r=I();return le(r._onBackdropClicked())}),u()}if(t&2){let e=I();R("mat-drawer-shown",e._isShowingBackdrop())}}function UL(t,n){t&1&&(m(0,"mat-sidenav-content"),V(1,2),u())}var zL=`.mat-drawer-container {
  position: relative;
  z-index: 1;
  color: var(--mat-sidenav-content-text-color, var(--mat-sys-on-background));
  background-color: var(--mat-sidenav-content-background-color, var(--mat-sys-background));
  box-sizing: border-box;
  display: block;
  overflow: hidden;
}
.mat-drawer-container[fullscreen] {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-drawer-container[fullscreen].mat-drawer-container-has-open {
  overflow: hidden;
}
.mat-drawer-container.mat-drawer-container-explicit-backdrop .mat-drawer-side {
  z-index: 3;
}
.mat-drawer-container.ng-animate-disabled .mat-drawer-backdrop,
.mat-drawer-container.ng-animate-disabled .mat-drawer-content, .ng-animate-disabled .mat-drawer-container .mat-drawer-backdrop,
.ng-animate-disabled .mat-drawer-container .mat-drawer-content {
  transition: none;
}

.mat-drawer-backdrop {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: block;
  z-index: 3;
  visibility: hidden;
}
.mat-drawer-backdrop.mat-drawer-shown {
  visibility: visible;
  background-color: var(--mat-sidenav-scrim-color, color-mix(in srgb, var(--mat-sys-neutral-variant20) 40%, transparent));
}
.mat-drawer-transition .mat-drawer-backdrop {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: background-color, visibility;
}
@media (forced-colors: active) {
  .mat-drawer-backdrop {
    opacity: 0.5;
  }
}

.mat-drawer-content {
  position: relative;
  z-index: 1;
  display: block;
  height: 100%;
  overflow: auto;
}
.mat-drawer-content.mat-drawer-content-hidden {
  opacity: 0;
}
.mat-drawer-transition .mat-drawer-content {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: transform, margin-left, margin-right;
}

.mat-drawer {
  position: relative;
  z-index: 4;
  color: var(--mat-sidenav-container-text-color, var(--mat-sys-on-surface-variant));
  box-shadow: var(--mat-sidenav-container-elevation-shadow, none);
  background-color: var(--mat-sidenav-container-background-color, var(--mat-sys-surface));
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  width: var(--mat-sidenav-container-width, 360px);
  display: block;
  position: absolute;
  top: 0;
  bottom: 0;
  z-index: 3;
  outline: 0;
  box-sizing: border-box;
  overflow-y: auto;
  transform: translate3d(-100%, 0, 0);
}
@media (forced-colors: active) {
  .mat-drawer, [dir=rtl] .mat-drawer.mat-drawer-end {
    border-right: solid 1px currentColor;
  }
}
@media (forced-colors: active) {
  [dir=rtl] .mat-drawer, .mat-drawer.mat-drawer-end {
    border-left: solid 1px currentColor;
    border-right: none;
  }
}
.mat-drawer.mat-drawer-side {
  z-index: 2;
}
.mat-drawer.mat-drawer-end {
  right: 0;
  transform: translate3d(100%, 0, 0);
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
[dir=rtl] .mat-drawer {
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  transform: translate3d(100%, 0, 0);
}
[dir=rtl] .mat-drawer.mat-drawer-end {
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  left: 0;
  right: auto;
  transform: translate3d(-100%, 0, 0);
}
.mat-drawer-transition .mat-drawer {
  transition: transform 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) {
  visibility: hidden;
  box-shadow: none;
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) .mat-drawer-inner-container {
  display: none;
}
.mat-drawer.mat-drawer-opened.mat-drawer-opened {
  transform: none;
}

.mat-drawer-side {
  box-shadow: none;
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
}
.mat-drawer-side.mat-drawer-end {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side.mat-drawer-end {
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
  border-left: none;
}

.mat-drawer-inner-container {
  width: 100%;
  height: 100%;
  overflow: auto;
}

.mat-sidenav-fixed {
  position: fixed;
}
`;var $L=new y("MAT_DRAWER_DEFAULT_AUTOSIZE",{providedIn:"root",factory:()=>!1}),cb=new y("MAT_DRAWER_CONTAINER"),$f=(()=>{class t extends Pr{_platform=d(Ee);_changeDetectorRef=d(me);_container=d(lb);constructor(){let e=d(N),i=d(sr),r=d(P);super(e,i,r)}ngAfterContentInit(){this._container._contentMarginChanges.subscribe(()=>{this._changeDetectorRef.markForCheck()})}_shouldBeHidden(){if(this._platform.isBrowser)return!1;let{start:e,end:i}=this._container;return e!=null&&e.mode!=="over"&&e.opened||i!=null&&i.mode!=="over"&&i.opened}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-drawer-content"]],hostAttrs:[1,"mat-drawer-content"],hostVars:6,hostBindings:function(i,r){i&2&&(Hn("margin-left",r._container._contentMargins.left,"px")("margin-right",r._container._contentMargins.right,"px"),R("mat-drawer-content-hidden",r._shouldBeHidden()))},features:[we([{provide:Pr,useExisting:t}]),pe],ngContentSelectors:Gf,decls:1,vars:0,template:function(i,r){i&1&&(Me(),V(0))},encapsulation:2,changeDetection:0})}return t})(),sb=(()=>{class t{_elementRef=d(N);_focusTrapFactory=d($c);_focusMonitor=d(On);_platform=d(Ee);_ngZone=d(P);_renderer=d(Se);_interactivityChecker=d(Ps);_doc=d(W);_container=d(cb,{optional:!0});_focusTrap=null;_elementFocusedBeforeDrawerWasOpened=null;_eventCleanups;_isAttached=!1;_anchor=null;get position(){return this._position}set position(e){e=e==="end"?"end":"start",e!==this._position&&(this._isAttached&&this._updatePositionInParent(e),this._position=e,this.onPositionChanged.emit())}_position="start";get mode(){return this._mode}set mode(e){this._mode=e,this._updateFocusTrapState(),this._modeChanged.next()}_mode="over";get disableClose(){return this._disableClose}set disableClose(e){this._disableClose=Xe(e)}_disableClose=!1;get autoFocus(){let e=this._autoFocus;return e??(this.mode==="side"?"dialog":"first-tabbable")}set autoFocus(e){(e==="true"||e==="false"||e==null)&&(e=Xe(e)),this._autoFocus=e}_autoFocus;get opened(){return this._opened()}set opened(e){this.toggle(Xe(e))}_opened=ee(!1);_openedVia=null;_animationStarted=new E;_animationEnd=new E;openedChange=new F(!0);_openedStream=this.openedChange.pipe(oe(e=>e),$(()=>{}));openedStart=this._animationStarted.pipe(oe(()=>this.opened),Fa(void 0));_closedStream=this.openedChange.pipe(oe(e=>!e),$(()=>{}));closedStart=this._animationStarted.pipe(oe(()=>!this.opened),Fa(void 0));_destroyed=new E;onPositionChanged=new F;_content;_modeChanged=new E;_injector=d(G);_changeDetectorRef=d(me);constructor(){this.openedChange.pipe(De(this._destroyed)).subscribe(e=>{e?(this._elementFocusedBeforeDrawerWasOpened=this._doc.activeElement,this._takeFocus()):this._isFocusWithinDrawer()&&this._restoreFocus(this._openedVia||"program")}),this._eventCleanups=this._ngZone.runOutsideAngular(()=>{let e=this._renderer,i=this._elementRef.nativeElement;return[e.listen(i,"keydown",r=>{r.keyCode===27&&!this.disableClose&&!Ye(r)&&this._ngZone.run(()=>{this.close(),r.stopPropagation(),r.preventDefault()})}),e.listen(i,"transitionend",this._handleTransitionEvent),e.listen(i,"transitioncancel",this._handleTransitionEvent)]}),this._animationEnd.subscribe(()=>{this.openedChange.emit(this.opened)})}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_takeFocus(){if(!this._focusTrap)return;let e=this._elementRef.nativeElement;switch(this.autoFocus){case!1:case"dialog":return;case!0:case"first-tabbable":ft(()=>{!this._focusTrap.focusInitialElement()&&typeof e.focus=="function"&&e.focus()},{injector:this._injector});break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]');break;default:this._focusByCssSelector(this.autoFocus);break}}_restoreFocus(e){this.autoFocus!=="dialog"&&(this._elementFocusedBeforeDrawerWasOpened?this._focusMonitor.focusVia(this._elementFocusedBeforeDrawerWasOpened,e):this._elementRef.nativeElement.blur(),this._elementFocusedBeforeDrawerWasOpened=null)}_isFocusWithinDrawer(){let e=this._doc.activeElement;return!!e&&this._elementRef.nativeElement.contains(e)}ngAfterViewInit(){this._isAttached=!0,this._position==="end"&&this._updatePositionInParent("end"),this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._updateFocusTrapState())}ngOnDestroy(){this._eventCleanups.forEach(e=>e()),this._focusTrap?.destroy(),this._anchor?.remove(),this._anchor=null,this._animationStarted.complete(),this._animationEnd.complete(),this._modeChanged.complete(),this._destroyed.next(),this._destroyed.complete()}open(e){return this.toggle(!0,e)}close(){return this.toggle(!1)}_closeViaBackdropClick(){return this._setOpen(!1,!0,"mouse")}toggle(e=!this.opened,i){e&&i&&(this._openedVia=i);let r=this._setOpen(e,!e&&this._isFocusWithinDrawer(),this._openedVia||"program");return e||(this._openedVia=null),r}_setOpen(e,i,r){return e===this.opened?Promise.resolve(e?"open":"close"):(this._opened.set(e),this._container?._transitionsEnabled?(this._setIsAnimating(!0),setTimeout(()=>this._animationStarted.next())):setTimeout(()=>{this._animationStarted.next(),this._animationEnd.next()}),this._elementRef.nativeElement.classList.toggle("mat-drawer-opened",e),!e&&i&&this._restoreFocus(r),this._changeDetectorRef.markForCheck(),this._updateFocusTrapState(),new Promise(o=>{this.openedChange.pipe(ke(1)).subscribe(a=>o(a?"open":"close"))}))}_setIsAnimating(e){this._elementRef.nativeElement.classList.toggle("mat-drawer-animating",e)}_getWidth(){return this._elementRef.nativeElement.offsetWidth||0}_updateFocusTrapState(){this._focusTrap&&(this._focusTrap.enabled=this.opened&&!!this._container?._isShowingBackdrop())}_updatePositionInParent(e){if(!this._platform.isBrowser)return;let i=this._elementRef.nativeElement,r=i.parentNode;e==="end"?(this._anchor||(this._anchor=this._doc.createComment("mat-drawer-anchor"),r.insertBefore(this._anchor,i)),r.appendChild(i)):this._anchor&&this._anchor.parentNode.insertBefore(i,this._anchor)}_handleTransitionEvent=e=>{let i=this._elementRef.nativeElement;e.target===i&&this._ngZone.run(()=>{e.type==="transitionend"&&this._setIsAnimating(!1),this._animationEnd.next(e)})};static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-drawer"]],viewQuery:function(i,r){if(i&1&&qe(NL,5),i&2){let o;H(o=U())&&(r._content=o.first)}},hostAttrs:[1,"mat-drawer"],hostVars:12,hostBindings:function(i,r){i&2&&(J("align",null)("tabIndex",r.mode!=="side"?"-1":null),Hn("visibility",!r._container&&!r.opened?"hidden":null),R("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side"))},inputs:{position:"position",mode:"mode",disableClose:"disableClose",autoFocus:"autoFocus",opened:"opened"},outputs:{openedChange:"openedChange",_openedStream:"opened",openedStart:"openedStart",_closedStream:"closed",closedStart:"closedStart",onPositionChanged:"positionChanged"},exportAs:["matDrawer"],ngContentSelectors:Gf,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(Me(),m(0,"div",1,0),V(2),u())},dependencies:[Pr],encapsulation:2,changeDetection:0})}return t})(),lb=(()=>{class t{_dir=d(It,{optional:!0});_element=d(N);_ngZone=d(P);_changeDetectorRef=d(me);_animationDisabled=He();_transitionsEnabled=!1;_allDrawers;_drawers=new Bn;_content;_userContent;get start(){return this._start}get end(){return this._end}get autosize(){return this._autosize}set autosize(e){this._autosize=Xe(e)}_autosize=d($L);get hasBackdrop(){return this._drawerHasBackdrop(this._start)||this._drawerHasBackdrop(this._end)}set hasBackdrop(e){this._backdropOverride=e==null?null:Xe(e)}_backdropOverride=null;backdropClick=new F;_start=null;_end=null;_left=null;_right=null;_destroyed=new E;_doCheckSubject=new E;_contentMargins={left:null,right:null};_contentMarginChanges=new E;get scrollable(){return this._userContent||this._content}_injector=d(G);constructor(){let e=d(Ee),i=d(Gn);this._dir?.change.pipe(De(this._destroyed)).subscribe(()=>{this._validateDrawers(),this.updateContentMargins()}),i.change().pipe(De(this._destroyed)).subscribe(()=>this.updateContentMargins()),!this._animationDisabled&&e.isBrowser&&this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._element.nativeElement.classList.add("mat-drawer-transition"),this._transitionsEnabled=!0},200)})}ngAfterContentInit(){this._allDrawers.changes.pipe(Ie(this._allDrawers),De(this._destroyed)).subscribe(e=>{this._drawers.reset(e.filter(i=>!i._container||i._container===this)),this._drawers.notifyOnChanges()}),this._drawers.changes.pipe(Ie(null)).subscribe(()=>{this._validateDrawers(),this._drawers.forEach(e=>{this._watchDrawerToggle(e),this._watchDrawerPosition(e),this._watchDrawerMode(e)}),(!this._drawers.length||this._isDrawerOpen(this._start)||this._isDrawerOpen(this._end))&&this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),this._ngZone.runOutsideAngular(()=>{this._doCheckSubject.pipe(ui(10),De(this._destroyed)).subscribe(()=>this.updateContentMargins())})}ngOnDestroy(){this._contentMarginChanges.complete(),this._doCheckSubject.complete(),this._drawers.destroy(),this._destroyed.next(),this._destroyed.complete()}open(){this._drawers.forEach(e=>e.open())}close(){this._drawers.forEach(e=>e.close())}updateContentMargins(){let e=0,i=0;if(this._left&&this._left.opened){if(this._left.mode=="side")e+=this._left._getWidth();else if(this._left.mode=="push"){let r=this._left._getWidth();e+=r,i-=r}}if(this._right&&this._right.opened){if(this._right.mode=="side")i+=this._right._getWidth();else if(this._right.mode=="push"){let r=this._right._getWidth();i+=r,e-=r}}e=e||null,i=i||null,(e!==this._contentMargins.left||i!==this._contentMargins.right)&&(this._contentMargins={left:e,right:i},this._ngZone.run(()=>this._contentMarginChanges.next(this._contentMargins)))}ngDoCheck(){this._autosize&&this._isPushed()&&this._ngZone.runOutsideAngular(()=>this._doCheckSubject.next())}_watchDrawerToggle(e){e._animationStarted.pipe(De(this._drawers.changes)).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),e.mode!=="side"&&e.openedChange.pipe(De(this._drawers.changes)).subscribe(()=>this._setContainerClass(e.opened))}_watchDrawerPosition(e){e.onPositionChanged.pipe(De(this._drawers.changes)).subscribe(()=>{ft({read:()=>this._validateDrawers()},{injector:this._injector})})}_watchDrawerMode(e){e._modeChanged.pipe(De(at(this._drawers.changes,this._destroyed))).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()})}_setContainerClass(e){let i=this._element.nativeElement.classList,r="mat-drawer-container-has-open";e?i.add(r):i.remove(r)}_validateDrawers(){this._start=this._end=null,this._drawers.forEach(e=>{e.position=="end"?(this._end!=null,this._end=e):(this._start!=null,this._start=e)}),this._right=this._left=null,this._dir&&this._dir.value==="rtl"?(this._left=this._end,this._right=this._start):(this._left=this._start,this._right=this._end)}_isPushed(){return this._isDrawerOpen(this._start)&&this._start.mode!="over"||this._isDrawerOpen(this._end)&&this._end.mode!="over"}_onBackdropClicked(){this.backdropClick.emit(),this._closeModalDrawersViaBackdrop()}_closeModalDrawersViaBackdrop(){[this._start,this._end].filter(e=>e&&!e.disableClose&&this._drawerHasBackdrop(e)).forEach(e=>e._closeViaBackdropClick())}_isShowingBackdrop(){return this._isDrawerOpen(this._start)&&this._drawerHasBackdrop(this._start)||this._isDrawerOpen(this._end)&&this._drawerHasBackdrop(this._end)}_isDrawerOpen(e){return e!=null&&e.opened}_drawerHasBackdrop(e){return this._backdropOverride==null?!!e&&e.mode!=="side":this._backdropOverride}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-drawer-container"]],contentQueries:function(i,r,o){if(i&1&&ut(o,$f,5)(o,sb,5),i&2){let a;H(a=U())&&(r._content=a.first),H(a=U())&&(r._allDrawers=a)}},viewQuery:function(i,r){if(i&1&&qe($f,5),i&2){let o;H(o=U())&&(r._userContent=o.first)}},hostAttrs:[1,"mat-drawer-container"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-drawer-container-explicit-backdrop",r._backdropOverride)},inputs:{autosize:"autosize",hasBackdrop:"hasBackdrop"},outputs:{backdropClick:"backdropClick"},exportAs:["matDrawerContainer"],features:[we([{provide:cb,useExisting:t}])],ngContentSelectors:PL,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(Me(FL),ge(0,LL,1,2,"div",0),V(1),V(2,1),ge(3,jL,2,0,"mat-drawer-content")),i&2&&(_e(r.hasBackdrop?0:-1),h(3),_e(r._content?-1:3))},dependencies:[$f],styles:[`.mat-drawer-container {
  position: relative;
  z-index: 1;
  color: var(--mat-sidenav-content-text-color, var(--mat-sys-on-background));
  background-color: var(--mat-sidenav-content-background-color, var(--mat-sys-background));
  box-sizing: border-box;
  display: block;
  overflow: hidden;
}
.mat-drawer-container[fullscreen] {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-drawer-container[fullscreen].mat-drawer-container-has-open {
  overflow: hidden;
}
.mat-drawer-container.mat-drawer-container-explicit-backdrop .mat-drawer-side {
  z-index: 3;
}
.mat-drawer-container.ng-animate-disabled .mat-drawer-backdrop,
.mat-drawer-container.ng-animate-disabled .mat-drawer-content, .ng-animate-disabled .mat-drawer-container .mat-drawer-backdrop,
.ng-animate-disabled .mat-drawer-container .mat-drawer-content {
  transition: none;
}

.mat-drawer-backdrop {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: block;
  z-index: 3;
  visibility: hidden;
}
.mat-drawer-backdrop.mat-drawer-shown {
  visibility: visible;
  background-color: var(--mat-sidenav-scrim-color, color-mix(in srgb, var(--mat-sys-neutral-variant20) 40%, transparent));
}
.mat-drawer-transition .mat-drawer-backdrop {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: background-color, visibility;
}
@media (forced-colors: active) {
  .mat-drawer-backdrop {
    opacity: 0.5;
  }
}

.mat-drawer-content {
  position: relative;
  z-index: 1;
  display: block;
  height: 100%;
  overflow: auto;
}
.mat-drawer-content.mat-drawer-content-hidden {
  opacity: 0;
}
.mat-drawer-transition .mat-drawer-content {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: transform, margin-left, margin-right;
}

.mat-drawer {
  position: relative;
  z-index: 4;
  color: var(--mat-sidenav-container-text-color, var(--mat-sys-on-surface-variant));
  box-shadow: var(--mat-sidenav-container-elevation-shadow, none);
  background-color: var(--mat-sidenav-container-background-color, var(--mat-sys-surface));
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  width: var(--mat-sidenav-container-width, 360px);
  display: block;
  position: absolute;
  top: 0;
  bottom: 0;
  z-index: 3;
  outline: 0;
  box-sizing: border-box;
  overflow-y: auto;
  transform: translate3d(-100%, 0, 0);
}
@media (forced-colors: active) {
  .mat-drawer, [dir=rtl] .mat-drawer.mat-drawer-end {
    border-right: solid 1px currentColor;
  }
}
@media (forced-colors: active) {
  [dir=rtl] .mat-drawer, .mat-drawer.mat-drawer-end {
    border-left: solid 1px currentColor;
    border-right: none;
  }
}
.mat-drawer.mat-drawer-side {
  z-index: 2;
}
.mat-drawer.mat-drawer-end {
  right: 0;
  transform: translate3d(100%, 0, 0);
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
[dir=rtl] .mat-drawer {
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  transform: translate3d(100%, 0, 0);
}
[dir=rtl] .mat-drawer.mat-drawer-end {
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  left: 0;
  right: auto;
  transform: translate3d(-100%, 0, 0);
}
.mat-drawer-transition .mat-drawer {
  transition: transform 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) {
  visibility: hidden;
  box-shadow: none;
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) .mat-drawer-inner-container {
  display: none;
}
.mat-drawer.mat-drawer-opened.mat-drawer-opened {
  transform: none;
}

.mat-drawer-side {
  box-shadow: none;
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
}
.mat-drawer-side.mat-drawer-end {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side.mat-drawer-end {
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
  border-left: none;
}

.mat-drawer-inner-container {
  width: 100%;
  height: 100%;
  overflow: auto;
}

.mat-sidenav-fixed {
  position: fixed;
}
`],encapsulation:2,changeDetection:0})}return t})(),qf=(()=>{class t extends $f{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-sidenav-content"]],hostAttrs:[1,"mat-drawer-content","mat-sidenav-content"],features:[we([{provide:Pr,useExisting:t}]),pe],ngContentSelectors:Gf,decls:1,vars:0,template:function(i,r){i&1&&(Me(),V(0))},encapsulation:2,changeDetection:0})}return t})(),db=(()=>{class t extends sb{get fixedInViewport(){return this._fixedInViewport}set fixedInViewport(e){this._fixedInViewport=Xe(e)}_fixedInViewport=!1;get fixedTopGap(){return this._fixedTopGap}set fixedTopGap(e){this._fixedTopGap=pn(e)}_fixedTopGap=0;get fixedBottomGap(){return this._fixedBottomGap}set fixedBottomGap(e){this._fixedBottomGap=pn(e)}_fixedBottomGap=0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-sidenav"]],hostAttrs:[1,"mat-drawer","mat-sidenav"],hostVars:16,hostBindings:function(i,r){i&2&&(J("tabIndex",r.mode!=="side"?"-1":null)("align",null),Hn("top",r.fixedInViewport?r.fixedTopGap:null,"px")("bottom",r.fixedInViewport?r.fixedBottomGap:null,"px"),R("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side")("mat-sidenav-fixed",r.fixedInViewport))},inputs:{fixedInViewport:"fixedInViewport",fixedTopGap:"fixedTopGap",fixedBottomGap:"fixedBottomGap"},exportAs:["matSidenav"],features:[we([{provide:sb,useExisting:t}]),pe],ngContentSelectors:Gf,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(Me(),m(0,"div",1,0),V(2),u())},dependencies:[Pr],encapsulation:2,changeDetection:0})}return t})(),SM=(()=>{class t extends lb{_allDrawers=void 0;_content=void 0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-sidenav-container"]],contentQueries:function(i,r,o){if(i&1&&ut(o,qf,5)(o,db,5),i&2){let a;H(a=U())&&(r._content=a.first),H(a=U())&&(r._allDrawers=a)}},hostAttrs:[1,"mat-drawer-container","mat-sidenav-container"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-drawer-container-explicit-backdrop",r._backdropOverride)},exportAs:["matSidenavContainer"],features:[we([{provide:cb,useExisting:t},{provide:lb,useExisting:t}]),pe],ngContentSelectors:BL,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(Me(VL),ge(0,HL,1,2,"div",0),V(1),V(2,1),ge(3,UL,2,0,"mat-sidenav-content")),i&2&&(_e(r.hasBackdrop?0:-1),h(3),_e(r._content?-1:3))},dependencies:[qf],styles:[zL],encapsulation:2,changeDetection:0})}return t})(),wt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Nn,Ce,Nn]})}return t})();var qL=["*",[["mat-toolbar-row"]]],GL=["*","mat-toolbar-row"],WL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),TM=(()=>{class t{_elementRef=d(N);_platform=d(Ee);_document=d(W);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&ut(o,WL,5),i&2){let a;H(a=U())&&(r._toolbarRows=a)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(on(r.color?"mat-"+r.color:""),R("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:GL,decls:2,vars:0,template:function(i,r){i&1&&(Me(qL),V(0),V(1,1))},styles:[`.mat-toolbar {
  background: var(--mat-toolbar-container-background-color, var(--mat-sys-surface));
  color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--mat-toolbar-title-text-font, var(--mat-sys-title-large-font));
  font-size: var(--mat-toolbar-title-text-size, var(--mat-sys-title-large-size));
  line-height: var(--mat-toolbar-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-weight: var(--mat-toolbar-title-text-weight, var(--mat-sys-title-large-weight));
  letter-spacing: var(--mat-toolbar-title-text-tracking, var(--mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --mat-button-text-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
  --mat-button-outlined-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var Et=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Ce]})}return t})();function YL(t,n){t&1&&(m(0,"mat-error"),p(1," El n\xFAmero es obligatorio "),u())}function ZL(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;v("value",e),h(),Y(" ",e.nombreCompleto||e.nombre," ")}}function QL(t,n){t&1&&(m(0,"mat-error"),p(1," El jugador es obligatorio "),u())}function KL(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;v("value",e),h(),Y(" ",e.nombre," ")}}function XL(t,n){t&1&&(m(0,"mat-error"),p(1," El equipo es obligatorio "),u())}function JL(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;v("value",e),h(),Y(" ",e.nombre," ")}}function e2(t,n){t&1&&(m(0,"mat-error"),p(1," El tipo de cromo es obligatorio "),u())}function t2(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;v("value",e.id),h(),Y(" ",e.nombre," ")}}function n2(t,n){t&1&&(m(0,"mat-error"),p(1," La edici\xF3n es obligatoria "),u())}function i2(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;v("value",e.id),h(),kt(" ",e.nombre," (",e.temporada,") ")}}function r2(t,n){t&1&&(m(0,"mat-error"),p(1," El \xE1lbum es obligatorio "),u())}var Wf=class t{constructor(n,e,i){this.fb=n;this.http=e;this.cdr=i}fb;http;cdr;cromoForm;ediciones=[];albumes=[];equipos=[];jugadores=[];tiposCromo=[];filteredEquipos$;filteredJugadores$;filteredTiposCromo$;cargando=!1;mensaje="";error="";apiUrl="/api";ngOnInit(){this.crearFormulario(),this.cargarDatos()}crearFormulario(){this.cromoForm=this.fb.group({numero:["",fe.required],edicionId:[null,fe.required],albumId:[null,fe.required],equipo:[null,fe.required],jugador:[{value:null,disabled:!0},fe.required],tipoCromo:[null,fe.required]})}cargarDatos(){this.cargarEdiciones(),this.cargarAlbumes(),this.cargarEquipos(),this.cargarTiposCromo()}cargarEdiciones(){this.http.get(`${this.apiUrl}/Ediciones`).subscribe({next:n=>{this.ediciones=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar las ediciones."}})}cargarAlbumes(){this.http.get(`${this.apiUrl}/Albumes`).subscribe({next:n=>{this.albumes=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los \xE1lbumes."}})}cargarEquipos(){this.http.get(`${this.apiUrl}/Equipos`).subscribe({next:n=>{this.equipos=n,this.filteredEquipos$=this.cromoForm.get("equipo").valueChanges.pipe(Ie(""),$(e=>this.filtrarEquipos(e))),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los equipos."}})}filtrarEquipos(n){let e=(typeof n=="string"?n:"").toLowerCase();return this.equipos.filter(i=>i.nombre.toLowerCase().includes(e))}onEquipoInput(n){this.cromoForm.get("equipo")?.setValue(n)}onEquipoSelected(n){this.cromoForm.get("equipo")?.setValue(n),this.onEquipoChange()}displayEquipo(n){return n?.nombre??""}onEquipoChange(){let e=this.cromoForm.get("equipo")?.value?.id;if(this.jugadores=[],this.cromoForm.patchValue({jugador:null}),!e){this.cromoForm.get("jugador")?.disable();return}this.http.get(`${this.apiUrl}/Equipos/${e}/Jugadores`).subscribe({next:i=>{this.jugadores=i,this.filteredJugadores$=this.cromoForm.get("jugador").valueChanges.pipe(Ie(""),$(r=>this.filtrarJugadores(r))),this.cromoForm.get("jugador")?.enable(),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los jugadores."}})}filtrarJugadores(n){let e=(typeof n=="string"?n:"").toLowerCase();return this.jugadores.filter(i=>(i.nombreCompleto||i.nombre).toLowerCase().includes(e))}onJugadorInput(n){this.cromoForm.get("jugador")?.setValue(n)}onJugadorSelected(n){this.cromoForm.get("jugador")?.setValue(n)}displayJugador(n){return n?typeof n=="string"?n:n.nombreCompleto||n.nombre:""}cargarTiposCromo(){this.http.get(`${this.apiUrl}/TiposCromo`).subscribe({next:n=>{this.tiposCromo=n,this.filteredTiposCromo$=this.cromoForm.get("tipoCromo").valueChanges.pipe(Ie(""),$(e=>this.filtrarTiposCromo(e))),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los tipos de cromo."}})}filtrarTiposCromo(n){let e=(typeof n=="string"?n:n?.nombre??"").toLowerCase();return this.tiposCromo.filter(i=>i.nombre.toLowerCase().includes(e))}onTipoCromoInput(n){this.cromoForm.get("tipoCromo")?.setValue(n)}onTipoCromoSelected(n){this.cromoForm.get("tipoCromo")?.setValue(n)}displayTipoCromo(n){return n?.nombre??""}guardar(){if(this.cromoForm.invalid){this.cromoForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n={numero:this.cromoForm.get("numero")?.value,edicionId:this.cromoForm.get("edicionId")?.value,albumId:this.cromoForm.get("albumId")?.value,equipoId:this.cromoForm.get("equipo")?.value?.id,jugadorId:this.cromoForm.get("jugador")?.value?.id,tipoCromoId:this.cromoForm.get("tipoCromo")?.value?.id};this.http.post(`${this.apiUrl}/Cromo`,n).subscribe({next:()=>{this.cargando=!1,this.mensaje="Cromo a\xF1adido correctamente.",this.cromoForm.patchValue({jugador:null}),this.cromoForm.get("jugador")?.disable(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el cromo."}})}cancelar(){this.cromoForm.reset(),this.mensaje="",this.error="",this.cromoForm.get("jugador")?.disable(),this.cdr.detectChanges()}static \u0275fac=function(e){return new(e||t)(M(kn),M(Je),M(me))};static \u0275cmp=T({type:t,selectors:[["app-cromos"]],decls:80,vars:28,consts:[["autoJugador","matAutocomplete"],["autoEquipo","matAutocomplete"],["autoTipoCromo","matAutocomplete"],[1,"page-container"],[1,"cromo-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],[1,"form-grid"],["appearance","outline"],["matInput","","type","text","formControlName","numero","placeholder","Ej. 108"],["matSuffix",""],[4,"ngIf"],["matInput","","type","text","formControlName","jugador","placeholder","Busca un jugador...",3,"input","matAutocomplete"],[3,"optionSelected","displayWith"],[3,"value",4,"ngFor","ngForOf"],["matInput","","type","text","formControlName","equipo","placeholder","Busca un equipo...",3,"matAutocomplete"],["matInput","","type","text","formControlName","tipoCromo","placeholder","Busca un tipo...",3,"input","matAutocomplete"],["formControlName","edicionId"],[3,"value"],["formControlName","albumId"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"]],template:function(e,i){if(e&1&&(m(0,"div",3)(1,"mat-card",4)(2,"mat-card-header")(3,"div",5)(4,"mat-icon"),p(5,"style"),u()(),m(6,"mat-card-title"),p(7,"A\xF1adir nuevo cromo"),u(),m(8,"mat-card-subtitle"),p(9," Completa los datos del cromo que quieres a\xF1adir "),u()(),m(10,"mat-card-content")(11,"form",6),D("ngSubmit",function(){return i.guardar()}),m(12,"div",7)(13,"mat-form-field",8)(14,"mat-label"),p(15,"N\xFAmero del cromo"),u(),te(16,"input",9),m(17,"mat-icon",10),p(18,"numbers"),u(),A(19,YL,2,0,"mat-error",11),u(),m(20,"mat-form-field",8)(21,"mat-label"),p(22,"Jugador"),u(),m(23,"input",12),D("input",function(o){return i.onJugadorInput(o.target.value)}),u(),m(24,"mat-autocomplete",13,0),D("optionSelected",function(o){return i.onJugadorSelected(o.option.value)}),A(26,ZL,2,2,"mat-option",14),Sr(27,"async"),u(),m(28,"mat-icon",10),p(29,"person"),u(),A(30,QL,2,0,"mat-error",11),u(),m(31,"mat-form-field",8)(32,"mat-label"),p(33,"Equipo"),u(),te(34,"input",15),m(35,"mat-autocomplete",13,1),D("optionSelected",function(o){return i.onEquipoSelected(o.option.value)}),A(37,KL,2,2,"mat-option",14),Sr(38,"async"),u(),m(39,"mat-icon",10),p(40,"groups"),u(),A(41,XL,2,0,"mat-error",11),u(),m(42,"mat-form-field",8)(43,"mat-label"),p(44,"Tipo de cromo"),u(),m(45,"input",16),D("input",function(o){return i.onTipoCromoInput(o.target.value)}),u(),m(46,"mat-autocomplete",13,2),D("optionSelected",function(o){return i.onTipoCromoSelected(o.option.value)}),A(48,JL,2,2,"mat-option",14),Sr(49,"async"),u(),m(50,"mat-icon",10),p(51,"category"),u(),A(52,e2,2,0,"mat-error",11),u(),m(53,"mat-form-field",8)(54,"mat-label"),p(55,"Edici\xF3n"),u(),m(56,"mat-select",17)(57,"mat-option",18),p(58," Selecciona una edici\xF3n "),u(),A(59,t2,2,2,"mat-option",14),u(),m(60,"mat-icon",10),p(61,"collections_bookmark"),u(),A(62,n2,2,0,"mat-error",11),u(),m(63,"mat-form-field",8)(64,"mat-label"),p(65,"\xC1lbum"),u(),m(66,"mat-select",19)(67,"mat-option",18),p(68," Selecciona un \xE1lbum "),u(),A(69,i2,2,3,"mat-option",14),u(),m(70,"mat-icon",10),p(71,"album"),u(),A(72,r2,2,0,"mat-error",11),u()(),m(73,"mat-card-actions",20)(74,"button",21),D("click",function(){return i.cancelar()}),p(75," Cancelar "),u(),m(76,"button",22)(77,"mat-icon"),p(78,"save"),u(),p(79),u()()()()()()),e&2){let r,o,a,s,l,c,f=Be(25),g=Be(36),_=Be(47);h(11),v("formGroup",i.cromoForm),h(8),v("ngIf",((r=i.cromoForm.get("numero"))==null?null:r.touched)&&((r=i.cromoForm.get("numero"))==null?null:r.hasError("required"))),h(4),v("matAutocomplete",f),h(),v("displayWith",i.displayJugador),h(2),v("ngForOf",Tr(27,22,i.filteredJugadores$)),h(4),v("ngIf",((o=i.cromoForm.get("jugador"))==null?null:o.touched)&&((o=i.cromoForm.get("jugador"))==null?null:o.hasError("required"))),h(4),v("matAutocomplete",g),h(),v("displayWith",i.displayEquipo),h(2),v("ngForOf",Tr(38,24,i.filteredEquipos$)),h(4),v("ngIf",((a=i.cromoForm.get("equipo"))==null?null:a.touched)&&((a=i.cromoForm.get("equipo"))==null?null:a.hasError("required"))),h(4),v("matAutocomplete",_),h(),v("displayWith",i.displayTipoCromo),h(2),v("ngForOf",Tr(49,26,i.filteredTiposCromo$)),h(4),v("ngIf",((s=i.cromoForm.get("tipoCromo"))==null?null:s.touched)&&((s=i.cromoForm.get("tipoCromo"))==null?null:s.hasError("required"))),h(5),v("value",null),h(2),v("ngForOf",i.ediciones),h(3),v("ngIf",((l=i.cromoForm.get("edicionId"))==null?null:l.touched)&&((l=i.cromoForm.get("edicionId"))==null?null:l.hasError("required"))),h(5),v("value",null),h(2),v("ngForOf",i.albumes),h(3),v("ngIf",((c=i.cromoForm.get("albumId"))==null?null:c.touched)&&((c=i.cromoForm.get("albumId"))==null?null:c.hasError("required"))),h(4),v("disabled",i.cargando),h(3),Y(" ",i.cargando?"Guardando...":"Guardar cromo"," ")}},dependencies:[Rt,Ot,Ut,fn,Jt,pt,Nt,Xt,zt,wn,gt,Vf,Rn,rd,_t,Pt,mt,et,Ft,hn,bt,vt,tt,Lt,gn,Pn,jt,nn,tn,en,nt,Ge,rt,it,yt,Ct,xt,xo,wt,Et,rc],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa;display:flex;justify-content:center;align-items:flex-start}.cromo-card[_ngcontent-%COMP%]{width:100%;max-width:750px;border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:24px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}.form-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 20px}.full-width[_ngcontent-%COMP%]{grid-column:1/-1}mat-form-field[_ngcontent-%COMP%]{width:100%}mat-card-actions[_ngcontent-%COMP%]{padding:16px 24px 24px;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}.form-grid[_ngcontent-%COMP%]{grid-template-columns:1fr}.full-width[_ngcontent-%COMP%]{grid-column:auto}mat-card-title[_ngcontent-%COMP%]{font-size:21px!important}}"]})};var cr=class t{constructor(n){this.http=n}http;apiUrl="/api/auth";userSignal=ee(this.loadUser());user=this.userSignal.asReadonly();isLoggedIn=an(()=>!!this.userSignal());login(n,e){return this.http.post(`${this.apiUrl}/login`,{email:n,contrasena:e}).pipe(st(i=>this.storeSession(i)))}register(n,e,i){return this.http.post(`${this.apiUrl}/register`,{nombre:n,email:e,contrasena:i}).pipe(st(r=>this.storeSession(r)))}logout(){sessionStorage.removeItem("auth_token"),sessionStorage.removeItem("auth_user"),this.userSignal.set(null)}getToken(){return sessionStorage.getItem("auth_token")}storeSession(n){sessionStorage.setItem("auth_token",n.token),sessionStorage.setItem("auth_user",JSON.stringify({nombre:n.nombre,email:n.email})),this.userSignal.set({nombre:n.nombre,email:n.email})}loadUser(){let n=sessionStorage.getItem("auth_user");return n?JSON.parse(n):null}static \u0275fac=function(e){return new(e||t)(K(Je))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};var o2=()=>({exact:!0});function a2(t,n){if(t&1){let e=Oe();m(0,"button",19)(1,"mat-icon"),p(2,"account_circle"),u(),p(3),u(),m(4,"mat-menu",null,1)(6,"button",20)(7,"mat-icon"),p(8,"email"),u(),m(9,"span"),p(10),u()(),te(11,"mat-divider"),m(12,"button",21),D("click",function(){se(e);let r=I();return le(r.logout())}),m(13,"mat-icon"),p(14,"logout"),u(),m(15,"span"),p(16,"Cerrar sesi\xF3n"),u()()()}if(t&2){let e=n,i=Be(5);v("matMenuTriggerFor",i),h(3),Y(" ",e.nombre," "),h(7),B(e.email)}}var Yf=class t{auth=d(cr);router=d(xn);logout(){this.auth.logout(),this.router.navigate(["/login"])}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-layout"]],decls:57,vars:3,consts:[["sidenav",""],["userMenu","matMenu"],[1,"sidenav-container"],["mode","side","opened",""],[1,"logo"],["mat-list-item","","routerLink","/","routerLinkActive","active",3,"routerLinkActiveOptions"],["matListItemIcon",""],["matListItemTitle",""],[1,"nav-section"],["mat-list-item","","routerLink","/cromos","routerLinkActive","active"],["mat-list-item","","routerLink","/jugadores","routerLinkActive","active"],["mat-list-item","","routerLink","/equipos","routerLinkActive","active"],["mat-list-item","","routerLink","/tipos-cromo","routerLinkActive","active"],["mat-list-item","","routerLink","/albumes","routerLinkActive","active"],["mat-list-item","","routerLink","/editoriales","routerLinkActive","active"],["mat-icon-button","",3,"click"],[1,"toolbar-title"],[1,"toolbar-spacer"],[1,"content"],["mat-button","",3,"matMenuTriggerFor"],["mat-menu-item","","disabled",""],["mat-menu-item","",3,"click"]],template:function(e,i){if(e&1){let r=Oe();m(0,"mat-sidenav-container",2)(1,"mat-sidenav",3,0)(3,"div",4)(4,"mat-icon"),p(5,"style"),u(),m(6,"span"),p(7,"CromosList"),u()(),m(8,"mat-nav-list")(9,"a",5)(10,"mat-icon",6),p(11,"dashboard"),u(),m(12,"span",7),p(13,"Panel"),u()(),m(14,"h3",8),p(15,"Cat\xE1logo"),u(),m(16,"a",9)(17,"mat-icon",6),p(18,"style"),u(),m(19,"span",7),p(20,"Cromos"),u()(),m(21,"a",10)(22,"mat-icon",6),p(23,"person"),u(),m(24,"span",7),p(25,"Jugadores"),u()(),m(26,"a",11)(27,"mat-icon",6),p(28,"groups"),u(),m(29,"span",7),p(30,"Equipos"),u()(),m(31,"a",12)(32,"mat-icon",6),p(33,"category"),u(),m(34,"span",7),p(35,"Tipos de cromo"),u()(),m(36,"a",13)(37,"mat-icon",6),p(38,"album"),u(),m(39,"span",7),p(40,"\xC1lbumes"),u()(),m(41,"a",14)(42,"mat-icon",6),p(43,"storefront"),u(),m(44,"span",7),p(45,"Editoriales"),u()()()(),m(46,"mat-sidenav-content")(47,"mat-toolbar")(48,"button",15),D("click",function(){se(r);let a=Be(2);return le(a.toggle())}),m(49,"mat-icon"),p(50,"menu"),u()(),m(51,"span",16),p(52," CromosList "),u(),te(53,"span",17),ge(54,a2,17,3),u(),m(55,"main",18),te(56,"router-outlet"),u()()()}if(e&2){let r;h(9),v("routerLinkActiveOptions",__(2,o2)),h(45),_e((r=i.auth.user())?54:-1,r)}},dependencies:[fa,mn,_v,bt,vt,Zn,gt,tt,nt,gM,Ge,rt,it,_t,yt,MM,ci,Kn,Qn,Ct,Zs,cd,IM,xt,wt,db,SM,qf,Et,TM],styles:[".sidenav-container[_ngcontent-%COMP%]{height:100vh}mat-sidenav[_ngcontent-%COMP%]{width:250px;background:var(--app-surface);border-right:1px solid var(--app-border)}.logo[_ngcontent-%COMP%]{height:64px;display:flex;align-items:center;gap:12px;padding:0 20px;font-size:20px;font-weight:700;color:var(--app-primary-dark);letter-spacing:.2px}.logo[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:28px;width:28px;height:28px;color:var(--app-green)}mat-nav-list[_ngcontent-%COMP%]{padding-top:6px}.nav-section[_ngcontent-%COMP%]{margin:20px 22px 6px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--app-text-muted)}mat-nav-list[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{margin:3px 10px;border-radius:10px;height:46px}mat-nav-list[_ngcontent-%COMP%]   a.active[_ngcontent-%COMP%]{background:var(--app-primary-soft);color:var(--app-primary-dark)}mat-nav-list[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover{background:var(--app-green-soft)}mat-nav-list[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--app-text-muted)}mat-nav-list[_ngcontent-%COMP%]   a.active[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--app-primary)}mat-toolbar[_ngcontent-%COMP%]{background:var(--app-surface);color:var(--app-primary-dark);border-bottom:1px solid var(--app-border)}mat-toolbar[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--app-primary)}.toolbar-title[_ngcontent-%COMP%]{margin-left:10px;font-weight:700}.toolbar-spacer[_ngcontent-%COMP%]{flex:1}.content[_ngcontent-%COMP%]{padding:24px;min-height:calc(100vh - 64px);background:var(--app-bg)}"]})};var dr=class t{http=d(Je);apiUrl="/api/Equipos";getEquipos(){return this.http.get(this.apiUrl)}crearEquipo(n){return this.http.post(this.apiUrl,n)}editarEquipo(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};function s2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function l2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function c2(t,n){if(t&1){let e=Oe();m(0,"mat-list-item")(1,"mat-icon",15),p(2,"groups"),u(),m(3,"span",16),p(4),u(),m(5,"button",17),D("click",function(){let r=se(e).$implicit,o=I();return le(o.editar(r))}),m(6,"mat-icon"),p(7,"edit"),u()()()}if(t&2){let e=n.$implicit,i=I();h(4),B(e.nombre),h(),v("disabled",i.esModoEdicion)}}function d2(t,n){t&1&&(m(0,"mat-list-item")(1,"span",16),p(2,"No hay equipos registrados."),u()())}var Qf=class t{constructor(n,e){this.fb=n;this.equipoService=e}fb;equipoService;equipoForm;equipos=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarEquipos()}crearFormulario(){this.equipoForm=this.fb.group({nombre:["",[fe.required,fe.maxLength(100)]]})}cargarEquipos(){this.equipoService.getEquipos().subscribe({next:n=>{this.equipos=n},error:()=>{this.error="No se han podido cargar los equipos."}})}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.equipoForm.patchValue({nombre:n.nombre}),this.mensaje="",this.error=""}guardar(){if(this.equipoForm.invalid){this.equipoForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n=this.equipoForm.get("nombre")?.value;this.esModoEdicion?this.equipoService.editarEquipo(this.editandoId,{nombre:n}).subscribe({next:e=>{this.cargando=!1;let i=this.equipos.findIndex(r=>r.id===this.editandoId);i!==-1&&(this.equipos[i]=e),this.mensaje="Equipo actualizado correctamente.",this.cancelar()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el equipo."}}):this.equipoService.crearEquipo({nombre:n}).subscribe({next:e=>{this.cargando=!1,this.equipos.push(e),this.mensaje="Equipo a\xF1adido correctamente.",this.equipoForm.reset()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el equipo."}})}cancelar(){this.editandoId=null,this.equipoForm.reset(),this.mensaje="",this.error=""}static \u0275fac=function(e){return new(e||t)(M(kn),M(dr))};static \u0275cmp=T({type:t,selectors:[["app-equipos"]],decls:43,vars:16,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. Real Madrid"],["matSuffix",""],[4,"ngIf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],["matListItemIcon",""],["matListItemTitle",""],["mat-icon-button","","matListItemMeta","",3,"click","disabled"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),D("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Nombre"),u(),te(16,"input",6),m(17,"mat-icon",7),p(18,"badge"),u(),A(19,s2,2,0,"mat-error",8)(20,l2,2,0,"mat-error",8),u(),m(21,"div",9),p(22),u(),m(23,"mat-card-actions",10)(24,"button",11),D("click",function(){return i.cancelar()}),p(25),u(),m(26,"button",12)(27,"mat-icon"),p(28),u(),p(29),u()()()()(),m(30,"mat-card",13)(31,"mat-card-header")(32,"div",3)(33,"mat-icon"),p(34,"list"),u()(),m(35,"mat-card-title"),p(36,"Equipos registrados"),u(),m(37,"mat-card-subtitle"),p(38," Equipos existentes en la base de datos "),u()(),m(39,"mat-card-content")(40,"mat-list"),A(41,c2,8,2,"mat-list-item",14)(42,d2,3,0,"mat-list-item",8),u()()()()()),e&2){let r,o;h(6),B(i.esModoEdicion?"edit":"groups"),h(2),B(i.esModoEdicion?"Editar equipo":"A\xF1adir nuevo equipo"),h(2),Y(" ",i.esModoEdicion?"Modifica el nombre del equipo":"Completa los datos del equipo que quieres a\xF1adir"," "),h(2),v("formGroup",i.equipoForm),h(7),v("ngIf",(r=i.equipoForm.get("nombre"))==null?null:r.hasError("required")),h(),v("ngIf",(o=i.equipoForm.get("nombre"))==null?null:o.hasError("maxlength")),h(),R("error",!!i.error),h(),kt(" ",i.mensaje,"",i.error," "),h(3),Y(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),v("disabled",i.cargando),h(2),B(i.esModoEdicion?"save":"add"),h(),Y(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar equipo"," "),h(12),v("ngForOf",i.equipos),h(),v("ngIf",i.equipos.length===0)}},dependencies:[Rt,Ot,Ut,fn,Jt,pt,Nt,Xt,zt,wn,bt,vt,Zn,gt,tt,Lt,gn,Pn,jt,nn,tn,en,nt,Ge,mt,et,Ft,hn,rt,it,_t,Pt,yt,lr,ci,Kn,Qn,Li,Ct,xt,wt,Et],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:#2e7d32;font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var Kf=class t{http=d(Je);apiUrl="/api/Jugadores";getJugadores(){return this.http.get(this.apiUrl)}crearJugador(n){return this.http.post(this.apiUrl,n)}editarJugador(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}eliminarJugador(n){return this.http.delete(`${this.apiUrl}/${n}`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};var m2=(t,n)=>e=>e.nombre;function f2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function p2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function h2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 150 caracteres "),u())}function g2(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;v("value",e.id),h(),Y(" ",e.nombre," ")}}function _2(t,n){t&1&&(m(0,"mat-error"),p(1," Debes seleccionar al menos un equipo "),u())}function v2(t,n){if(t&1){let e=Oe();m(0,"mat-list-item")(1,"mat-icon",19),p(2,"person"),u(),m(3,"span",20),p(4),u(),m(5,"span",21),p(6),u(),m(7,"button",22),D("click",function(){let r=se(e).$implicit,o=I();return le(o.editar(r))}),m(8,"mat-icon"),p(9,"edit"),u()(),m(10,"button",22),D("click",function(){let r=se(e).$implicit,o=I();return le(o.eliminar(r))}),m(11,"mat-icon"),p(12,"delete"),u()()()}if(t&2){let e=n.$implicit,i=I();h(4),B(e.nombre),h(2),B(e.equipos.map(ss(4,m2,n)).join(", ")),h(),v("disabled",i.esModoEdicion),h(3),v("disabled",i.esModoEdicion)}}function b2(t,n){t&1&&(m(0,"mat-list-item")(1,"span",20),p(2,"No hay jugadores registrados."),u()())}var Xf=class t{constructor(n,e,i,r){this.fb=n;this.equipoService=e;this.jugadorService=i;this.cdr=r}fb;equipoService;jugadorService;cdr;jugadorForm;equipos=[];jugadores=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarEquipos(),this.cargarJugadores()}crearFormulario(){this.jugadorForm=this.fb.group({nombre:["",[fe.required,fe.maxLength(100)]],nombreCompleto:["",[fe.maxLength(150)]],equipos:[[],[fe.required]]})}cargarEquipos(){this.equipoService.getEquipos().subscribe({next:n=>{this.equipos=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los equipos.",this.cdr.detectChanges()}})}cargarJugadores(){this.jugadorService.getJugadores().subscribe({next:n=>{this.jugadores=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los jugadores.",this.cdr.detectChanges()}})}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.jugadorForm.patchValue({nombre:n.nombre,nombreCompleto:n.nombreCompleto,equipos:n.equipos.map(e=>e.id)}),this.mensaje="",this.error=""}guardar(){if(this.jugadorForm.invalid){this.jugadorForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n={nombre:this.jugadorForm.get("nombre")?.value,nombreCompleto:this.jugadorForm.get("nombreCompleto")?.value,equipos:this.jugadorForm.get("equipos")?.value};this.esModoEdicion?this.jugadorService.editarJugador(this.editandoId,n).subscribe({next:e=>{this.cargando=!1;let i=this.jugadores.findIndex(r=>r.id===this.editandoId);i!==-1&&(this.jugadores[i]=e),this.mensaje="Jugador actualizado correctamente.",this.cancelar(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el jugador.",this.cdr.detectChanges()}}):this.jugadorService.crearJugador(n).subscribe({next:e=>{this.cargando=!1,this.jugadores.push(e),this.mensaje="Jugador a\xF1adido correctamente.",this.jugadorForm.reset(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el jugador.",this.cdr.detectChanges()}})}eliminar(n){confirm(`\xBFSeguro que quieres eliminar el jugador "${n.nombre}"?`)&&this.jugadorService.eliminarJugador(n.id).subscribe({next:()=>{this.jugadores=this.jugadores.filter(e=>e.id!==n.id),this.mensaje="Jugador eliminado correctamente.",this.error="",this.cdr.detectChanges()},error:()=>{this.error="No se ha podido eliminar el jugador.",this.cdr.detectChanges()}})}cancelar(){this.editandoId=null,this.jugadorForm.reset({nombre:"",nombreCompleto:"",equipos:[]}),this.mensaje="",this.error=""}static \u0275fac=function(e){return new(e||t)(M(kn),M(dr),M(Kf),M(me))};static \u0275cmp=T({type:t,selectors:[["app-jugadores"]],decls:58,vars:19,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. Lamine Yamal"],["matSuffix",""],[4,"ngIf"],["matInput","","type","text","formControlName","nombreCompleto","placeholder","Ej. Lamine Yamal Nasraoui Ebana"],["formControlName","equipos","multiple",""],[3,"value",4,"ngFor","ngForOf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],[3,"value"],["matListItemIcon",""],["matListItemTitle",""],["matListItemLine",""],["mat-icon-button","","matListItemMeta","",3,"click","disabled"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),D("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Nombre"),u(),te(16,"input",6),m(17,"mat-icon",7),p(18,"badge"),u(),A(19,f2,2,0,"mat-error",8)(20,p2,2,0,"mat-error",8),u(),m(21,"mat-form-field",5)(22,"mat-label"),p(23,"Nombre completo"),u(),te(24,"input",9),m(25,"mat-icon",7),p(26,"badge"),u(),A(27,h2,2,0,"mat-error",8),u(),m(28,"mat-form-field",5)(29,"mat-label"),p(30,"Equipos"),u(),m(31,"mat-select",10),A(32,g2,2,2,"mat-option",11),u(),m(33,"mat-icon",7),p(34,"groups"),u(),A(35,_2,2,0,"mat-error",8),u(),m(36,"div",12),p(37),u(),m(38,"mat-card-actions",13)(39,"button",14),D("click",function(){return i.cancelar()}),p(40),u(),m(41,"button",15)(42,"mat-icon"),p(43),u(),p(44),u()()()()(),m(45,"mat-card",16)(46,"mat-card-header")(47,"div",3)(48,"mat-icon"),p(49,"list"),u()(),m(50,"mat-card-title"),p(51,"Jugadores registrados"),u(),m(52,"mat-card-subtitle"),p(53," Jugadores existentes en la base de datos "),u()(),m(54,"mat-card-content")(55,"mat-list"),A(56,v2,13,5,"mat-list-item",17)(57,b2,3,0,"mat-list-item",8),u()()()()()),e&2){let r,o,a,s;h(6),B(i.esModoEdicion?"edit":"person"),h(2),B(i.esModoEdicion?"Editar jugador":"A\xF1adir nuevo jugador"),h(2),Y(" ",i.esModoEdicion?"Modifica los datos del jugador":"Completa los datos del jugador que quieres a\xF1adir"," "),h(2),v("formGroup",i.jugadorForm),h(7),v("ngIf",(r=i.jugadorForm.get("nombre"))==null?null:r.hasError("required")),h(),v("ngIf",(o=i.jugadorForm.get("nombre"))==null?null:o.hasError("maxlength")),h(7),v("ngIf",(a=i.jugadorForm.get("nombreCompleto"))==null?null:a.hasError("maxlength")),h(5),v("ngForOf",i.equipos),h(3),v("ngIf",(s=i.jugadorForm.get("equipos"))==null?null:s.hasError("required")),h(),R("error",!!i.error),h(),kt(" ",i.mensaje,"",i.error," "),h(3),Y(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),v("disabled",i.cargando),h(2),B(i.esModoEdicion?"save":"add"),h(),Y(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar jugador"," "),h(12),v("ngForOf",i.jugadores),h(),v("ngIf",i.jugadores.length===0)}},dependencies:[Rt,Ot,Ut,fn,Jt,pt,Nt,Xt,zt,wn,bt,vt,Zn,gt,Rn,tt,Lt,gn,Pn,jt,nn,tn,en,nt,Ge,mt,et,Ft,hn,rt,it,_t,Pt,yt,lr,ci,Kn,ld,Qn,Li,Ct,xt,xo,wt,Et],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:#2e7d32;font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var wo=class t{http=d(Je);apiUrl="/api/TiposCromo";getTiposCromo(){return this.http.get(this.apiUrl)}crearTipoCromo(n){return this.http.post(this.apiUrl,n)}editarTipoCromo(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}eliminarTipoCromo(n){return this.http.delete(`${this.apiUrl}/${n}`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};function y2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function C2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function x2(t,n){if(t&1){let e=Oe();m(0,"mat-list-item")(1,"mat-icon",15),p(2,"category"),u(),m(3,"span",16),p(4),u(),m(5,"button",17),D("click",function(){let r=se(e).$implicit,o=I();return le(o.editar(r))}),m(6,"mat-icon"),p(7,"edit"),u()(),m(8,"button",18),D("click",function(){let r=se(e).$implicit,o=I();return le(o.eliminar(r))}),m(9,"mat-icon",19),p(10,"delete"),u()()()}if(t&2){let e=n.$implicit,i=I();h(4),B(e.nombre),h(),v("disabled",i.esModoEdicion)}}function w2(t,n){t&1&&(m(0,"mat-list-item")(1,"span",16),p(2,"No hay tipos de cromo registrados."),u()())}var Jf=class t{constructor(n,e){this.fb=n;this.tipoCromoService=e}fb;tipoCromoService;tipoCromoForm;tiposCromo=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarTiposCromo()}crearFormulario(){this.tipoCromoForm=this.fb.group({nombre:["",[fe.required,fe.maxLength(100)]]})}cargarTiposCromo(){this.tipoCromoService.getTiposCromo().subscribe({next:n=>{this.tiposCromo=n},error:()=>{this.error="No se han podido cargar los tipos de cromo."}})}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.tipoCromoForm.patchValue({nombre:n.nombre}),this.mensaje="",this.error=""}guardar(){if(this.tipoCromoForm.invalid){this.tipoCromoForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n=this.tipoCromoForm.get("nombre")?.value;this.esModoEdicion?this.tipoCromoService.editarTipoCromo(this.editandoId,{nombre:n}).subscribe({next:e=>{this.cargando=!1;let i=this.tiposCromo.findIndex(r=>r.id===this.editandoId);i!==-1&&(this.tiposCromo[i]=e),this.mensaje="Tipo de cromo actualizado correctamente.",this.cancelar()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el tipo de cromo."}}):this.tipoCromoService.crearTipoCromo({nombre:n}).subscribe({next:e=>{this.cargando=!1,this.tiposCromo.push(e),this.mensaje="Tipo de cromo a\xF1adido correctamente.",this.tipoCromoForm.reset()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el tipo de cromo."}})}eliminar(n){this.tipoCromoService.eliminarTipoCromo(n.id).subscribe({next:()=>{this.tiposCromo=this.tiposCromo.filter(e=>e.id!==n.id),this.mensaje="Tipo de cromo eliminado correctamente.",this.error=""},error:()=>{this.mensaje="",this.error="No se ha podido eliminar el tipo de cromo."}})}cancelar(){this.editandoId=null,this.tipoCromoForm.reset(),this.mensaje="",this.error=""}static \u0275fac=function(e){return new(e||t)(M(kn),M(wo))};static \u0275cmp=T({type:t,selectors:[["app-tipos-cromo"]],decls:43,vars:16,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. Normal, Caja, Flor, Hologr\xE1fico..."],["matSuffix",""],[4,"ngIf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],["matListItemIcon",""],["matListItemTitle",""],["mat-icon-button","","matListItemMeta","",3,"click","disabled"],["mat-icon-button","","matListItemMeta","",3,"click"],["color","warn"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),D("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Nombre"),u(),te(16,"input",6),m(17,"mat-icon",7),p(18,"sell"),u(),A(19,y2,2,0,"mat-error",8)(20,C2,2,0,"mat-error",8),u(),m(21,"div",9),p(22),u(),m(23,"mat-card-actions",10)(24,"button",11),D("click",function(){return i.cancelar()}),p(25),u(),m(26,"button",12)(27,"mat-icon"),p(28),u(),p(29),u()()()()(),m(30,"mat-card",13)(31,"mat-card-header")(32,"div",3)(33,"mat-icon"),p(34,"list"),u()(),m(35,"mat-card-title"),p(36,"Tipos de cromo registrados"),u(),m(37,"mat-card-subtitle"),p(38," Tipos de cromo existentes en la base de datos "),u()(),m(39,"mat-card-content")(40,"mat-list"),A(41,x2,11,2,"mat-list-item",14)(42,w2,3,0,"mat-list-item",8),u()()()()()),e&2){let r,o;h(6),B(i.esModoEdicion?"edit":"category"),h(2),B(i.esModoEdicion?"Editar tipo de cromo":"A\xF1adir nuevo tipo de cromo"),h(2),Y(" ",i.esModoEdicion?"Modifica el nombre del tipo de cromo":"Completa los datos del tipo de cromo que quieres a\xF1adir"," "),h(2),v("formGroup",i.tipoCromoForm),h(7),v("ngIf",(r=i.tipoCromoForm.get("nombre"))==null?null:r.hasError("required")),h(),v("ngIf",(o=i.tipoCromoForm.get("nombre"))==null?null:o.hasError("maxlength")),h(),R("error",!!i.error),h(),kt(" ",i.mensaje,"",i.error," "),h(3),Y(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),v("disabled",i.cargando),h(2),B(i.esModoEdicion?"save":"add"),h(),Y(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar tipo"," "),h(12),v("ngForOf",i.tiposCromo),h(),v("ngIf",i.tiposCromo.length===0)}},dependencies:[Rt,Ot,Ut,fn,Jt,pt,Nt,Xt,zt,wn,bt,vt,Zn,gt,tt,Lt,gn,Pn,jt,nn,tn,en,nt,Ge,mt,et,Ft,hn,rt,it,_t,Pt,yt,lr,ci,Kn,Qn,Li,Ct,xt,wt,Et],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:#2e7d32;font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var E2=["tooltip"],D2=20;var M2=new y("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(G);return()=>Ai(t,{scrollThrottle:D2})}}),I2=new y("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var AM="tooltip-panel",S2={passive:!0},T2=8,A2=8,k2=24,O2=200,mb=(()=>{class t{_elementRef=d(N);_ngZone=d(P);_platform=d(Ee);_ariaDescriber=d(xD);_focusMonitor=d(On);_dir=d(It);_injector=d(G);_viewContainerRef=d(ct);_mediaMatcher=d(Ns);_document=d(W);_renderer=d(Se);_animationsDisabled=He();_defaultOptions=d(I2,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=kM;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=Xe(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=Xe(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=pn(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=pn(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new E;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=T2}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(De(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new yo(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(De(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let a=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&a._origin instanceof N)return this._overlayRef;this._detach()}let i=this._injector.get(sr).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${AM}`,o=jr(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(De(this._destroyed)).subscribe(a=>{this._updateCurrentPositionClass(a.connectionPair),this._tooltipInstance&&a.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=ki(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(M2)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(De(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(De(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(De(this._destroyed)).subscribe(a=>{a.preventDefault(),a.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(De(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(C(C({},r.main),o.main)),this._addOffset(C(C({},r.fallback),o.fallback))])}_addOffset(e){let i=A2,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:a}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:a}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:a}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:a}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),ft(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,a;if(i==="center"?this._dir&&this._dir.value==="rtl"?a=r==="end"?"left":"right":a=r==="start"?"left":"right":a=i==="bottom"&&o==="top"?"above":"below",a!==this._currentPosition){let s=this._overlayRef;if(s){let l=`${this._cssClassPrefix}-${AM}-`;s.removePanelClass(l+this._currentPosition),s.addPanelClass(l+a)}this._currentPosition=a}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,S2))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||ft({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!Ye(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),kM=(()=>{class t{_changeDetectorRef=d(me);_elementRef=d(N);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=He();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new E;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";constructor(){}show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>k2&&e.width>=O2}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let a=getComputedStyle(i);(a.getPropertyValue("animation-duration")==="0s"||a.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&qe(E2,7),i&2){let o;H(o=U())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&D("mouseleave",function(a){return r._handleMouseLeave(a)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(dt(0,"div",1,0),as("animationend",function(a){return r._handleAnimationEnd(a)}),dt(2,"div",2),p(3),At()()),i&2&&(on(r.tooltipClass),R("mdc-tooltip--multiline",r._isMultiline),h(3),B(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--mat-tooltip-container-color, var(--mat-sys-inverse-surface));
  color: var(--mat-tooltip-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-tooltip-container-shape, var(--mat-sys-corner-extra-small));
  font-family: var(--mat-tooltip-supporting-text-font, var(--mat-sys-body-small-font));
  font-size: var(--mat-tooltip-supporting-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-tooltip-supporting-text-weight, var(--mat-sys-body-small-weight));
  line-height: var(--mat-tooltip-supporting-text-line-height, var(--mat-sys-body-small-line-height));
  letter-spacing: var(--mat-tooltip-supporting-text-tracking, var(--mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2,changeDetection:0})}return t})();var OM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[qc,Yn,Ce,Nn]})}return t})();var Eo=class t{http=d(Je);apiUrl="/api/Albumes";getAlbumes(){return this.http.get(this.apiUrl)}crearAlbum(n){return this.http.post(this.apiUrl,n)}editarAlbum(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}eliminarAlbum(n){return this.http.delete(`${this.apiUrl}/${n}`)}getAlbumConCromos(n){return this.http.get(`${this.apiUrl}/${n}/Cromos`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};var Qs=class t{http=d(Je);apiUrl="/api/Editoriales";getEditoriales(){return this.http.get(this.apiUrl)}crearEditorial(n){return this.http.post(this.apiUrl,n)}editarEditorial(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}eliminarEditorial(n){return this.http.delete(`${this.apiUrl}/${n}`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};var N2=t=>["/album",t],F2=(t,n)=>e=>e.nombre,P2=(t,n)=>e=>e.nombre;function L2(t,n){if(t&1&&(m(0,"mat-option",23),p(1),u()),t&2){let e=n.$implicit;v("value",e.id),h(),Y(" ",e.nombre," ")}}function j2(t,n){t&1&&(m(0,"mat-error"),p(1," La editorial es obligatoria "),u())}function V2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function B2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function H2(t,n){t&1&&(m(0,"mat-error"),p(1," La temporada es obligatoria "),u())}function U2(t,n){t&1&&(m(0,"mat-error"),p(1," La temporada no puede superar los 50 caracteres "),u())}function z2(t,n){if(t&1&&(m(0,"mat-option",23),p(1),u()),t&2){let e=n.$implicit;v("value",e.id),h(),Y(" ",e.nombre," ")}}function $2(t,n){t&1&&(m(0,"mat-error"),p(1," Selecciona un equipo "),u())}function q2(t,n){if(t&1){let e=Oe();m(0,"div",24)(1,"span",25),p(2),u(),m(3,"mat-form-field",26)(4,"mat-select",27),A(5,z2,2,2,"mat-option",7),u(),A(6,$2,2,0,"mat-error",9),u(),m(7,"div",28)(8,"button",29),D("click",function(){let r=se(e).index,o=I();return le(o.moverEquipo(r,-1))}),m(9,"mat-icon"),p(10,"arrow_upward"),u()(),m(11,"button",29),D("click",function(){let r=se(e).index,o=I();return le(o.moverEquipo(r,1))}),m(12,"mat-icon"),p(13,"arrow_downward"),u()()(),m(14,"button",30),D("click",function(){let r=se(e).index,o=I();return le(o.quitarEquipo(r))}),m(15,"mat-icon",31),p(16,"delete"),u()()()}if(t&2){let e=n.index,i=I();h(2),B(e+1),h(2),v("formControl",i.equipoControl(e)),h(),v("ngForOf",i.equipos),h(),v("ngIf",i.equipoControl(e).hasError("required")&&i.equipoControl(e).touched),h(2),v("disabled",e===0),h(3),v("disabled",e===i.equiposForm.length-1)}}function G2(t,n){t&1&&(m(0,"div",32),p(1," A\xFAn no hay equipos a\xF1adidos. "),u())}function W2(t,n){if(t&1&&(m(0,"mat-option",23),p(1),u()),t&2){let e=n.$implicit;v("value",e.id),h(),Y(" ",e.nombre," ")}}function Y2(t,n){t&1&&(m(0,"mat-error"),p(1," Selecciona un tipo de cromo "),u())}function Z2(t,n){if(t&1){let e=Oe();m(0,"div",24)(1,"span",25),p(2),u(),m(3,"mat-form-field",26)(4,"mat-select",27),A(5,W2,2,2,"mat-option",7),u(),A(6,Y2,2,0,"mat-error",9),u(),m(7,"div",28)(8,"button",29),D("click",function(){let r=se(e).index,o=I();return le(o.moverTipoCromo(r,-1))}),m(9,"mat-icon"),p(10,"arrow_upward"),u()(),m(11,"button",29),D("click",function(){let r=se(e).index,o=I();return le(o.moverTipoCromo(r,1))}),m(12,"mat-icon"),p(13,"arrow_downward"),u()()(),m(14,"button",30),D("click",function(){let r=se(e).index,o=I();return le(o.quitarTipoCromo(r))}),m(15,"mat-icon",31),p(16,"delete"),u()()()}if(t&2){let e=n.index,i=I();h(2),B(e+1),h(2),v("formControl",i.tipoCromoControl(e)),h(),v("ngForOf",i.tiposCromo),h(),v("ngIf",i.tipoCromoControl(e).hasError("required")&&i.tipoCromoControl(e).touched),h(2),v("disabled",e===0),h(3),v("disabled",e===i.tiposCromoForm.length-1)}}function Q2(t,n){t&1&&(m(0,"div",32),p(1," A\xFAn no hay tipos de cromo a\xF1adidos. "),u())}function K2(t,n){if(t&1){let e=Oe();m(0,"mat-list-item")(1,"mat-icon",33),p(2,"album"),u(),m(3,"span",34),p(4),u(),m(5,"span",35),p(6),u(),m(7,"span",35),p(8),u(),m(9,"span",35),p(10),u(),m(11,"span",36)(12,"mat-icon",37),p(13,"style"),u(),p(14),u(),m(15,"a",38)(16,"mat-icon"),p(17,"visibility"),u()(),m(18,"button",39),D("click",function(){let r=se(e).$implicit,o=I();return le(o.editar(r))}),m(19,"mat-icon"),p(20,"edit"),u()(),m(21,"button",40),D("click",function(){let r=se(e).$implicit,o=I();return le(o.eliminar(r))}),m(22,"mat-icon",31),p(23,"delete"),u()()()}if(t&2){let e=n.$implicit,i=I();h(4),B(e.nombre),h(2),kt(" ",e.editorial.nombre," \xB7 ",e.temporada," "),h(2),Y(" Equipos: ",e.equipos.map(ss(8,F2,n)).join(", ")," "),h(2),Y(" Cromos especiales: ",e.tiposCromo.map(ss(9,P2,n)).join(", ")," "),h(4),Y(" ",e.contadorCromos," cromos "),h(),v("routerLink",v_(10,N2,e.id)),h(3),v("disabled",i.esModoEdicion)}}function X2(t,n){t&1&&(m(0,"mat-list-item")(1,"span",34),p(2,"No hay \xE1lbumes registrados."),u()())}var ep=class t{constructor(n,e,i,r,o,a){this.fb=n;this.albumService=e;this.editorialService=i;this.equipoService=r;this.tipoCromoService=o;this.cdr=a}fb;albumService;editorialService;equipoService;tipoCromoService;cdr;albumForm;albumes=[];editoriales=[];equipos=[];tiposCromo=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarAlbumes(),this.cargarEditoriales(),this.cargarEquipos(),this.cargarTiposCromo()}get equiposForm(){return this.albumForm.get("equipos")}get tiposCromoForm(){return this.albumForm.get("tiposCromo")}equipoControl(n){return this.equiposForm.at(n)}tipoCromoControl(n){return this.tiposCromoForm.at(n)}crearFormulario(){this.albumForm=this.fb.group({nombre:["",[fe.required,fe.maxLength(100)]],temporada:["",[fe.required,fe.maxLength(50)]],editorialId:[null,[fe.required]],equipos:this.fb.array([],[fe.required]),tiposCromo:this.fb.array([],[fe.required])})}cargarAlbumes(){this.albumService.getAlbumes().subscribe({next:n=>{this.albumes=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los \xE1lbumes.",this.cdr.detectChanges()}})}cargarEditoriales(){this.editorialService.getEditoriales().subscribe({next:n=>{this.editoriales=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar las editoriales.",this.cdr.detectChanges()}})}cargarEquipos(){this.equipoService.getEquipos().subscribe({next:n=>{this.equipos=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los equipos.",this.cdr.detectChanges()}})}cargarTiposCromo(){this.tipoCromoService.getTiposCromo().subscribe({next:n=>{this.tiposCromo=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los tipos de cromo.",this.cdr.detectChanges()}})}agregarEquipo(){this.equiposForm.push(this.fb.control(null,fe.required))}agregarTipoCromo(){this.tiposCromoForm.push(this.fb.control(null,fe.required))}quitarEquipo(n){this.equiposForm.removeAt(n)}quitarTipoCromo(n){this.tiposCromoForm.removeAt(n)}moverEquipo(n,e){let i=n+e;i<0||i>=this.equiposForm.length||this._mover(this.equiposForm,n,i)}moverTipoCromo(n,e){let i=n+e;i<0||i>=this.tiposCromoForm.length||this._mover(this.tiposCromoForm,n,i)}_mover(n,e,i){let r=n.at(e).value;n.removeAt(e),n.insert(i,this.fb.control(r,fe.required))}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.albumForm.patchValue({nombre:n.nombre,temporada:n.temporada,editorialId:n.editorial.id}),this.equiposForm.clear(),n.equipos.slice().sort((e,i)=>e.orden-i.orden).forEach(e=>this.equiposForm.push(this.fb.control(e.id,fe.required))),this.tiposCromoForm.clear(),n.tiposCromo.slice().sort((e,i)=>e.orden-i.orden).forEach(e=>this.tiposCromoForm.push(this.fb.control(e.id,fe.required))),this.mensaje="",this.error="",this.cdr.detectChanges()}guardar(){if(this.albumForm.invalid){this.albumForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n={nombre:this.albumForm.get("nombre")?.value,temporada:this.albumForm.get("temporada")?.value,editorialId:this.albumForm.get("editorialId")?.value,equipos:this.equiposForm.value.map(e=>({equipoId:e})),tiposCromo:this.tiposCromoForm.value.map(e=>({tipoCromoId:e}))};this.esModoEdicion?this.albumService.editarAlbum(this.editandoId,n).subscribe({next:()=>{this.cargando=!1,this.mensaje="\xC1lbum actualizado correctamente.",this.cargarAlbumes(),this.cancelar(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el \xE1lbum.",this.cdr.detectChanges()}}):this.albumService.crearAlbum(n).subscribe({next:()=>{this.cargando=!1,this.mensaje="\xC1lbum a\xF1adido correctamente.",this.cargarAlbumes(),this.cancelar(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el \xE1lbum.",this.cdr.detectChanges()}})}eliminar(n){this.albumService.eliminarAlbum(n.id).subscribe({next:()=>{this.albumes=this.albumes.filter(e=>e.id!==n.id),this.mensaje="\xC1lbum eliminado correctamente.",this.error="",this.cdr.detectChanges()},error:()=>{this.mensaje="",this.error="No se ha podido eliminar el \xE1lbum.",this.cdr.detectChanges()}})}cancelar(){this.editandoId=null,this.albumForm.reset(),this.equiposForm.clear(),this.tiposCromoForm.clear(),this.mensaje="",this.error="",this.cdr.detectChanges()}static \u0275fac=function(e){return new(e||t)(M(kn),M(Eo),M(Qs),M(dr),M(wo),M(me))};static \u0275cmp=T({type:t,selectors:[["app-albumes"]],decls:81,vars:24,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["formControlName","editorialId"],[3,"value",4,"ngFor","ngForOf"],["matSuffix",""],[4,"ngIf"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. La Liga 2025"],["matInput","","type","text","formControlName","temporada","placeholder","Ej. 2024/2025"],[1,"list-block"],[1,"list-block-title"],["mat-icon-button","","type","button",1,"add-item-btn",3,"click"],["class","item-row",4,"ngFor","ngForOf"],["class","empty-note",4,"ngIf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],[3,"value"],[1,"item-row"],[1,"item-index"],["appearance","outline",1,"item-field"],[3,"formControl"],[1,"move-buttons"],["mat-icon-button","","type","button",3,"click","disabled"],["mat-icon-button","","type","button",3,"click"],["color","warn"],[1,"empty-note"],["matListItemIcon",""],["matListItemTitle",""],["matListItemLine","",1,"album-detail"],["matListItemLine","",1,"album-counter"],[1,"counter-icon"],["mat-icon-button","","matListItemMeta","","matTooltip","Ver todo","matTooltipPosition","left",3,"routerLink"],["mat-icon-button","","matListItemMeta","",3,"click","disabled"],["mat-icon-button","","matListItemMeta","",3,"click"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),D("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Editorial"),u(),m(16,"mat-select",6),A(17,L2,2,2,"mat-option",7),u(),m(18,"mat-icon",8),p(19,"business"),u(),A(20,j2,2,0,"mat-error",9),u(),m(21,"mat-form-field",5)(22,"mat-label"),p(23,"Nombre"),u(),te(24,"input",10),m(25,"mat-icon",8),p(26,"badge"),u(),A(27,V2,2,0,"mat-error",9)(28,B2,2,0,"mat-error",9),u(),m(29,"mat-form-field",5)(30,"mat-label"),p(31,"Temporada"),u(),te(32,"input",11),m(33,"mat-icon",8),p(34,"calendar_today"),u(),A(35,H2,2,0,"mat-error",9)(36,U2,2,0,"mat-error",9),u(),m(37,"div",12)(38,"div",13)(39,"mat-icon"),p(40,"groups"),u(),m(41,"span"),p(42,"Equipos"),u(),m(43,"button",14),D("click",function(){return i.agregarEquipo()}),m(44,"mat-icon"),p(45,"add"),u()()(),A(46,q2,17,6,"div",15)(47,G2,2,0,"div",16),u(),m(48,"div",12)(49,"div",13)(50,"mat-icon"),p(51,"category"),u(),m(52,"span"),p(53,"Cromos especiales"),u(),m(54,"button",14),D("click",function(){return i.agregarTipoCromo()}),m(55,"mat-icon"),p(56,"add"),u()()(),A(57,Z2,17,6,"div",15)(58,Q2,2,0,"div",16),u(),m(59,"div",17),p(60),u(),m(61,"mat-card-actions",18)(62,"button",19),D("click",function(){return i.cancelar()}),p(63),u(),m(64,"button",20)(65,"mat-icon"),p(66),u(),p(67),u()()()()(),m(68,"mat-card",21)(69,"mat-card-header")(70,"div",3)(71,"mat-icon"),p(72,"list"),u()(),m(73,"mat-card-title"),p(74,"\xC1lbumes registrados"),u(),m(75,"mat-card-subtitle"),p(76," \xC1lbumes existentes en la base de datos "),u()(),m(77,"mat-card-content")(78,"mat-list"),A(79,K2,24,12,"mat-list-item",22)(80,X2,3,0,"mat-list-item",9),u()()()()()),e&2){let r,o,a,s,l;h(6),B(i.esModoEdicion?"edit":"album"),h(2),B(i.esModoEdicion?"Editar \xE1lbum":"A\xF1adir nuevo \xE1lbum"),h(2),Y(" ",i.esModoEdicion?"Modifica los datos del \xE1lbum":"Completa los datos del \xE1lbum que quieres a\xF1adir"," "),h(2),v("formGroup",i.albumForm),h(5),v("ngForOf",i.editoriales),h(3),v("ngIf",(r=i.albumForm.get("editorialId"))==null?null:r.hasError("required")),h(7),v("ngIf",(o=i.albumForm.get("nombre"))==null?null:o.hasError("required")),h(),v("ngIf",(a=i.albumForm.get("nombre"))==null?null:a.hasError("maxlength")),h(7),v("ngIf",(s=i.albumForm.get("temporada"))==null?null:s.hasError("required")),h(),v("ngIf",(l=i.albumForm.get("temporada"))==null?null:l.hasError("maxlength")),h(10),v("ngForOf",i.equiposForm.controls),h(),v("ngIf",i.equiposForm.length===0),h(10),v("ngForOf",i.tiposCromoForm.controls),h(),v("ngIf",i.tiposCromoForm.length===0),h(),R("error",!!i.error),h(),kt(" ",i.mensaje,"",i.error," "),h(3),Y(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),v("disabled",i.cargando),h(2),B(i.esModoEdicion?"save":"add"),h(),Y(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar \xE1lbum"," "),h(12),v("ngForOf",i.albumes),h(),v("ngIf",i.albumes.length===0)}},dependencies:[Rt,Ot,Ut,mn,OM,mb,fn,Jt,pt,Nt,Xt,jc,zt,wn,bt,vt,Zn,gt,Rn,tt,Lt,gn,Pn,jt,nn,tn,en,nt,Ge,mt,et,Ft,hn,rt,it,_t,Pt,yt,lr,ci,Kn,ld,Qn,Li,Ct,xt,xo,wt,Et],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:#2e7d32;font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}.list-block[_ngcontent-%COMP%]{border:1px solid #e0e0e0;border-radius:8px;padding:12px;display:flex;flex-direction:column;gap:8px;background:#fafafa}.list-block-title[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;font-weight:500;color:#333}.list-block-title[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:#1976d2}.add-item-btn[_ngcontent-%COMP%]{margin-left:auto}.item-row[_ngcontent-%COMP%]{display:flex;align-items:center;gap:4px}.item-index[_ngcontent-%COMP%]{display:inline-flex;align-items:center;justify-content:center;min-width:28px;height:28px;padding:0 8px;border-radius:14px;background:#1976d2;color:#fff;font-size:13px;font-weight:500}.item-field[_ngcontent-%COMP%]{flex:1 1 auto}.move-buttons[_ngcontent-%COMP%]{display:flex;flex-direction:column}.move-buttons[_ngcontent-%COMP%]   mat-icon-button[_ngcontent-%COMP%]{width:32px;height:32px;line-height:32px}.empty-note[_ngcontent-%COMP%]{color:#888;font-size:13px;padding:4px 0}.album-detail[_ngcontent-%COMP%]{font-size:13px;color:#666}.album-counter[_ngcontent-%COMP%]{display:flex;align-items:center;gap:4px;font-size:13px;color:#1976d2;font-weight:500}.counter-icon[_ngcontent-%COMP%]{font-size:16px;width:16px;height:16px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var J2=["determinateSpinner"];function ej(t,n){if(t&1&&(ni(),m(0,"svg",11),te(1,"circle",12),u()),t&2){let e=I();J("viewBox",e._viewBox()),h(),Hn("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),J("r",e._circleRadius())}}var tj=new y("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:NM})}),NM=100,nj=10,Ks=(()=>{class t{_elementRef=d(N);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=d(tj),i=qv(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=NM;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-nj)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&qe(J2,5),i&2){let o;H(o=U())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(J("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),on("mat-"+r.color),Hn("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),R("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",Ei],diameter:[2,"diameter","diameter",Ei],strokeWidth:[2,"strokeWidth","strokeWidth",Ei]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(A(0,ej,2,8,"ng-template",null,0,ls),m(2,"div",2,1),ni(),m(4,"svg",3),te(5,"circle",4),u()(),kl(),m(6,"div",5)(7,"div",6)(8,"div",7),os(9,8),u(),m(10,"div",9),os(11,8),u(),m(12,"div",10),os(13,8),u()()()),i&2){let o=Be(1);h(4),J("viewBox",r._viewBox()),h(),Hn("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),J("r",r._circleRadius()),h(4),v("ngTemplateOutlet",o),h(2),v("ngTemplateOutlet",o),h(2),v("ngTemplateOutlet",o)}},dependencies:[ic],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var Xs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[Ce]})}return t})();var tp=class t{http=d(Je);apiUrl="/api";cargarResumen(){return jo({cromos:this.http.get(`${this.apiUrl}/Cromo`),jugadores:this.http.get(`${this.apiUrl}/Jugadores`),equipos:this.http.get(`${this.apiUrl}/Equipos`),ediciones:this.http.get(`${this.apiUrl}/Ediciones`),editoriales:this.http.get(`${this.apiUrl}/Editoriales`),publicaciones:this.http.get(`${this.apiUrl}/Publicaciones`),tiposCromo:this.http.get(`${this.apiUrl}/TiposCromo`),albumes:this.http.get(`${this.apiUrl}/Albumes`),colecciones:this.http.get(`${this.apiUrl}/Colecciones`),usuarios:this.http.get(`${this.apiUrl}/Usuarios`),usuariosCromo:this.http.get(`${this.apiUrl}/UsuariosCromos`)}).pipe($(({cromos:n,jugadores:e,equipos:i,ediciones:r,editoriales:o,publicaciones:a,tiposCromo:s,albumes:l,colecciones:c,usuarios:f,usuariosCromo:g})=>({cromos:n.length,jugadores:e.length,equipos:i.length,ediciones:r.length,editoriales:o.length,publicaciones:a.length,tiposCromo:s.length,albumes:l.length,colecciones:c.length,usuarios:f.length,cromosPorUsuario:f.length>0?Math.round(g.length/f.length):0})))}buscarCromos(n){return this.http.get(`${this.apiUrl}/Cromo`,{params:n?{q:n}:{}})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};function rj(t,n){if(t&1&&(m(0,"a",19)(1,"mat-card-content")(2,"div",20)(3,"mat-icon"),p(4),u()(),m(5,"div",21)(6,"p",22),p(7),u(),m(8,"p",23),p(9),u(),m(10,"p",24),p(11),u()()()()),t&2){let e=I().$implicit,i=I(2);v("routerLink",e.ruta),h(4),B(e.icono),h(3),B(i.valor(e.clave)),h(2),B(e.titulo),h(2),B(e.descripcion)}}function oj(t,n){if(t&1&&(m(0,"mat-card",25)(1,"mat-card-content")(2,"div",20)(3,"mat-icon"),p(4),u()(),m(5,"div",21)(6,"p",22),p(7),u(),m(8,"p",23),p(9),u(),m(10,"p",24),p(11),u()()()()),t&2){let e=I().$implicit,i=I(2);h(4),B(e.icono),h(3),B(i.valor(e.clave)),h(2),B(e.titulo),h(2),B(e.descripcion)}}function aj(t,n){if(t&1&&(m(0,"div",16),A(1,rj,12,5,"a",17)(2,oj,12,4,"mat-card",18),u()),t&2){let e=n.$implicit;h(),v("ngIf",e.ruta),h(),v("ngIf",!e.ruta)}}function sj(t,n){if(t&1&&(m(0,"div",8)(1,"div",9)(2,"mat-card",10)(3,"div",11)(4,"mat-icon",12),p(5,"style"),u(),m(6,"div")(7,"p",13),p(8),u(),m(9,"p",14),p(10,"Cromos en total"),u()()(),m(11,"mat-card-content")(12,"p")(13,"mat-icon"),p(14,"style"),u(),p(15),u(),m(16,"p")(17,"mat-icon"),p(18,"album"),u(),p(19),u()()()(),A(20,aj,3,2,"div",15),u()),t&2){let e=I();h(8),B(e.resumen.cromos),h(7),Y(" ",e.resumen.cromosPorUsuario," cromos por usuario "),h(4),Y(" ",e.resumen.albumes," \xE1lbumes disponibles "),h(),v("ngForOf",e.tarjetas)}}function lj(t,n){if(t&1){let e=Oe();m(0,"div",36)(1,"button",37),D("click",function(){se(e);let r=I(2);return le(r.limpiar())}),m(2,"mat-icon"),p(3,"close"),u(),p(4," Limpiar "),u()()}}function cj(t,n){t&1&&(m(0,"div",38),te(1,"mat-spinner",39),m(2,"p"),p(3,"Buscando..."),u()())}function dj(t,n){if(t&1&&(m(0,"div",40)(1,"mat-icon"),p(2,"search_off"),u(),m(3,"p"),p(4),u()()),t&2){let e=I(2);h(4),Y("No hay cromos para \xAB",e.buscado,"\xBB.")}}function uj(t,n){if(t&1&&(m(0,"div",38)(1,"p",41),p(2),u()()),t&2){let e=I(2);h(2),kt("",e.resultados.length," cromo(s) para \xAB",e.buscado,"\xBB")}}function mj(t,n){if(t&1&&(m(0,"mat-card",42)(1,"mat-card-content")(2,"div",43)(3,"mat-icon"),p(4,"style"),u()(),m(5,"div",44)(6,"p",45),p(7),u(),m(8,"p",46),p(9),u(),m(10,"p",47)(11,"mat-icon"),p(12,"groups"),u(),p(13),m(14,"mat-icon",48),p(15,"album"),u(),p(16),u()()()()),t&2){let e=n.$implicit;h(7),Y("N.\xBA ",e.numero),h(2),B((e.jugador==null?null:e.jugador.nombreCompleto)||(e.jugador==null?null:e.jugador.nombre)||"\u2014"),h(4),Y(" ",(e.equipo==null?null:e.equipo.nombre)||"\u2014"," "),h(3),Y(" ",(e.album==null?null:e.album.nombre)||"\u2014"," ")}}function fj(t,n){if(t&1&&(m(0,"section",26)(1,"mat-card",27)(2,"mat-card-header")(3,"div",28)(4,"mat-icon"),p(5,"search"),u()(),m(6,"mat-card-title"),p(7,"Buscar cromos"),u(),m(8,"mat-card-subtitle"),p(9,"Busca por nombre de jugador o de equipo"),u()(),m(10,"mat-card-content")(11,"mat-form-field",29)(12,"mat-label"),p(13,"Jugador o equipo"),u(),te(14,"input",30),m(15,"mat-icon",31),p(16,"search"),u()(),A(17,lj,5,0,"div",32),u()(),A(18,cj,4,0,"div",33)(19,dj,5,1,"div",34)(20,uj,3,2,"div",33)(21,mj,17,4,"mat-card",35),u()),t&2){let e=I();h(14),v("formControl",e.busqueda),h(3),v("ngIf",e.busqueda.value),h(),v("ngIf",e.buscando),h(),v("ngIf",!e.buscando&&e.sinResultados),h(),v("ngIf",!e.buscando&&e.buscado&&!e.sinResultados),h(),v("ngForOf",e.resultados)}}function pj(t,n){t&1&&(m(0,"div",49),te(1,"mat-spinner",50),m(2,"p"),p(3,"Cargando datos del panel..."),u()())}function hj(t,n){if(t&1&&(m(0,"div",51)(1,"mat-icon"),p(2,"error_outline"),u(),m(3,"p"),p(4),u()()),t&2){let e=I();h(4),B(e.error)}}var np=class t{constructor(n,e){this.adminService=n;this.cdr=e}adminService;cdr;cargando=!0;error="";resumen=null;busqueda=new pa("");buscando=!1;resultados=[];buscado="";sinResultados=!1;subBusqueda;tarjetas=[{clave:"cromos",titulo:"Cromos",descripcion:"Personajes y tarjetas",icono:"style",ruta:"/cromos"},{clave:"jugadores",titulo:"Jugadores",descripcion:"Deportistas registrados",icono:"person",ruta:"/jugadores"},{clave:"equipos",titulo:"Equipos",descripcion:"Clubes y selecciones",icono:"groups",ruta:"/equipos"},{clave:"ediciones",titulo:"Ediciones",descripcion:"Lanzamientos y series",icono:"auto_awesome",ruta:"/ediciones"},{clave:"editoriales",titulo:"Editoriales",descripcion:"Empresas editoras",icono:"storefront",ruta:"/editoriales"},{clave:"publicaciones",titulo:"Publicaciones",descripcion:"Entregas publicadas",icono:"newspaper"},{clave:"tiposCromo",titulo:"Tipos de cromo",descripcion:"Categor\xEDas especiales",icono:"category",ruta:"/tipos-cromo"},{clave:"albumes",titulo:"\xC1lbumes",descripcion:"Colecciones armadas",icono:"album",ruta:"/albumes"},{clave:"colecciones",titulo:"Colecciones",descripcion:"Conjuntos definidos",icono:"collections_bookmark",ruta:"/albumes"},{clave:"usuarios",titulo:"Usuarios",descripcion:"Cuentas activas",icono:"group"}];ngOnInit(){this.cargarResumen(),this.subBusqueda=this.busqueda.valueChanges.pipe(ui(350),Pa(),We(n=>{let e=(n??"").trim();return e?(this.buscando=!0,this.sinResultados=!1,this.adminService.buscarCromos(e)):(this.resultados=[],this.buscado="",this.sinResultados=!1,this.buscando=!1,this.cdr.detectChanges(),Q([]))})).subscribe({next:n=>{this.buscando=!1,this.resultados=n,this.buscado=(this.busqueda.value??"").trim(),this.sinResultados=this.buscado!==""&&n.length===0,this.cdr.detectChanges()},error:()=>{this.buscando=!1,this.error="No se ha podido realizar la b\xFAsqueda.",this.cdr.detectChanges()}})}ngOnDestroy(){this.subBusqueda?.unsubscribe()}limpiar(){this.busqueda.setValue(""),this.resultados=[],this.buscado="",this.sinResultados=!1}cargarResumen(){this.cargando=!0,this.error="",this.adminService.cargarResumen().subscribe({next:n=>{this.resumen=n,this.cargando=!1,this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido cargar el panel de administraci\xF3n.",this.cdr.detectChanges()}})}valor(n){return this.resumen?this.resumen[n]??0:0}static \u0275fac=function(e){return new(e||t)(M(tp),M(me))};static \u0275cmp=T({type:t,selectors:[["app-admin-panel"]],decls:15,vars:5,consts:[[1,"panel"],[1,"panel-header"],[1,"subtitle"],["mat-stroked-button","","color","primary",3,"click","disabled"],["class","card-grid",4,"ngIf"],["class","busqueda-section",4,"ngIf"],["class","cargando",4,"ngIf"],["class","error",4,"ngIf"],[1,"card-grid"],[1,"highlight"],[1,"hero"],[1,"hero-body"],[1,"hero-icon"],[1,"hero-title"],[1,"hero-label"],["class","grid-item",4,"ngFor","ngForOf"],[1,"grid-item"],["mat-card","","class","stat-card",3,"routerLink",4,"ngIf"],["class","stat-card",4,"ngIf"],["mat-card","",1,"stat-card",3,"routerLink"],[1,"stat-icon"],[1,"stat-body"],[1,"stat-value"],[1,"stat-title"],[1,"stat-desc"],[1,"stat-card"],[1,"busqueda-section"],[1,"busqueda-card"],["mat-card-avatar","",1,"busqueda-icon"],["appearance","outline",1,"full-width"],["matInput","","type","text","placeholder","Ej. Lamine Yamal o FC Barcelona",3,"formControl"],["matSuffix",""],["class","busqueda-actions",4,"ngIf"],["class","resultados",4,"ngIf"],["class","resultados vacio",4,"ngIf"],["class","result-card",4,"ngFor","ngForOf"],[1,"busqueda-actions"],["mat-button","","type","button",3,"click"],[1,"resultados"],["diameter","32"],[1,"resultados","vacio"],[1,"resultado-titulo"],[1,"result-card"],[1,"result-icon"],[1,"result-body"],[1,"result-numero"],[1,"result-jugador"],[1,"result-meta"],[1,"sep"],[1,"cargando"],["diameter","40"],[1,"error"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"header",1)(2,"div")(3,"h1"),p(4,"Panel de administraci\xF3n"),u(),m(5,"p",2),p(6,"Resumen general de la colecci\xF3n CromosList"),u()(),m(7,"button",3),D("click",function(){return i.cargarResumen()}),m(8,"mat-icon"),p(9,"refresh"),u(),p(10," Actualizar "),u()(),A(11,sj,21,4,"div",4)(12,fj,22,6,"section",5)(13,pj,4,0,"div",6)(14,hj,5,1,"div",7),u()),e&2&&(h(7),v("disabled",i.cargando),h(4),v("ngIf",i.resumen),h(),v("ngIf",i.resumen),h(),v("ngIf",i.cargando&&!i.resumen),h(),v("ngIf",i.error&&!i.cargando))},dependencies:[Rt,Ot,Ut,mn,fn,pt,Nt,jc,Xs,Ks,bt,vt,gt,tt,Lt,Pn,jt,nn,tn,en,nt,Ge,mt,et,hn,rt,it,_t,Pt,yt,Ct,xt,wt,Et],styles:['@charset "UTF-8";.panel[_ngcontent-%COMP%]{max-width:1200px;margin:0 auto;display:flex;flex-direction:column;gap:24px}.panel-header[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}.panel-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0;font-size:26px;font-weight:700;color:var(--app-primary-dark)}.panel-header[_ngcontent-%COMP%]   .subtitle[_ngcontent-%COMP%]{margin:6px 0 0;color:var(--app-text-muted);font-size:14px}.card-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(2,1fr);gap:20px}@media(min-width:768px){.card-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(4,1fr)}}.highlight[_ngcontent-%COMP%]{grid-column:1/-1}.hero[_ngcontent-%COMP%]{background:linear-gradient(135deg,var(--app-primary) 0%,var(--app-green) 100%);border-radius:20px;color:#fff}.hero-body[_ngcontent-%COMP%]{display:flex;align-items:center;gap:20px;padding:28px 24px 8px}.hero-icon[_ngcontent-%COMP%]{font-size:48px;width:48px;height:48px;opacity:.9}.hero-title[_ngcontent-%COMP%]{margin:0;font-size:44px;font-weight:700;line-height:1}.hero-label[_ngcontent-%COMP%]{margin:4px 0 0;font-size:15px;opacity:.9}.hero[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{display:flex;gap:32px;flex-wrap:wrap;padding:8px 24px 24px 92px!important}.hero[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;margin:0;font-size:14px;opacity:.95}.stat-card[_ngcontent-%COMP%]{border-radius:18px;background:var(--app-surface);border:1px solid var(--app-border);box-shadow:0 6px 18px #1f6f8b0f;transition:transform .18s ease,box-shadow .18s ease;height:100%}a.stat-card[_ngcontent-%COMP%]{text-decoration:none;color:inherit;cursor:pointer;display:block}.stat-card[_ngcontent-%COMP%]:hover{transform:translateY(-4px);box-shadow:0 12px 28px #1f6f8b1f}.stat-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{display:flex;align-items:center;gap:16px;padding:20px!important}.stat-icon[_ngcontent-%COMP%]{width:52px;height:52px;flex-shrink:0;border-radius:14px;display:flex;align-items:center;justify-content:center;background:var(--app-primary-soft);color:var(--app-primary)}.grid-item[_ngcontent-%COMP%]:nth-child(3n)   .stat-icon[_ngcontent-%COMP%], .grid-item[_ngcontent-%COMP%]:nth-child(4n)   .stat-icon[_ngcontent-%COMP%]{background:var(--app-green-soft);color:var(--app-green)}.stat-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:28px;width:28px;height:28px}.stat-body[_ngcontent-%COMP%]{min-width:0}.stat-value[_ngcontent-%COMP%]{margin:0;font-size:28px;font-weight:700;color:var(--app-text);line-height:1.1}.stat-title[_ngcontent-%COMP%]{margin:2px 0 0;font-size:14px;font-weight:600;color:var(--app-primary-dark)}.stat-desc[_ngcontent-%COMP%]{margin:2px 0 0;font-size:12px;color:var(--app-text-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.cargando[_ngcontent-%COMP%], .error[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:center;gap:12px;padding:48px 0}.cargando[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{color:var(--app-text-muted);font-size:14px}.error[_ngcontent-%COMP%]{color:#c62828}.error[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;font-size:14px}@media(max-width:560px){.hero[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{padding-left:24px!important}}.busqueda-section[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.busqueda-card[_ngcontent-%COMP%], .result-card[_ngcontent-%COMP%]{border-radius:18px;background:var(--app-surface);border:1px solid var(--app-border);box-shadow:0 6px 18px #1f6f8b0f}.busqueda-card[_ngcontent-%COMP%]   mat-card-header[_ngcontent-%COMP%]{padding:20px 20px 0}.busqueda-card[_ngcontent-%COMP%]   mat-card-title[_ngcontent-%COMP%]{font-weight:600}.busqueda-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{padding:16px 20px 20px!important}.busqueda-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px;background:var(--app-primary-soft);color:var(--app-primary)}.busqueda-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}.full-width[_ngcontent-%COMP%]{width:100%}.busqueda-actions[_ngcontent-%COMP%]{display:flex;justify-content:flex-end;margin-top:8px}.resultados[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:center;gap:12px;color:var(--app-text-muted);font-size:14px;padding:8px 0}.resultados.vacio[_ngcontent-%COMP%]{flex-direction:column;padding:24px 0}.result-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{display:flex;align-items:center;gap:16px;padding:16px 20px!important}.result-icon[_ngcontent-%COMP%]{width:44px;height:44px;flex-shrink:0;border-radius:12px;display:flex;align-items:center;justify-content:center;background:var(--app-primary-soft);color:var(--app-primary)}.result-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}.result-body[_ngcontent-%COMP%]{min-width:0}.result-numero[_ngcontent-%COMP%]{margin:0;font-size:12px;font-weight:600;color:var(--app-primary);text-transform:uppercase;letter-spacing:.4px}.result-jugador[_ngcontent-%COMP%]{margin:2px 0 0;font-size:16px;font-weight:600;color:var(--app-text)}.result-meta[_ngcontent-%COMP%]{margin:4px 0 0;display:flex;align-items:center;gap:4px;font-size:13px;color:var(--app-text-muted)}.result-meta[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:16px;width:16px;height:16px}.result-meta[_ngcontent-%COMP%]   .sep[_ngcontent-%COMP%]{margin-left:8px}']})};function gj(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function _j(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function vj(t,n){if(t&1){let e=Oe();m(0,"mat-list-item")(1,"mat-icon",15),p(2,"storefront"),u(),m(3,"span",16),p(4),u(),m(5,"button",17),D("click",function(){let r=se(e).$implicit,o=I();return le(o.editar(r))}),m(6,"mat-icon"),p(7,"edit"),u()(),m(8,"button",18),D("click",function(){let r=se(e).$implicit,o=I();return le(o.eliminar(r))}),m(9,"mat-icon",19),p(10,"delete"),u()()()}if(t&2){let e=n.$implicit,i=I();h(4),B(e.nombre),h(),v("disabled",i.esModoEdicion)}}function bj(t,n){t&1&&(m(0,"mat-list-item")(1,"span",16),p(2,"No hay editoriales registradas."),u()())}var ip=class t{constructor(n,e){this.fb=n;this.editorialService=e}fb;editorialService;editorialForm;editoriales=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarEditoriales()}crearFormulario(){this.editorialForm=this.fb.group({nombre:["",[fe.required,fe.maxLength(100)]]})}cargarEditoriales(){this.editorialService.getEditoriales().subscribe({next:n=>{this.editoriales=n},error:()=>{this.error="No se han podido cargar las editoriales."}})}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.editorialForm.patchValue({nombre:n.nombre}),this.mensaje="",this.error=""}guardar(){if(this.editorialForm.invalid){this.editorialForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n=this.editorialForm.get("nombre")?.value;this.esModoEdicion?this.editorialService.editarEditorial(this.editandoId,{nombre:n}).subscribe({next:e=>{this.cargando=!1;let i=this.editoriales.findIndex(r=>r.id===this.editandoId);i!==-1&&(this.editoriales[i]=e),this.mensaje="Editorial actualizada correctamente.",this.cancelar()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar la editorial."}}):this.editorialService.crearEditorial({nombre:n}).subscribe({next:e=>{this.cargando=!1,this.editoriales.push(e),this.mensaje="Editorial a\xF1adida correctamente.",this.editorialForm.reset()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir la editorial."}})}eliminar(n){this.editorialService.eliminarEditorial(n.id).subscribe({next:()=>{this.editoriales=this.editoriales.filter(e=>e.id!==n.id),this.mensaje="Editorial eliminada correctamente.",this.error=""},error:()=>{this.mensaje="",this.error="No se ha podido eliminar la editorial."}})}cancelar(){this.editandoId=null,this.editorialForm.reset(),this.mensaje="",this.error=""}static \u0275fac=function(e){return new(e||t)(M(kn),M(Qs))};static \u0275cmp=T({type:t,selectors:[["app-editoriales"]],decls:43,vars:16,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. Panini, Topps..."],["matSuffix",""],[4,"ngIf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],["matListItemIcon",""],["matListItemTitle",""],["mat-icon-button","","matListItemMeta","",3,"click","disabled"],["mat-icon-button","","matListItemMeta","",3,"click"],["color","warn"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),D("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Nombre"),u(),te(16,"input",6),m(17,"mat-icon",7),p(18,"storefront"),u(),A(19,gj,2,0,"mat-error",8)(20,_j,2,0,"mat-error",8),u(),m(21,"div",9),p(22),u(),m(23,"mat-card-actions",10)(24,"button",11),D("click",function(){return i.cancelar()}),p(25),u(),m(26,"button",12)(27,"mat-icon"),p(28),u(),p(29),u()()()()(),m(30,"mat-card",13)(31,"mat-card-header")(32,"div",3)(33,"mat-icon"),p(34,"list"),u()(),m(35,"mat-card-title"),p(36,"Editoriales registradas"),u(),m(37,"mat-card-subtitle"),p(38," Editoriales existentes en la base de datos "),u()(),m(39,"mat-card-content")(40,"mat-list"),A(41,vj,11,2,"mat-list-item",14)(42,bj,3,0,"mat-list-item",8),u()()()()()),e&2){let r,o;h(6),B(i.esModoEdicion?"edit":"storefront"),h(2),B(i.esModoEdicion?"Editar editorial":"A\xF1adir nueva editorial"),h(2),Y(" ",i.esModoEdicion?"Modifica el nombre de la editorial":"Completa los datos de la editorial que quieres a\xF1adir"," "),h(2),v("formGroup",i.editorialForm),h(7),v("ngIf",(r=i.editorialForm.get("nombre"))==null?null:r.hasError("required")),h(),v("ngIf",(o=i.editorialForm.get("nombre"))==null?null:o.hasError("maxlength")),h(),R("error",!!i.error),h(),kt(" ",i.mensaje,"",i.error," "),h(3),Y(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),v("disabled",i.cargando),h(2),B(i.esModoEdicion?"save":"add"),h(),Y(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar editorial"," "),h(12),v("ngForOf",i.editoriales),h(),v("ngIf",i.editoriales.length===0)}},dependencies:[Rt,Ot,Ut,fn,Jt,pt,Nt,Xt,zt,wn,bt,vt,Zn,gt,tt,Lt,gn,Pn,jt,nn,tn,en,nt,Ge,mt,et,Ft,hn,rt,it,_t,Pt,yt,lr,ci,Kn,Qn,Li,Ct,xt,wt,Et],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:var(--app-bg)}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden;border:1px solid var(--app-border)}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px;background:var(--app-primary-soft);color:var(--app-primary)}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:var(--app-green);font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var hb=new y("CdkAccordion"),FM=(()=>{class t{_stateChanges=new E;_openCloseAllActions=new E;id=d(ze).getId("cdk-accordion-");multi=!1;openAll(){this.multi&&this._openCloseAllActions.next(!0)}closeAll(){this._openCloseAllActions.next(!1)}ngOnChanges(e){this._stateChanges.next(e)}ngOnDestroy(){this._stateChanges.complete(),this._openCloseAllActions.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["cdk-accordion"],["","cdkAccordion",""]],inputs:{multi:[2,"multi","multi",Z]},exportAs:["cdkAccordion"],features:[we([{provide:hb,useExisting:t}]),je]})}return t})(),PM=(()=>{class t{accordion=d(hb,{optional:!0,skipSelf:!0});_changeDetectorRef=d(me);_expansionDispatcher=d(ad);_openCloseAllSubscription=de.EMPTY;closed=new F;opened=new F;destroyed=new F;expandedChange=new F;id=d(ze).getId("cdk-accordion-child-");get expanded(){return this._expanded}set expanded(e){if(this._expanded!==e){if(this._expanded=e,this.expandedChange.emit(e),e){this.opened.emit();let i=this.accordion?this.accordion.id:this.id;this._expansionDispatcher.notify(this.id,i)}else this.closed.emit();this._changeDetectorRef.markForCheck()}}_expanded=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=ee(!1);_removeUniqueSelectionListener=()=>{};constructor(){}ngOnInit(){this._removeUniqueSelectionListener=this._expansionDispatcher.listen((e,i)=>{this.accordion&&!this.accordion.multi&&this.accordion.id===i&&this.id!==e&&(this.expanded=!1)}),this.accordion&&(this._openCloseAllSubscription=this._subscribeToOpenCloseAllActions())}ngOnDestroy(){this.opened.complete(),this.closed.complete(),this.destroyed.emit(),this.destroyed.complete(),this._removeUniqueSelectionListener(),this._openCloseAllSubscription.unsubscribe()}toggle(){this.disabled||(this.expanded=!this.expanded)}close(){this.disabled||(this.expanded=!1)}open(){this.disabled||(this.expanded=!0)}_subscribeToOpenCloseAllActions(){return this.accordion._openCloseAllActions.subscribe(e=>{this.disabled||(this.expanded=e)})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["cdk-accordion-item"],["","cdkAccordionItem",""]],inputs:{expanded:[2,"expanded","expanded",Z],disabled:[2,"disabled","disabled",Z]},outputs:{closed:"closed",opened:"opened",destroyed:"destroyed",expandedChange:"expandedChange"},exportAs:["cdkAccordionItem"],features:[we([{provide:hb,useValue:void 0}])]})}return t})(),LM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({})}return t})();var yj=["body"],Cj=["bodyWrapper"],xj=[[["mat-expansion-panel-header"]],"*",[["mat-action-row"]]],wj=["mat-expansion-panel-header","*","mat-action-row"];function Ej(t,n){}var Dj=[[["mat-panel-title"]],[["mat-panel-description"]],"*"],Mj=["mat-panel-title","mat-panel-description","*"];function Ij(t,n){t&1&&(dt(0,"span",1),ni(),dt(1,"svg",2),An(2,"path",3),At()())}var gb=new y("MAT_ACCORDION"),jM=new y("MAT_EXPANSION_PANEL"),Sj=(()=>{class t{_template=d(St);_expansionPanel=d(jM,{optional:!0});constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["ng-template","matExpansionPanelContent",""]]})}return t})(),VM=new y("MAT_EXPANSION_PANEL_DEFAULT_OPTIONS"),_b=(()=>{class t extends PM{_viewContainerRef=d(ct);_animationsDisabled=He();_document=d(W);_ngZone=d(P);_elementRef=d(N);_renderer=d(Se);_cleanupTransitionEnd;get hideToggle(){return this._hideToggle||this.accordion&&this.accordion.hideToggle}set hideToggle(e){this._hideToggle=e}_hideToggle=!1;get togglePosition(){return this._togglePosition||this.accordion&&this.accordion.togglePosition}set togglePosition(e){this._togglePosition=e}_togglePosition;afterExpand=new F;afterCollapse=new F;_inputChanges=new E;accordion=d(gb,{optional:!0,skipSelf:!0});_lazyContent;_body;_bodyWrapper;_portal;_headerId=d(ze).getId("mat-expansion-panel-header-");constructor(){super();let e=d(VM,{optional:!0});this._expansionDispatcher=d(ad),e&&(this.hideToggle=e.hideToggle)}_hasSpacing(){return this.accordion?this.expanded&&this.accordion.displayMode==="default":!1}_getExpandedState(){return this.expanded?"expanded":"collapsed"}toggle(){this.expanded=!this.expanded}close(){this.expanded=!1}open(){this.expanded=!0}ngAfterContentInit(){this._lazyContent&&this._lazyContent._expansionPanel===this&&this.opened.pipe(Ie(null),oe(()=>this.expanded&&!this._portal),ke(1)).subscribe(()=>{this._portal=new Wn(this._lazyContent._template,this._viewContainerRef)}),this._setupAnimationEvents()}ngOnChanges(e){this._inputChanges.next(e)}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransitionEnd?.(),this._inputChanges.complete()}_containsFocus(){if(this._body){let e=this._document.activeElement,i=this._body.nativeElement;return e===i||i.contains(e)}return!1}_transitionEndListener=({target:e,propertyName:i})=>{e===this._bodyWrapper?.nativeElement&&i==="grid-template-rows"&&this._ngZone.run(()=>{this.expanded?this.afterExpand.emit():this.afterCollapse.emit()})};_setupAnimationEvents(){this._ngZone.runOutsideAngular(()=>{this._animationsDisabled?(this.opened.subscribe(()=>this._ngZone.run(()=>this.afterExpand.emit())),this.closed.subscribe(()=>this._ngZone.run(()=>this.afterCollapse.emit()))):setTimeout(()=>{let e=this._elementRef.nativeElement;this._cleanupTransitionEnd=this._renderer.listen(e,"transitionend",this._transitionEndListener),e.classList.add("mat-expansion-panel-animations-enabled")},200)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-expansion-panel"]],contentQueries:function(i,r,o){if(i&1&&ut(o,Sj,5),i&2){let a;H(a=U())&&(r._lazyContent=a.first)}},viewQuery:function(i,r){if(i&1&&qe(yj,5)(Cj,5),i&2){let o;H(o=U())&&(r._body=o.first),H(o=U())&&(r._bodyWrapper=o.first)}},hostAttrs:[1,"mat-expansion-panel"],hostVars:4,hostBindings:function(i,r){i&2&&R("mat-expanded",r.expanded)("mat-expansion-panel-spacing",r._hasSpacing())},inputs:{hideToggle:[2,"hideToggle","hideToggle",Z],togglePosition:"togglePosition"},outputs:{afterExpand:"afterExpand",afterCollapse:"afterCollapse"},exportAs:["matExpansionPanel"],features:[we([{provide:gb,useValue:void 0},{provide:jM,useExisting:t}]),pe,je],ngContentSelectors:wj,decls:9,vars:4,consts:[["bodyWrapper",""],["body",""],[1,"mat-expansion-panel-content-wrapper"],["role","region",1,"mat-expansion-panel-content",3,"id"],[1,"mat-expansion-panel-body"],[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(Me(xj),V(0),m(1,"div",2,0)(3,"div",3,1)(5,"div",4),V(6,1),A(7,Ej,0,0,"ng-template",5),u(),V(8,2),u()()),i&2&&(h(),J("inert",r.expanded?null:""),h(2),v("id",r.id),J("aria-labelledby",r._headerId),h(4),v("cdkPortalOutlet",r._portal))},dependencies:[Ea],styles:[`.mat-expansion-panel {
  box-sizing: content-box;
  display: block;
  margin: 0;
  overflow: hidden;
}
.mat-expansion-panel.mat-expansion-panel-animations-enabled {
  transition: margin 225ms cubic-bezier(0.4, 0, 0.2, 1), box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel {
  position: relative;
  background: var(--mat-expansion-container-background-color, var(--mat-sys-surface));
  color: var(--mat-expansion-container-text-color, var(--mat-sys-on-surface));
  border-radius: var(--mat-expansion-container-shape, 12px);
}
.mat-expansion-panel:not([class*=mat-elevation-z]) {
  box-shadow: var(--mat-expansion-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}
.mat-accordion .mat-expansion-panel:not(.mat-expanded), .mat-accordion .mat-expansion-panel:not(.mat-expansion-panel-spacing) {
  border-radius: 0;
}
.mat-accordion .mat-expansion-panel:first-of-type {
  border-top-right-radius: var(--mat-expansion-container-shape, 12px);
  border-top-left-radius: var(--mat-expansion-container-shape, 12px);
}
.mat-accordion .mat-expansion-panel:last-of-type {
  border-bottom-right-radius: var(--mat-expansion-container-shape, 12px);
  border-bottom-left-radius: var(--mat-expansion-container-shape, 12px);
}
@media (forced-colors: active) {
  .mat-expansion-panel {
    outline: solid 1px;
  }
}

.mat-expansion-panel-content-wrapper {
  display: grid;
  grid-template-rows: 0fr;
  grid-template-columns: 100%;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-content-wrapper {
  transition: grid-template-rows 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
  grid-template-rows: 1fr;
}
@supports not (grid-template-rows: 0fr) {
  .mat-expansion-panel-content-wrapper {
    height: 0;
  }
  .mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
    height: auto;
  }
}
@media print {
  .mat-expansion-panel-content-wrapper {
    height: 0;
  }
  .mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
    height: auto;
  }
}

.mat-expansion-panel-content {
  display: flex;
  flex-direction: column;
  overflow: visible;
  min-height: 0;
  visibility: hidden;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-content {
  transition: visibility 190ms linear;
}
.mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper > .mat-expansion-panel-content {
  visibility: visible;
}
.mat-expansion-panel-content {
  font-family: var(--mat-expansion-container-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-expansion-container-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-expansion-container-text-weight, var(--mat-sys-body-large-weight));
  line-height: var(--mat-expansion-container-text-line-height, var(--mat-sys-body-large-line-height));
  letter-spacing: var(--mat-expansion-container-text-tracking, var(--mat-sys-body-large-tracking));
}

.mat-expansion-panel-body {
  padding: 0 24px 16px;
}

.mat-expansion-panel-spacing {
  margin: 16px 0;
}
.mat-accordion > .mat-expansion-panel-spacing:first-child, .mat-accordion > *:first-child:not(.mat-expansion-panel) .mat-expansion-panel-spacing {
  margin-top: 0;
}
.mat-accordion > .mat-expansion-panel-spacing:last-child, .mat-accordion > *:last-child:not(.mat-expansion-panel) .mat-expansion-panel-spacing {
  margin-bottom: 0;
}

.mat-action-row {
  border-top-style: solid;
  border-top-width: 1px;
  display: flex;
  flex-direction: row;
  justify-content: flex-end;
  padding: 16px 8px 16px 24px;
  border-top-color: var(--mat-expansion-actions-divider-color, var(--mat-sys-outline));
}
.mat-action-row .mat-button-base,
.mat-action-row .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-action-row .mat-button-base,
[dir=rtl] .mat-action-row .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}
`],encapsulation:2,changeDetection:0})}return t})();var vb=(()=>{class t{panel=d(_b,{host:!0});_element=d(N);_focusMonitor=d(On);_changeDetectorRef=d(me);_parentChangeSubscription=de.EMPTY;constructor(){d(ht).load(or);let e=this.panel,i=d(VM,{optional:!0}),r=d(new tr("tabindex"),{optional:!0}),o=e.accordion?e.accordion._stateChanges.pipe(oe(a=>!!(a.hideToggle||a.togglePosition))):ot;this.tabIndex=parseInt(r||"")||0,this._parentChangeSubscription=at(e.opened,e.closed,o,e._inputChanges.pipe(oe(a=>!!(a.hideToggle||a.disabled||a.togglePosition)))).subscribe(()=>this._changeDetectorRef.markForCheck()),e.closed.pipe(oe(()=>e._containsFocus())).subscribe(()=>this._focusMonitor.focusVia(this._element,"program")),i&&(this.expandedHeight=i.expandedHeight,this.collapsedHeight=i.collapsedHeight)}expandedHeight;collapsedHeight;tabIndex=0;get disabled(){return this.panel.disabled}_toggle(){this.disabled||this.panel.toggle()}_isExpanded(){return this.panel.expanded}_getExpandedState(){return this.panel._getExpandedState()}_getPanelId(){return this.panel.id}_getTogglePosition(){return this.panel.togglePosition}_showToggle(){return!this.panel.hideToggle&&!this.panel.disabled}_getHeaderHeight(){let e=this._isExpanded();return e&&this.expandedHeight?this.expandedHeight:!e&&this.collapsedHeight?this.collapsedHeight:null}_keydown(e){switch(e.keyCode){case 32:case 13:Ye(e)||(e.preventDefault(),this._toggle());break;default:this.panel.accordion&&this.panel.accordion._handleHeaderKeydown(e);return}}focus(e,i){e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}ngAfterViewInit(){this._focusMonitor.monitor(this._element).subscribe(e=>{e&&this.panel.accordion&&this.panel.accordion._handleHeaderFocus(this)})}ngOnDestroy(){this._parentChangeSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._element)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-expansion-panel-header"]],hostAttrs:["role","button",1,"mat-expansion-panel-header","mat-focus-indicator"],hostVars:13,hostBindings:function(i,r){i&1&&D("click",function(){return r._toggle()})("keydown",function(a){return r._keydown(a)}),i&2&&(J("id",r.panel._headerId)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r._getPanelId())("aria-expanded",r._isExpanded())("aria-disabled",r.panel.disabled),Hn("height",r._getHeaderHeight()),R("mat-expanded",r._isExpanded())("mat-expansion-toggle-indicator-after",r._getTogglePosition()==="after")("mat-expansion-toggle-indicator-before",r._getTogglePosition()==="before"))},inputs:{expandedHeight:"expandedHeight",collapsedHeight:"collapsedHeight",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Ei(e)]},ngContentSelectors:Mj,decls:5,vars:3,consts:[[1,"mat-content"],[1,"mat-expansion-indicator"],["xmlns","http://www.w3.org/2000/svg","viewBox","0 -960 960 960","aria-hidden","true","focusable","false"],["d","M480-345 240-585l56-56 184 184 184-184 56 56-240 240Z"]],template:function(i,r){i&1&&(Me(Dj),dt(0,"span",0),V(1),V(2,1),V(3,2),At(),ge(4,Ij,3,0,"span",1)),i&2&&(R("mat-content-hide-toggle",!r._showToggle()),h(4),_e(r._showToggle()?4:-1))},styles:[`.mat-expansion-panel-header {
  display: flex;
  flex-direction: row;
  align-items: center;
  padding: 0 24px;
  border-radius: inherit;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-header {
  transition: height 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel-header::before {
  border-radius: inherit;
}
.mat-expansion-panel-header {
  height: var(--mat-expansion-header-collapsed-state-height, 48px);
  font-family: var(--mat-expansion-header-text-font, var(--mat-sys-title-medium-font));
  font-size: var(--mat-expansion-header-text-size, var(--mat-sys-title-medium-size));
  font-weight: var(--mat-expansion-header-text-weight, var(--mat-sys-title-medium-weight));
  line-height: var(--mat-expansion-header-text-line-height, var(--mat-sys-title-medium-line-height));
  letter-spacing: var(--mat-expansion-header-text-tracking, var(--mat-sys-title-medium-tracking));
}
.mat-expansion-panel-header.mat-expanded {
  height: var(--mat-expansion-header-expanded-state-height, 64px);
}
.mat-expansion-panel-header[aria-disabled=true] {
  color: var(--mat-expansion-header-disabled-state-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-expansion-panel-header:not([aria-disabled=true]) {
  cursor: pointer;
}
.mat-expansion-panel:not(.mat-expanded) .mat-expansion-panel-header:not([aria-disabled=true]):hover {
  background: var(--mat-expansion-header-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
@media (hover: none) {
  .mat-expansion-panel:not(.mat-expanded) .mat-expansion-panel-header:not([aria-disabled=true]):hover {
    background: var(--mat-expansion-container-background-color, var(--mat-sys-surface));
  }
}
.mat-expansion-panel .mat-expansion-panel-header:not([aria-disabled=true]).cdk-keyboard-focused, .mat-expansion-panel .mat-expansion-panel-header:not([aria-disabled=true]).cdk-program-focused {
  background: var(--mat-expansion-header-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
.mat-expansion-panel-header._mat-animation-noopable {
  transition: none;
}
.mat-expansion-panel-header:focus, .mat-expansion-panel-header:hover {
  outline: none;
}
.mat-expansion-panel-header.mat-expanded:focus, .mat-expansion-panel-header.mat-expanded:hover {
  background: inherit;
}
.mat-expansion-panel-header.mat-expansion-toggle-indicator-before {
  flex-direction: row-reverse;
}
.mat-expansion-panel-header.mat-expansion-toggle-indicator-before .mat-expansion-indicator {
  margin: 0 16px 0 0;
}
[dir=rtl] .mat-expansion-panel-header.mat-expansion-toggle-indicator-before .mat-expansion-indicator {
  margin: 0 0 0 16px;
}

.mat-content {
  display: flex;
  flex: 1;
  flex-direction: row;
  overflow: hidden;
}
.mat-content.mat-content-hide-toggle {
  margin-right: 8px;
}
[dir=rtl] .mat-content.mat-content-hide-toggle {
  margin-right: 0;
  margin-left: 8px;
}
.mat-expansion-toggle-indicator-before .mat-content.mat-content-hide-toggle {
  margin-left: 24px;
  margin-right: 0;
}
[dir=rtl] .mat-expansion-toggle-indicator-before .mat-content.mat-content-hide-toggle {
  margin-right: 24px;
  margin-left: 0;
}

.mat-expansion-panel-header-title {
  color: var(--mat-expansion-header-text-color, var(--mat-sys-on-surface));
}

.mat-expansion-panel-header-title,
.mat-expansion-panel-header-description {
  display: flex;
  flex-grow: 1;
  flex-basis: 0;
  margin-right: 16px;
  align-items: center;
}
[dir=rtl] .mat-expansion-panel-header-title,
[dir=rtl] .mat-expansion-panel-header-description {
  margin-right: 0;
  margin-left: 16px;
}
.mat-expansion-panel-header[aria-disabled=true] .mat-expansion-panel-header-title,
.mat-expansion-panel-header[aria-disabled=true] .mat-expansion-panel-header-description {
  color: inherit;
}

.mat-expansion-panel-header-description {
  flex-grow: 2;
  color: var(--mat-expansion-header-description-color, var(--mat-sys-on-surface-variant));
}

.mat-expansion-panel-animations-enabled .mat-expansion-indicator {
  transition: transform 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel-header.mat-expanded .mat-expansion-indicator {
  transform: rotate(180deg);
}
.mat-expansion-indicator::after {
  border-style: solid;
  border-width: 0 2px 2px 0;
  content: "";
  padding: 3px;
  transform: rotate(45deg);
  vertical-align: middle;
  color: var(--mat-expansion-header-indicator-color, var(--mat-sys-on-surface-variant));
  display: var(--mat-expansion-legacy-header-indicator-display, none);
}
.mat-expansion-indicator svg {
  width: 24px;
  height: 24px;
  margin: 0 -8px;
  vertical-align: middle;
  fill: var(--mat-expansion-header-indicator-color, var(--mat-sys-on-surface-variant));
  display: var(--mat-expansion-header-indicator-display, inline-block);
}

@media (forced-colors: active) {
  .mat-expansion-panel-content {
    border-top: 1px solid;
    border-top-left-radius: 0;
    border-top-right-radius: 0;
  }
}
`],encapsulation:2,changeDetection:0})}return t})(),BM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-panel-description"]],hostAttrs:[1,"mat-expansion-panel-header-description"]})}return t})(),HM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-panel-title"]],hostAttrs:[1,"mat-expansion-panel-header-title"]})}return t})(),UM=(()=>{class t extends FM{_keyManager;_ownHeaders=new Bn;_headers;hideToggle=!1;displayMode="default";togglePosition="after";ngAfterContentInit(){this._headers.changes.pipe(Ie(this._headers)).subscribe(e=>{this._ownHeaders.reset(e.filter(i=>i.panel.accordion===this)),this._ownHeaders.notifyOnChanges()}),this._keyManager=new Ca(this._ownHeaders).withWrap().withHomeAndEnd()}_handleHeaderKeydown(e){this._keyManager.onKeydown(e)}_handleHeaderFocus(e){this._keyManager.updateActiveItem(e)}ngOnDestroy(){super.ngOnDestroy(),this._keyManager?.destroy(),this._ownHeaders.destroy()}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["mat-accordion"]],contentQueries:function(i,r,o){if(i&1&&ut(o,vb,5),i&2){let a;H(a=U())&&(r._headers=a)}},hostAttrs:[1,"mat-accordion"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-accordion-multi",r.multi)},inputs:{hideToggle:[2,"hideToggle","hideToggle",Z],displayMode:"displayMode",togglePosition:"togglePosition"},exportAs:["matAccordion"],features:[we([{provide:gb,useExisting:t}]),pe]})}return t})(),zM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({imports:[LM,Lr,Ce]})}return t})();function Aj(t,n){}var Do=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var yb=(()=>{class t extends Hs{_elementRef=d(N);_focusTrapFactory=d($c);_config;_interactivityChecker=d(Ps);_ngZone=d(P);_focusMonitor=d(On);_renderer=d(Se);_changeDetectorRef=d(me);_injector=d(G);_platform=d(Ee);_document=d(W);_portalOutlet;_focusTrapped=new E;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(Do,{optional:!0})||new Do,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||ft(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=ho(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=ho();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=ho()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&qe(Ea,7),i&2){let o;H(o=U())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&J("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[pe],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&A(0,Aj,0,0,"ng-template",0)},dependencies:[Ea],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),dd=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new E;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!Ye(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},kj=new y("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(G);return()=>$s(t)}}),Oj=new y("DialogData"),Rj=new y("DefaultDialogConfig");function Nj(t){let n=ee(t),e=new F;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var Cb=(()=>{class t{_injector=d(G);_defaultOptions=d(Rj,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(Nf);_idGenerator=d(ze);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new E;_afterOpenedAtThisLevel=new E;_ariaHiddenElements=new Map;_scrollStrategy=d(kj);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=jn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Ie(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new Do;i=C(C({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),a=ki(this._injector,o),s=new dd(a,i),l=this._attachContainer(a,s,i);if(s.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(ke(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,s,l,i),this.openDialogs.push(s),s.closed.subscribe(()=>this._removeOpenDialog(s,!0)),this.afterOpened.next(s),s}closeAll(){bb(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){bb(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),bb(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new Ti({positionStrategy:e.positionStrategy||qs().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,a=[{provide:Do,useValue:r},{provide:dd,useValue:i},{provide:Us,useValue:e}],s;r.container?typeof r.container=="function"?s=r.container:(s=r.container.type,a.push(...r.container.providers(r))):s=yb;let l=new yo(s,r.viewContainerRef,G.create({parent:o||this._injector,providers:a}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof St){let a=this._createInjector(o,i,r,void 0),s={$implicit:o.data,dialogRef:i};o.templateContext&&(s=C(C({},s),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Wn(e,null,s,a))}else{let a=this._createInjector(o,i,r,this._injector),s=r.attachComponentPortal(new yo(e,o.viewContainerRef,a));i.componentRef=s,i.componentInstance=s.instance}}_createInjector(e,i,r,o){let a=e.injector||e.viewContainerRef?.injector,s=[{provide:Oj,useValue:e.data},{provide:dd,useValue:i}];return e.providers&&(typeof e.providers=="function"?s.push(...e.providers(i,e,r)):s.push(...e.providers)),e.direction&&(!a||!a.get(It,null,{optional:!0}))&&s.push({provide:It,useValue:Nj(e.direction)}),G.create({parent:a||o,providers:s})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,a)=>{o?a.setAttribute("aria-hidden",o):a.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function bb(t,n){let e=t.length;for(;e--;)n(t[e])}var $M=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({providers:[Cb],imports:[Yn,Lr,qc,Lr]})}return t})();function Fj(t,n){}var op=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},xb="mdc-dialog--open",qM="mdc-dialog--opening",GM="mdc-dialog--closing",Pj=150,Lj=75,jj=(()=>{class t extends yb{_animationStateChanged=new F;_animationsEnabled=!He();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?YM(this._config.enterAnimationDuration)??Pj:0;_exitAnimationDuration=this._animationsEnabled?YM(this._config.exitAnimationDuration)??Lj:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(WM,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(qM,xb)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(xb),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(xb),this._animationsEnabled?(this._hostElement.style.setProperty(WM,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(GM)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(qM,GM)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(dn("id",r._config.id),J("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),R("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[pe],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(m(0,"div",0)(1,"div",1),A(2,Fj,0,0,"ng-template",2),u()())},dependencies:[Ea],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2})}return t})(),WM="--mat-dialog-transition-duration";function YM(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?pn(t.substring(0,t.length-2)):t.endsWith("s")?pn(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var rp=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(rp||{}),Js=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new $r(1);_beforeClosed=new $r(1);_result;_closeFallbackTimeout;_state=rp.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(oe(r=>r.state==="opened"),ke(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(oe(r=>r.state==="closed"),ke(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),at(this.backdropClick(),this.keydownEvents().pipe(oe(r=>r.keyCode===27&&!this.disableClose&&!Ye(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),Vj(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(oe(i=>i.state==="closing"),ke(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=rp.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=rp.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function Vj(t,n,e){return t._closeInteractionType=n,t.close(e)}var wb=new y("MatMdcDialogData"),Bj=new y("mat-mdc-dialog-default-options"),Hj=new y("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(G);return()=>$s(t)}}),ap=(()=>{class t{_defaultOptions=d(Bj,{optional:!0});_scrollStrategy=d(Hj);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(ze);_injector=d(G);_dialog=d(Cb);_animationsDisabled=He();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new E;_afterOpenedAtThisLevel=new E;dialogConfigClass=op;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=jn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Ie(void 0)));constructor(){this._dialogRefConstructor=Js,this._dialogContainerType=jj,this._dialogDataToken=wb}open(e,i){let r;i=C(C({},this._defaultOptions||new op),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,ne(C({},i),{positionStrategy:qs(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:Do,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(a,s,l)=>(r=new this._dialogRefConstructor(a,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:s.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let a=this.openDialogs.indexOf(r);a>-1&&(this.openDialogs.splice(a,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ZM=(()=>{class t{_dialogRef=d(Js,{optional:!0});_elementRef=d(N);_dialog=d(ap);constructor(){}ngOnInit(){this._dialogRef||(this._dialogRef=Uj(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t})}return t})(),QM=(()=>{class t extends ZM{id=d(ze).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&dn("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[pe]})}return t})(),KM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[u_([Pr])]})}return t})(),XM=(()=>{class t extends ZM{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&R("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[pe]})}return t})();function Uj(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var sp=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=j({type:t});static \u0275inj=L({providers:[ap],imports:[$M,Yn,Lr,Ce]})}return t})();var lp=class t{http=d(Je);apiUrl="/api/Ediciones";getEdiciones(){return this.http.get(this.apiUrl)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};function $j(t,n){if(t&1&&(m(0,"div",20)(1,"mat-icon"),p(2,"error_outline"),u(),m(3,"p"),p(4),u()()),t&2){let e=I();h(4),B(e.error)}}function qj(t,n){t&1&&(m(0,"mat-error"),p(1," El n\xFAmero es obligatorio. "),u())}function Gj(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;v("value",e),h(),Y(" ",e.nombre," ")}}function Wj(t,n){t&1&&(m(0,"mat-error"),p(1," El equipo es obligatorio. "),u())}function Yj(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;v("value",e),h(),Y(" ",e.nombreCompleto||e.nombre," ")}}function Zj(t,n){t&1&&(m(0,"mat-error"),p(1," El jugador es obligatorio. "),u())}function Qj(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;v("value",e.id),h(),Y(" ",e.nombre," ")}}function Kj(t,n){t&1&&(m(0,"mat-error"),p(1," La edici\xF3n es obligatoria. "),u())}function Xj(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;v("value",e),h(),Y(" ",e.nombre," ")}}function Jj(t,n){t&1&&(m(0,"mat-error"),p(1," El tipo de cromo es obligatorio. "),u())}function eV(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;v("value",e.id),h(),Y(" ",e.nombre," ")}}function tV(t,n){t&1&&(m(0,"mat-error"),p(1," El \xE1lbum es obligatorio. "),u())}function nV(t,n){t&1&&te(0,"mat-spinner",22)}var cp=class t{constructor(n,e,i,r,o,a,s,l,c){this.data=n;this.dialogRef=e;this.fb=i;this.http=r;this.equipoService=o;this.edicionService=a;this.tipoCromoService=s;this.albumService=l;this.cdr=c}data;dialogRef;fb;http;equipoService;edicionService;tipoCromoService;albumService;cdr;cromoForm;equipos=[];ediciones=[];tiposCromo=[];albumes=[];jugadores=[];filteredEquipos$;filteredJugadores$;filteredTiposCromo$;cargando=!1;error="";apiUrl="/api";ngOnInit(){this.crearFormulario(),this.cargarDatosMaestros()}crearFormulario(){let n=this.data.cromo;this.cromoForm=this.fb.group({numero:[n.numero,fe.required],equipo:[C({},n.equipo),fe.required],jugador:[{value:C({},n.jugador),disabled:!1},fe.required],edicionId:[n.edicion.id,fe.required],tipoCromo:[C({},n.tipoCromo),fe.required],albumId:[this.data.albumId,fe.required]})}cargarDatosMaestros(){this.equipoService.getEquipos().subscribe({next:n=>{this.equipos=n,this.filteredEquipos$=this.cromoForm.get("equipo").valueChanges.pipe(Ie(""),$(e=>this.filtrarEquipos(e))),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los equipos."}}),this.edicionService.getEdiciones().subscribe({next:n=>{this.ediciones=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar las ediciones."}}),this.tipoCromoService.getTiposCromo().subscribe({next:n=>{this.tiposCromo=n,this.filteredTiposCromo$=this.cromoForm.get("tipoCromo").valueChanges.pipe(Ie(""),$(e=>this.filtrarTiposCromo(e))),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los tipos de cromo."}}),this.albumService.getAlbumes().subscribe({next:n=>{this.albumes=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los \xE1lbumes."}}),this.cargarJugadoresDelEquipo(this.data.cromo.equipo.id)}filtrarEquipos(n){let e=(typeof n=="string"?n:n?.nombre??"").toLowerCase();return this.equipos.filter(i=>i.nombre.toLowerCase().includes(e))}onEquipoInput(n){this.cromoForm.get("equipo")?.setValue(n)}onEquipoSelected(n){this.cromoForm.get("equipo")?.setValue(n),this.onEquipoChange()}displayEquipo(n){return n?.nombre??""}onEquipoChange(){let e=this.cromoForm.get("equipo")?.value?.id;e&&this.cargarJugadoresDelEquipo(e)}cargarJugadoresDelEquipo(n){this.jugadores=[],this.cromoForm.get("jugador")?.setValue(null),this.cromoForm.get("jugador")?.disable(),this.http.get(`${this.apiUrl}/Equipos/${n}/Jugadores`).subscribe({next:e=>{this.jugadores=e,this.filteredJugadores$=this.cromoForm.get("jugador").valueChanges.pipe(Ie(""),$(i=>this.filtrarJugadores(i))),this.cromoForm.get("jugador")?.enable(),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los jugadores."}})}filtrarJugadores(n){let e=(typeof n=="string"?n:"").toLowerCase();return this.jugadores.filter(i=>(i.nombreCompleto||i.nombre).toLowerCase().includes(e))}onJugadorInput(n){this.cromoForm.get("jugador")?.setValue(n)}onJugadorSelected(n){this.cromoForm.get("jugador")?.setValue(n)}displayJugador(n){return n?typeof n=="string"?n:n.nombreCompleto||n.nombre:""}filtrarTiposCromo(n){let e=(typeof n=="string"?n:n?.nombre??"").toLowerCase();return this.tiposCromo.filter(i=>i.nombre.toLowerCase().includes(e))}onTipoCromoInput(n){this.cromoForm.get("tipoCromo")?.setValue(n)}onTipoCromoSelected(n){this.cromoForm.get("tipoCromo")?.setValue(n)}displayTipoCromo(n){return n?.nombre??""}guardar(){if(this.cromoForm.invalid){this.cromoForm.markAllAsTouched();return}this.cargando=!0,this.error="";let n={numero:this.cromoForm.get("numero")?.value,edicionId:this.cromoForm.get("edicionId")?.value,albumId:this.cromoForm.get("albumId")?.value,equipoId:this.cromoForm.get("equipo")?.value?.id,jugadorId:this.cromoForm.get("jugador")?.value?.id,tipoCromoId:this.cromoForm.get("tipoCromo")?.value?.id,publicacionId:null,coleccionId:null};this.http.put(`${this.apiUrl}/Cromo/${this.data.cromo.id}`,n).subscribe({next:()=>{this.cargando=!1,this.dialogRef.close(!0)},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el cromo.",this.cdr.detectChanges()}})}cancelar(){this.dialogRef.close(!1)}static \u0275fac=function(e){return new(e||t)(M(wb),M(Js),M(kn),M(Je),M(dr),M(lp),M(wo),M(Eo),M(me))};static \u0275cmp=T({type:t,selectors:[["app-cromo-editar"]],decls:55,vars:24,consts:[["autoEquipo","matAutocomplete"],["autoJugador","matAutocomplete"],["autoTipo","matAutocomplete"],["mat-dialog-title",""],["class","error",4,"ngIf"],[1,"editar-form",3,"formGroup"],["appearance","outline"],["matInput","","formControlName","numero"],[4,"ngIf"],["type","text","matInput","","formControlName","equipo","placeholder","Buscar equipo...",3,"input","matAutocomplete"],[3,"optionSelected"],[3,"value",4,"ngFor","ngForOf"],["type","text","matInput","","formControlName","jugador","placeholder","Buscar jugador...",3,"input","matAutocomplete"],["formControlName","edicionId"],["type","text","matInput","","formControlName","tipoCromo","placeholder","Buscar tipo...",3,"input","matAutocomplete"],["formControlName","albumId"],["align","end"],["mat-button","",3,"click"],["mat-raised-button","","color","primary",3,"click","disabled"],["diameter","18","style","display:inline-block; margin-right:8px",4,"ngIf"],[1,"error"],[3,"value"],["diameter","18",2,"display","inline-block","margin-right","8px"]],template:function(e,i){if(e&1&&(m(0,"h2",3),p(1,"Editar cromo"),u(),m(2,"mat-dialog-content"),A(3,$j,5,1,"div",4),m(4,"form",5)(5,"mat-form-field",6)(6,"mat-label"),p(7,"N\xFAmero"),u(),te(8,"input",7),A(9,qj,2,0,"mat-error",8),u(),m(10,"mat-form-field",6)(11,"mat-label"),p(12,"Equipo"),u(),m(13,"input",9),D("input",function(o){return i.onEquipoInput(o.target.value)}),u(),m(14,"mat-autocomplete",10,0),D("optionSelected",function(o){return i.onEquipoSelected(o.option.value)}),A(16,Gj,2,2,"mat-option",11),Sr(17,"async"),u(),A(18,Wj,2,0,"mat-error",8),u(),m(19,"mat-form-field",6)(20,"mat-label"),p(21,"Jugador"),u(),m(22,"input",12),D("input",function(o){return i.onJugadorInput(o.target.value)}),u(),m(23,"mat-autocomplete",10,1),D("optionSelected",function(o){return i.onJugadorSelected(o.option.value)}),A(25,Yj,2,2,"mat-option",11),Sr(26,"async"),u(),A(27,Zj,2,0,"mat-error",8),u(),m(28,"mat-form-field",6)(29,"mat-label"),p(30,"Edici\xF3n"),u(),m(31,"mat-select",13),A(32,Qj,2,2,"mat-option",11),u(),A(33,Kj,2,0,"mat-error",8),u(),m(34,"mat-form-field",6)(35,"mat-label"),p(36,"Tipo de cromo"),u(),m(37,"input",14),D("input",function(o){return i.onTipoCromoInput(o.target.value)}),u(),m(38,"mat-autocomplete",10,2),D("optionSelected",function(o){return i.onTipoCromoSelected(o.option.value)}),A(40,Xj,2,2,"mat-option",11),Sr(41,"async"),u(),A(42,Jj,2,0,"mat-error",8),u(),m(43,"mat-form-field",6)(44,"mat-label"),p(45,"\xC1lbum"),u(),m(46,"mat-select",15),A(47,eV,2,2,"mat-option",11),u(),A(48,tV,2,0,"mat-error",8),u()()(),m(49,"mat-dialog-actions",16)(50,"button",17),D("click",function(){return i.cancelar()}),p(51,"Cancelar"),u(),m(52,"button",18),D("click",function(){return i.guardar()}),A(53,nV,1,0,"mat-spinner",19),p(54," Guardar "),u()()),e&2){let r,o,a,s,l,c,f=Be(15),g=Be(24),_=Be(39);h(3),v("ngIf",i.error),h(),v("formGroup",i.cromoForm),h(5),v("ngIf",(r=i.cromoForm.get("numero"))==null?null:r.hasError("required")),h(4),v("matAutocomplete",f),h(3),v("ngForOf",Tr(17,18,i.filteredEquipos$)),h(2),v("ngIf",(o=i.cromoForm.get("equipo"))==null?null:o.hasError("required")),h(4),v("matAutocomplete",g),h(3),v("ngForOf",Tr(26,20,i.filteredJugadores$)),h(2),v("ngIf",(a=i.cromoForm.get("jugador"))==null?null:a.hasError("required")),h(5),v("ngForOf",i.ediciones),h(),v("ngIf",(s=i.cromoForm.get("edicionId"))==null?null:s.hasError("required")),h(4),v("matAutocomplete",_),h(3),v("ngForOf",Tr(41,22,i.filteredTiposCromo$)),h(2),v("ngIf",(l=i.cromoForm.get("tipoCromo"))==null?null:l.hasError("required")),h(5),v("ngForOf",i.albumes),h(),v("ngIf",(c=i.cromoForm.get("albumId"))==null?null:c.hasError("required")),h(4),v("disabled",i.cargando),h(),v("ngIf",i.cargando)}},dependencies:[Rt,Ot,Ut,fn,Jt,pt,Nt,Xt,zt,wn,sp,QM,XM,KM,gt,Vf,Rn,rd,_t,Pt,mt,et,Ft,Xs,Ks,bt,vt,tt,nt,Ge,rt,it,yt,Ct,xt,xo,wt,Et,rc],styles:[".error[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;color:#f44336;margin-bottom:12px}.error[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0}"]})};function iV(t,n){t&1&&(m(0,"div",4),te(1,"mat-spinner",5),m(2,"p"),p(3,"Cargando colecci\xF3n..."),u()())}function rV(t,n){if(t&1&&(m(0,"div",6)(1,"mat-icon"),p(2,"error_outline"),u(),m(3,"p"),p(4),u(),m(5,"a",7)(6,"mat-icon"),p(7,"arrow_back"),u(),p(8," Volver a \xE1lbumes "),u()()),t&2){let e=I();h(4),B(e.error)}}function oV(t,n){if(t&1&&(m(0,"p",25),p(1),u()),t&2){let e=I().$implicit;h(),B(e.edicion.nombre)}}function aV(t,n){if(t&1){let e=Oe();m(0,"mat-card",19)(1,"mat-card-content")(2,"span",20),p(3),u(),m(4,"p",21),p(5),u(),m(6,"p",22),p(7),u(),A(8,oV,2,1,"p",23),u(),m(9,"button",24),D("click",function(){let r=se(e).$implicit,o=I(4);return le(o.editarCromo(r))}),m(10,"mat-icon"),p(11,"edit"),u()()()}if(t&2){let e=n.$implicit,i=I(4);h(3),B(e.numero),h(2),B(e.jugador.nombre),h(2),B(e.tipoCromo.nombre),h(),v("ngIf",i.mostrarEdicion(e))}}function sV(t,n){if(t&1&&(m(0,"div",17),A(1,aV,12,4,"mat-card",18),u()),t&2){let e=I().$implicit,i=I(2);h(),v("ngForOf",i.cromosDeEquipo(e))}}function lV(t,n){t&1&&(m(0,"mat-card",27)(1,"mat-card-content")(2,"span",20),p(3,"1"),u(),m(4,"p",21),p(5,"Sin cromo"),u(),m(6,"p",22),p(7,"\u2014"),u()()())}function cV(t,n){if(t&1&&(m(0,"div",17),A(1,lV,8,0,"mat-card",26),u()),t&2){let e=I(3);h(),v("ngForOf",e.placeholders)}}function dV(t,n){if(t&1&&(m(0,"mat-expansion-panel")(1,"mat-expansion-panel-header")(2,"mat-panel-title")(3,"mat-icon"),p(4,"groups"),u(),p(5),u(),m(6,"mat-panel-description"),p(7),u()(),A(8,sV,2,1,"div",16)(9,cV,2,1,"div",16),u()),t&2){let e=n.$implicit,i=I(2);h(5),Y(" ",e.nombre," "),h(2),kt(" ",i.cromosDeEquipo(e).length," cromo",i.cromosDeEquipo(e).length!==1?"s":""," "),h(),v("ngIf",i.cromosDeEquipo(e).length>0),h(),v("ngIf",i.cromosDeEquipo(e).length===0)}}function uV(t,n){t&1&&(m(0,"div",28)(1,"mat-icon"),p(2,"info"),u(),m(3,"p"),p(4,"Este \xE1lbum no tiene equipos definidos todav\xEDa."),u()())}function mV(t,n){if(t&1&&(m(0,"p",25),p(1),u()),t&2){let e=I().$implicit;h(),B(e.edicion.nombre)}}function fV(t,n){if(t&1){let e=Oe();m(0,"mat-card",19)(1,"mat-card-content")(2,"span",20),p(3),u(),m(4,"p",21),p(5),u(),m(6,"p",32),p(7),u(),A(8,mV,2,1,"p",23),u(),m(9,"button",24),D("click",function(){let r=se(e).$implicit,o=I(4);return le(o.editarCromo(r))}),m(10,"mat-icon"),p(11,"edit"),u()()()}if(t&2){let e=n.$implicit,i=I(4);h(3),B(e.numero),h(2),B(e.jugador.nombre),h(2),B(e.equipo.nombre),h(),v("ngIf",i.mostrarEdicion(e))}}function pV(t,n){if(t&1&&(m(0,"mat-expansion-panel",31)(1,"mat-expansion-panel-header")(2,"mat-panel-title")(3,"mat-icon"),p(4,"star"),u(),p(5),u(),m(6,"mat-panel-description"),p(7),u()(),m(8,"div",17),A(9,fV,12,4,"mat-card",18),u()()),t&2){let e=n.$implicit;h(5),Y(" ",e.tipo," "),h(2),kt(" ",e.cromos.length," cromo",e.cromos.length!==1?"s":""," "),h(2),v("ngForOf",e.cromos)}}function hV(t,n){if(t&1&&(Kl(0),m(1,"mat-accordion",29),A(2,pV,10,4,"mat-expansion-panel",30),u(),Xl()),t&2){let e=I(2);h(2),v("ngForOf",e.especiales)("ngForTrackBy",e.trackTipo)}}function gV(t,n){if(t&1&&(m(0,"div",8)(1,"header",9)(2,"a",10)(3,"mat-icon"),p(4,"arrow_back"),u()(),m(5,"div")(6,"h1"),p(7),u(),m(8,"p",11),p(9),u()()(),m(10,"mat-accordion",12),A(11,dV,10,5,"mat-expansion-panel",13),u(),A(12,uV,5,0,"div",14)(13,hV,3,2,"ng-container",15),u()),t&2){let e=I();h(7),B(e.album.nombre),h(2),cm("",e.album.temporada," \xB7 ",e.album.equipos.length," equipo",e.album.equipos.length!==1?"s":""),h(2),v("ngForOf",e.album.equipos)("ngForTrackBy",e.trackEquipo),h(),v("ngIf",e.album.equipos.length===0),h(),v("ngIf",e.especiales.length>0)}}var dp=class t{constructor(n,e,i,r){this.route=n;this.albumService=e;this.dialog=i;this.cdr=r}route;albumService;dialog;cdr;album=null;cargando=!0;error="";placeholders=[1,2,3,4];especiales=[];editarCromo(n){if(!this.album)return;this.dialog.open(cp,{width:"520px",data:{cromo:n,albumId:this.album.id}}).afterClosed().subscribe(i=>{i&&this.album&&this.cargarDetalle(this.album.id)})}ngOnInit(){let n=Number(this.route.snapshot.paramMap.get("id"));if(!n){this.error="ID de \xE1lbum no v\xE1lido.",this.cargando=!1;return}this.cargarDetalle(n)}trackEquipo(n,e){return e.id}trackTipo(n,e){return e.tipo}esBasico(n){let e=(n||"").trim().toLowerCase();return e==="b\xE1sica"||e==="basica"||e==="coloca"||e==="baja"}cromosDeEquipo(n){return n.cromos.filter(e=>this.esBasico(e.tipoCromo.nombre)).sort((e,i)=>this.compararNumero(e,i))}mostrarEdicion(n){return n.edicion!=null&&n.edicion.id!==1}compararNumero(n,e){return this.numeroValor(n.numero)-this.numeroValor(e.numero)}numeroValor(n){let e=(n||"").match(/\d+/);return e?parseInt(e[0],10):Number.MAX_SAFE_INTEGER}construirEspeciales(n){let e=new Map;for(let i of n.equipos)for(let r of i.cromos){if(this.esBasico(r.tipoCromo.nombre))continue;let o=r.tipoCromo.nombre;e.has(o)||e.set(o,[]),e.get(o).push(r)}this.especiales=Array.from(e.entries()).map(([i,r])=>({tipo:i,cromos:r.sort((o,a)=>this.compararNumero(o,a))})).sort((i,r)=>i.tipo<r.tipo?-1:1)}cargarDetalle(n){this.cargando=!0,this.albumService.getAlbumConCromos(n).subscribe({next:e=>{this.album=e,this.construirEspeciales(e),this.cargando=!1,this.cdr.detectChanges()},error:e=>{this.error="No se ha podido cargar el \xE1lbum.",this.cargando=!1,this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(M(Ii),M(Eo),M(ap),M(me))};static \u0275cmp=T({type:t,selectors:[["app-coleccion-detalle"]],decls:4,vars:3,consts:[[1,"page-container"],["class","cargando",4,"ngIf"],["class","error",4,"ngIf"],["class","detalle",4,"ngIf"],[1,"cargando"],["diameter","40"],[1,"error"],["mat-stroked-button","","color","primary","routerLink","/albumes"],[1,"detalle"],[1,"detalle-header"],["mat-icon-button","","routerLink","/albumes",1,"back-btn"],[1,"subtitle"],["multi",""],[4,"ngFor","ngForOf","ngForTrackBy"],["class","sin-equipos",4,"ngIf"],[4,"ngIf"],["class","cromos-grid",4,"ngIf"],[1,"cromos-grid"],["class","cromo-card",4,"ngFor","ngForOf"],[1,"cromo-card"],[1,"cromo-numero"],[1,"cromo-jugador"],[1,"cromo-tipo"],["class","cromo-edicion",4,"ngIf"],["mat-icon-button","","aria-label","Editar cromo",1,"cromo-editar-btn",3,"click"],[1,"cromo-edicion"],["class","cromo-card placeholder",4,"ngFor","ngForOf"],[1,"cromo-card","placeholder"],[1,"sin-equipos"],["multi","",1,"especiales-accordion"],["class","especiales-panel",4,"ngFor","ngForOf","ngForTrackBy"],[1,"especiales-panel"],[1,"cromo-equipo"]],template:function(e,i){e&1&&(m(0,"div",0),A(1,iV,4,0,"div",1)(2,rV,9,1,"div",2)(3,gV,14,8,"div",3),u()),e&2&&(h(),v("ngIf",i.cargando),h(),v("ngIf",i.error&&!i.cargando),h(),v("ngIf",i.album&&!i.cargando))},dependencies:[Rt,Ot,Ut,mn,tt,Lt,jt,rt,it,zM,UM,_b,vb,HM,BM,Xs,Ks,sp],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:var(--app-bg)}.cargando[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:16px;padding:80px 24px;color:var(--app-muted)}.error[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:12px;padding:80px 24px;color:#c62828}.error[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:48px;width:48px;height:48px}.detalle[_ngcontent-%COMP%]{max-width:960px;margin:0 auto;display:flex;flex-direction:column;gap:24px}.detalle-header[_ngcontent-%COMP%]{display:flex;align-items:center;gap:16px}.detalle-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:28px;font-weight:700;margin:0;color:var(--app-text)}.detalle-header[_ngcontent-%COMP%]   .subtitle[_ngcontent-%COMP%]{margin:4px 0 0;color:var(--app-muted);font-size:14px}.back-btn[_ngcontent-%COMP%]{color:var(--app-primary)}mat-accordion[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:12px}mat-expansion-panel[_ngcontent-%COMP%]{border-radius:12px!important;border:1px solid var(--app-border);box-shadow:none!important}mat-expansion-panel[_ngcontent-%COMP%]:hover{border-color:var(--app-primary)}mat-panel-title[_ngcontent-%COMP%]{display:flex;align-items:center;gap:10px;font-weight:600;font-size:16px}mat-panel-title[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--app-primary)}mat-panel-description[_ngcontent-%COMP%]{font-size:13px}.cromos-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:12px;padding-top:8px}.cromo-card[_ngcontent-%COMP%]{position:relative;border-radius:12px;border:1px solid var(--app-border);text-align:center;transition:border-color .2s}.cromo-card[_ngcontent-%COMP%]:hover{border-color:var(--app-primary)}.cromo-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{padding:20px 12px!important;display:flex;flex-direction:column;align-items:center;gap:6px}.cromo-editar-btn[_ngcontent-%COMP%]{position:absolute;top:4px;right:4px;width:28px;height:28px;line-height:28px;color:var(--app-muted)}.cromo-editar-btn[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:16px;width:16px;height:16px;line-height:16px}.cromo-editar-btn[_ngcontent-%COMP%]:hover{color:var(--app-primary);background:var(--app-primary-soft)}.cromo-numero[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:center;width:48px;height:48px;border-radius:50%;background:var(--app-primary-soft);color:var(--app-primary);font-size:20px;font-weight:700}.cromo-jugador[_ngcontent-%COMP%]{margin:0;font-weight:600;font-size:13px;color:var(--app-text)}.cromo-tipo[_ngcontent-%COMP%]{margin:0;font-size:12px;color:var(--app-muted)}.cromo-equipo[_ngcontent-%COMP%]{margin:0;font-size:12px;color:var(--app-muted);font-style:italic}.cromo-edicion[_ngcontent-%COMP%]{margin:0;font-size:11px;color:var(--app-primary);font-weight:600}.especiales-accordion[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:12px}.especiales-panel[_ngcontent-%COMP%]{border-radius:12px!important;border:1px solid var(--app-border);box-shadow:none!important;background:var(--app-primary-soft)}.especiales-panel[_ngcontent-%COMP%]:hover{border-color:var(--app-primary)}.especiales-panel[_ngcontent-%COMP%]   .mat-expansion-panel-header[_ngcontent-%COMP%]{border-radius:12px}.cromo-card.placeholder[_ngcontent-%COMP%]{border-style:dashed;opacity:.5}.cromo-card.placeholder[_ngcontent-%COMP%]   .cromo-numero[_ngcontent-%COMP%]{background:var(--app-border);color:var(--app-muted)}.cromo-card.placeholder[_ngcontent-%COMP%]   .cromo-jugador[_ngcontent-%COMP%]{color:var(--app-muted);font-style:italic}.sin-equipos[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:12px;padding:60px 24px;color:var(--app-muted)}.sin-equipos[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:48px;width:48px;height:48px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}.detalle-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:22px}.cromos-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:8px}}"]})};function _V(t,n){t&1&&(m(0,"mat-error"),p(1,"Introduce un email v\xE1lido."),u())}function vV(t,n){t&1&&(m(0,"mat-error"),p(1,"La contrase\xF1a es obligatoria."),u())}function bV(t,n){if(t&1&&(m(0,"div",10),p(1),u()),t&2){let e=I();h(),B(e.error)}}var up=class t{auth=d(cr);router=d(xn);email="";contrasena="";error="";loading=!1;onSubmit(){this.error="",this.loading=!0,this.auth.login(this.email,this.contrasena).subscribe({next:()=>{this.router.navigate(["/panel"])},error:n=>{this.loading=!1,this.error=n.error?.message||"Email o contrase\xF1a incorrectos."}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-login"]],decls:30,vars:7,consts:[["loginForm","ngForm"],["emailField","ngModel"],["passField","ngModel"],[1,"auth-container"],[1,"auth-card"],[1,"auth-icon"],[3,"ngSubmit"],["appearance","outline",1,"full-width"],["matInput","","type","email","name","email","required","","email","",3,"ngModelChange","ngModel"],["matInput","","type","password","name","contrasena","required","",3,"ngModelChange","ngModel"],[1,"error-message"],["mat-raised-button","","color","primary","type","submit",1,"full-width","submit-btn",3,"disabled"],["align","end"],["mat-button","","routerLink","/registro"]],template:function(e,i){if(e&1){let r=Oe();m(0,"div",3)(1,"mat-card",4)(2,"mat-card-header")(3,"mat-card-title")(4,"mat-icon",5),p(5,"lock"),u(),p(6," Iniciar sesi\xF3n "),u(),m(7,"mat-card-subtitle"),p(8,"Accede a tu cuenta de CromosList"),u()(),m(9,"mat-card-content")(10,"form",6,0),D("ngSubmit",function(){return i.onSubmit()}),m(12,"mat-form-field",7)(13,"mat-label"),p(14,"Email"),u(),m(15,"input",8,1),er("ngModelChange",function(a){return se(r),Ir(i.email,a)||(i.email=a),le(a)}),u(),ge(17,_V,2,0,"mat-error"),u(),m(18,"mat-form-field",7)(19,"mat-label"),p(20,"Contrase\xF1a"),u(),m(21,"input",9,2),er("ngModelChange",function(a){return se(r),Ir(i.contrasena,a)||(i.contrasena=a),le(a)}),u(),ge(23,vV,2,0,"mat-error"),u(),ge(24,bV,2,1,"div",10),m(25,"button",11),p(26),u()()(),m(27,"mat-card-actions",12)(28,"a",13),p(29,"\xBFNo tienes cuenta? Reg\xEDstrate"),u()()()()}if(e&2){let r=Be(11),o=Be(16),a=Be(22);h(15),Ji("ngModel",i.email),h(2),_e(o.invalid&&o.touched?17:-1),h(4),Ji("ngModel",i.contrasena),h(2),_e(a.invalid&&a.touched?23:-1),h(),_e(i.error?24:-1),h(),v("disabled",r.invalid||i.loading),h(),Y(" ",i.loading?"Entrando...":"Iniciar sesi\xF3n"," ")}},dependencies:[hf,Jt,pt,Nt,Xt,Vc,Bc,Lc,Nr,mn,bt,vt,gt,tt,Lt,gn,jt,nn,tn,en,nt,Ge,mt,et,Ft,rt,it,_t,Pt,yt,Ct,xt,wt,Et],styles:[".auth-container[_ngcontent-%COMP%]{display:flex;justify-content:center;align-items:center;min-height:calc(100vh - 112px)}.auth-card[_ngcontent-%COMP%]{width:100%;max-width:420px;padding:24px}.auth-card[_ngcontent-%COMP%]   mat-card-header[_ngcontent-%COMP%]{margin-bottom:16px}.auth-card[_ngcontent-%COMP%]   mat-card-title[_ngcontent-%COMP%]{display:flex;align-items:center;gap:10px;font-size:22px}.auth-icon[_ngcontent-%COMP%]{font-size:28px;width:28px;height:28px;color:var(--app-primary)}.full-width[_ngcontent-%COMP%]{width:100%}.submit-btn[_ngcontent-%COMP%]{margin-top:8px;height:44px;font-size:15px}.error-message[_ngcontent-%COMP%]{background:#fdecea;color:#b71c1c;padding:10px 14px;border-radius:8px;font-size:13px;margin-bottom:8px}mat-card-actions[_ngcontent-%COMP%]{padding:0 16px 8px!important}"]})};function yV(t,n){t&1&&(m(0,"mat-error"),p(1,"El nombre es obligatorio."),u())}function CV(t,n){t&1&&(m(0,"mat-error"),p(1,"Introduce un email v\xE1lido."),u())}function xV(t,n){t&1&&(m(0,"mat-error"),p(1,"M\xEDnimo 6 caracteres."),u())}function wV(t,n){t&1&&(m(0,"mat-error"),p(1,"Las contrase\xF1as no coinciden."),u())}function EV(t,n){if(t&1&&(m(0,"div",14),p(1),u()),t&2){let e=I();h(),B(e.error)}}var mp=class t{auth=d(cr);router=d(xn);nombre="";email="";contrasena="";confirmar="";error="";loading=!1;get passwordsMatch(){return this.contrasena===this.confirmar}onSubmit(){if(this.error="",!this.passwordsMatch){this.error="Las contrase\xF1as no coinciden.";return}this.loading=!0,this.auth.register(this.nombre,this.email,this.contrasena).subscribe({next:()=>{this.router.navigate(["/panel"])},error:n=>{this.loading=!1,this.error=n.error?.message||"Error al registrarse."}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-registro"]],decls:42,vars:11,consts:[["registerForm","ngForm"],["nameField","ngModel"],["emailField","ngModel"],["passField","ngModel"],["confirmField","ngModel"],[1,"auth-container"],[1,"auth-card"],[1,"auth-icon"],[3,"ngSubmit"],["appearance","outline",1,"full-width"],["matInput","","name","nombre","required","",3,"ngModelChange","ngModel"],["matInput","","type","email","name","email","required","","email","",3,"ngModelChange","ngModel"],["matInput","","type","password","name","contrasena","required","","minlength","6",3,"ngModelChange","ngModel"],["matInput","","type","password","name","confirmar","required","",3,"ngModelChange","ngModel"],[1,"error-message"],["mat-raised-button","","color","primary","type","submit",1,"full-width","submit-btn",3,"disabled"],["align","end"],["mat-button","","routerLink","/login"]],template:function(e,i){if(e&1){let r=Oe();m(0,"div",5)(1,"mat-card",6)(2,"mat-card-header")(3,"mat-card-title")(4,"mat-icon",7),p(5,"person_add"),u(),p(6," Crear cuenta "),u(),m(7,"mat-card-subtitle"),p(8,"Reg\xEDstrate para empezar a usar CromosList"),u()(),m(9,"mat-card-content")(10,"form",8,0),D("ngSubmit",function(){return i.onSubmit()}),m(12,"mat-form-field",9)(13,"mat-label"),p(14,"Nombre"),u(),m(15,"input",10,1),er("ngModelChange",function(a){return se(r),Ir(i.nombre,a)||(i.nombre=a),le(a)}),u(),ge(17,yV,2,0,"mat-error"),u(),m(18,"mat-form-field",9)(19,"mat-label"),p(20,"Email"),u(),m(21,"input",11,2),er("ngModelChange",function(a){return se(r),Ir(i.email,a)||(i.email=a),le(a)}),u(),ge(23,CV,2,0,"mat-error"),u(),m(24,"mat-form-field",9)(25,"mat-label"),p(26,"Contrase\xF1a"),u(),m(27,"input",12,3),er("ngModelChange",function(a){return se(r),Ir(i.contrasena,a)||(i.contrasena=a),le(a)}),u(),ge(29,xV,2,0,"mat-error"),u(),m(30,"mat-form-field",9)(31,"mat-label"),p(32,"Confirmar contrase\xF1a"),u(),m(33,"input",13,4),er("ngModelChange",function(a){return se(r),Ir(i.confirmar,a)||(i.confirmar=a),le(a)}),u(),ge(35,wV,2,0,"mat-error"),u(),ge(36,EV,2,1,"div",14),m(37,"button",15),p(38),u()()(),m(39,"mat-card-actions",16)(40,"a",17),p(41,"\xBFYa tienes cuenta? Inicia sesi\xF3n"),u()()()()}if(e&2){let r=Be(11),o=Be(16),a=Be(22),s=Be(28),l=Be(34);h(15),Ji("ngModel",i.nombre),h(2),_e(o.invalid&&o.touched?17:-1),h(4),Ji("ngModel",i.email),h(2),_e(a.invalid&&a.touched?23:-1),h(4),Ji("ngModel",i.contrasena),h(2),_e(s.invalid&&s.touched?29:-1),h(4),Ji("ngModel",i.confirmar),h(2),_e(l.touched&&i.confirmar&&!i.passwordsMatch?35:-1),h(),_e(i.error?36:-1),h(),v("disabled",r.invalid||!i.passwordsMatch||i.loading),h(),Y(" ",i.loading?"Creando cuenta...":"Registrarse"," ")}},dependencies:[hf,Jt,pt,Nt,Xt,Vc,Rv,Bc,Lc,Nr,mn,bt,vt,gt,tt,Lt,gn,jt,nn,tn,en,nt,Ge,mt,et,Ft,rt,it,_t,Pt,yt,Ct,xt,wt,Et],styles:[".auth-container[_ngcontent-%COMP%]{display:flex;justify-content:center;align-items:center;min-height:calc(100vh - 112px)}.auth-card[_ngcontent-%COMP%]{width:100%;max-width:420px;padding:24px}.auth-card[_ngcontent-%COMP%]   mat-card-header[_ngcontent-%COMP%]{margin-bottom:16px}.auth-card[_ngcontent-%COMP%]   mat-card-title[_ngcontent-%COMP%]{display:flex;align-items:center;gap:10px;font-size:22px}.auth-icon[_ngcontent-%COMP%]{font-size:28px;width:28px;height:28px;color:var(--app-primary)}.full-width[_ngcontent-%COMP%]{width:100%}.submit-btn[_ngcontent-%COMP%]{margin-top:8px;height:44px;font-size:15px}.error-message[_ngcontent-%COMP%]{background:#fdecea;color:#b71c1c;padding:10px 14px;border-radius:8px;font-size:13px;margin-bottom:8px}mat-card-actions[_ngcontent-%COMP%]{padding:0 16px 8px!important}"]})};var eI=()=>{let t=d(cr),n=d(xn);return t.getToken()?!0:n.createUrlTree(["/login"])};var tI=[{path:"login",component:up},{path:"registro",component:mp},{path:"",component:Yf,canActivate:[eI],children:[{path:"",redirectTo:"panel",pathMatch:"full"},{path:"panel",component:np},{path:"cromos",component:Wf},{path:"equipos",component:Qf},{path:"jugadores",component:Xf},{path:"tipos-cromo",component:Jf},{path:"albumes",component:ep},{path:"editoriales",component:ip},{path:"album/:id",component:dp}]}];var nI={providers:[Lh(),vv(tI),z_()]};var fp=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-root"]],decls:1,vars:0,template:function(e,i){e&1&&te(0,"router-outlet")},dependencies:[fa],encapsulation:2})};V_(fp,nI).catch(t=>console.error(t));
