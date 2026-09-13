var wI=Object.defineProperty,EI=Object.defineProperties;var DI=Object.getOwnPropertyDescriptors;var Y_=Object.getOwnPropertySymbols;var II=Object.prototype.hasOwnProperty,MI=Object.prototype.propertyIsEnumerable;var Z_=(t,n,e)=>n in t?wI(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,C=(t,n)=>{for(var e in n||={})II.call(n,e)&&Z_(t,e,n[e]);if(Y_)for(var e of Y_(n))MI.call(n,e)&&Z_(t,e,n[e]);return t},ee=(t,n)=>EI(t,DI(n));var $t=null,Uc=!1,zf=1,SI=null,tt=Symbol("SIGNAL");function oe(t){let n=$t;return $t=t,n}function zc(){return $t}var Dr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Ir(t){if(Uc)throw new Error("");if($t===null)return;$t.consumerOnSignalRead(t);let n=$t.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=$t.recomputing;if(i&&(e=n!==void 0?n.nextProducer:$t.producers,e!==void 0&&e.producer===t)){$t.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===$t&&(!i||AI(r,$t)))return;let o=da($t),a={producer:t,consumer:$t,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};$t.producersTail=a,n!==void 0?n.nextProducer=a:$t.producers=a,o&&J_(t,a)}function Q_(){zf++}function po(t){if(!(da(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===zf)){if(!t.producerMustRecompute(t)&&!ca(t)){la(t);return}t.producerRecomputeValue(t),la(t)}}function $f(t){if(t.consumers===void 0)return;let n=Uc;Uc=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||TI(i)}}finally{Uc=n}}function qf(){return $t?.consumerAllowSignalWrites!==!1}function TI(t){t.dirty=!0,$f(t),t.consumerMarkedDirty?.(t)}function la(t){t.dirty=!1,t.lastCleanEpoch=zf}function Hi(t){return t&&K_(t),oe(t)}function K_(t){t.producersTail=void 0,t.recomputing=!0}function Mr(t,n){oe(n),t&&X_(t)}function X_(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(da(t))do e=Gf(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function ca(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(po(e),i!==e.version))return!0}return!1}function Sr(t){if(da(t)){let n=t.producers;for(;n!==void 0;)n=Gf(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function J_(t,n){let e=t.consumersTail,i=da(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)J_(r.producer,r)}function Gf(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!da(n)){let o=n.producers;for(;o!==void 0;)o=Gf(o)}return e}function da(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Fs(t){SI?.(t)}function AI(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function Ps(t,n){return Object.is(t,n)}function Ls(t,n){let e=Object.create(kI);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(po(e),Ir(e),e.value===Ei)throw e.error;return e.value};return i[tt]=e,Fs(e),i}var mo=Symbol("UNSET"),fo=Symbol("COMPUTING"),Ei=Symbol("ERRORED"),kI=ee(C({},Dr),{value:mo,dirty:!0,error:null,equal:Ps,kind:"computed",producerMustRecompute(t){return t.value===mo||t.value===fo},producerRecomputeValue(t){if(t.value===fo)throw new Error("");let n=t.value;t.value=fo;let e=Hi(t),i,r=!1;try{i=t.computation(),oe(null),r=n!==mo&&n!==Ei&&i!==Ei&&t.equal(n,i)}catch(o){i=Ei,t.error=o}finally{Mr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function OI(){throw new Error}var eb=OI;function tb(t){eb(t)}function Wf(t){eb=t}var RI=null;function Yf(t,n){let e=Object.create(js);e.value=t,n!==void 0&&(e.equal=n);let i=()=>nb(e);return i[tt]=e,Fs(e),[i,a=>ho(e,a),a=>$c(e,a)]}function nb(t){return Ir(t),t.value}function ho(t,n){qf()||tb(t),t.equal(t.value,n)||(t.value=n,NI(t))}function $c(t,n){qf()||tb(t),ho(t,n(t.value))}var js=ee(C({},Dr),{equal:Ps,value:void 0,kind:"signal"});function NI(t){t.version++,Q_(),$f(t),RI?.(t)}var Zf=ee(C({},Dr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Qf(t){if(t.dirty=!1,t.version>0&&!ca(t))return;t.version++;let n=Hi(t);try{t.cleanup(),t.fn()}finally{Mr(t,n)}}function me(t){return typeof t=="function"}function ua(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var qc=ua(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function go(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var fe=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(me(i))try{i()}catch(o){n=o instanceof qc?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{ib(o)}catch(a){n=n??[],a instanceof qc?n=[...n,...a.errors]:n.push(a)}}if(n)throw new qc(n)}}add(n){var e;if(n&&n!==this)if(this.closed)ib(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&go(e,n)}remove(n){let{_finalizers:e}=this;e&&go(e,n),n instanceof t&&n._removeParent(this)}};fe.EMPTY=(()=>{let t=new fe;return t.closed=!0,t})();var Kf=fe.EMPTY;function Gc(t){return t instanceof fe||t&&"closed"in t&&me(t.remove)&&me(t.add)&&me(t.unsubscribe)}function ib(t){me(t)?t():t.unsubscribe()}var Kn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ma={setTimeout(t,n,...e){let{delegate:i}=ma;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=ma;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Wc(t){ma.setTimeout(()=>{let{onUnhandledError:n}=Kn;if(n)n(t);else throw t})}function vo(){}var rb=Xf("C",void 0,void 0);function ob(t){return Xf("E",void 0,t)}function ab(t){return Xf("N",t,void 0)}function Xf(t,n,e){return{kind:t,value:n,error:e}}var _o=null;function fa(t){if(Kn.useDeprecatedSynchronousErrorHandling){let n=!_o;if(n&&(_o={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=_o;if(_o=null,e)throw i}}else t()}function sb(t){Kn.useDeprecatedSynchronousErrorHandling&&_o&&(_o.errorThrown=!0,_o.error=t)}var bo=class extends fe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Gc(n)&&n.add(this)):this.destination=LI}static create(n,e,i){return new Ui(n,e,i)}next(n){this.isStopped?ep(ab(n),this):this._next(n)}error(n){this.isStopped?ep(ob(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?ep(rb,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},FI=Function.prototype.bind;function Jf(t,n){return FI.call(t,n)}var tp=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){Yc(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){Yc(i)}else Yc(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){Yc(e)}}},Ui=class extends bo{constructor(n,e,i){super();let r;if(me(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&Kn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Jf(n.next,o),error:n.error&&Jf(n.error,o),complete:n.complete&&Jf(n.complete,o)}):r=n}this.destination=new tp(r)}};function Yc(t){Kn.useDeprecatedSynchronousErrorHandling?sb(t):Wc(t)}function PI(t){throw t}function ep(t,n){let{onStoppedNotification:e}=Kn;e&&ma.setTimeout(()=>e(t,n))}var LI={closed:!0,next:vo,error:PI,complete:vo};var pa=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Sn(t){return t}function np(...t){return ip(t)}function ip(t){return t.length===0?Sn:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ie=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=VI(e)?e:new Ui(e,i,r);return fa(()=>{let{operator:a,source:s}=this;o.add(a?a.call(o,s):s?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=lb(i),new i((r,o)=>{let a=new Ui({next:s=>{try{e(s)}catch(l){o(l),a.unsubscribe()}},error:o,complete:r});this.subscribe(a)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[pa](){return this}pipe(...e){return ip(e)(this)}toPromise(e){return e=lb(e),new e((i,r)=>{let o;this.subscribe(a=>o=a,a=>r(a),()=>i(o))})}}return t.create=n=>new t(n),t})();function lb(t){var n;return(n=t??Kn.Promise)!==null&&n!==void 0?n:Promise}function jI(t){return t&&me(t.next)&&me(t.error)&&me(t.complete)}function VI(t){return t&&t instanceof bo||jI(t)&&Gc(t)}function BI(t){return me(t?.lift)}function de(t){return n=>{if(BI(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function pe(t,n,e,i,r){return new rp(t,n,e,i,r)}var rp=class extends bo{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=r?function(s){try{r(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var cb=ua(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var E=(()=>{class t extends ie{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Zc(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new cb}next(e){fa(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){fa(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){fa(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?Kf:(this.currentObservers=null,o.push(e),new fe(()=>{this.currentObservers=null,go(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new ie;return e.source=this,e}}return t.create=(n,e)=>new Zc(n,e),t})(),Zc=class extends E{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:Kf}};var vt=class extends E{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Vs={now(){return(Vs.delegate||Date).now()},delegate:void 0};var Tr=class extends E{constructor(n=1/0,e=1/0,i=Vs){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let l=1;l<i.length&&i[l]<=a;l+=2)s=l;s&&i.splice(0,s+1)}}};var Qc=class extends fe{constructor(n,e){super()}schedule(n,e=0){return this}};var Bs={setInterval(t,n,...e){let{delegate:i}=Bs;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Bs;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var Kc=class extends Qc{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Bs.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Bs.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,go(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var ha=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};ha.now=Vs.now;var Xc=class extends ha{constructor(n,e=ha.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var yo=new Xc(Kc),db=yo;var Ye=new ie(t=>t.complete());function Jc(t){return t&&me(t.schedule)}function op(t){return t[t.length-1]}function ed(t){return me(op(t))?t.pop():void 0}function Di(t){return Jc(op(t))?t.pop():void 0}function ub(t,n){return typeof op(t)=="number"?t.pop():n}function fb(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(f){try{c(i.next(f))}catch(g){a(g)}}function l(f){try{c(i.throw(f))}catch(g){a(g)}}function c(f){f.done?o(f.value):r(f.value).then(s,l)}c((i=i.apply(t,n||[])).next())})}function mb(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Co(t){return this instanceof Co?(this.v=t,this):new Co(t)}function pb(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(b){return function(w){return Promise.resolve(w).then(b,g)}}function s(b,w){i[b]&&(r[b]=function(j){return new Promise(function(U,Y){o.push([b,j,U,Y])>1||l(b,j)})},w&&(r[b]=w(r[b])))}function l(b,w){try{c(i[b](w))}catch(j){v(o[0][3],j)}}function c(b){b.value instanceof Co?Promise.resolve(b.value.v).then(f,g):v(o[0][2],b)}function f(b){l("next",b)}function g(b){l("throw",b)}function v(b,w){b(w),o.shift(),o.length&&l(o[0][0],o[0][1])}}function hb(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof mb=="function"?mb(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),r(s,l,a.done,a.value)})}}function r(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var td=t=>t&&typeof t.length=="number"&&typeof t!="function";function nd(t){return me(t?.then)}function id(t){return me(t[pa])}function rd(t){return Symbol.asyncIterator&&me(t?.[Symbol.asyncIterator])}function od(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function HI(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var ad=HI();function sd(t){return me(t?.[ad])}function ld(t){return pb(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Co(e.read());if(r)return yield Co(void 0);yield yield Co(i)}}finally{e.releaseLock()}})}function cd(t){return me(t?.getReader)}function Le(t){if(t instanceof ie)return t;if(t!=null){if(id(t))return UI(t);if(td(t))return zI(t);if(nd(t))return $I(t);if(rd(t))return gb(t);if(sd(t))return qI(t);if(cd(t))return GI(t)}throw od(t)}function UI(t){return new ie(n=>{let e=t[pa]();if(me(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function zI(t){return new ie(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function $I(t){return new ie(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Wc)})}function qI(t){return new ie(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function gb(t){return new ie(n=>{WI(t,n).catch(e=>n.error(e))})}function GI(t){return gb(ld(t))}function WI(t,n){var e,i,r,o;return fb(this,void 0,void 0,function*(){try{for(e=hb(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function pn(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function dd(t,n=0){return de((e,i)=>{e.subscribe(pe(i,r=>pn(i,t,()=>i.next(r),n),()=>pn(i,t,()=>i.complete(),n),r=>pn(i,t,()=>i.error(r),n)))})}function ud(t,n=0){return de((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function vb(t,n){return Le(t).pipe(ud(n),dd(n))}function _b(t,n){return Le(t).pipe(ud(n),dd(n))}function bb(t,n){return new ie(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function yb(t,n){return new ie(e=>{let i;return pn(e,n,()=>{i=t[ad](),pn(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>me(i?.return)&&i.return()})}function md(t,n){if(!t)throw new Error("Iterable cannot be null");return new ie(e=>{pn(e,n,()=>{let i=t[Symbol.asyncIterator]();pn(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function Cb(t,n){return md(ld(t),n)}function xb(t,n){if(t!=null){if(id(t))return vb(t,n);if(td(t))return bb(t,n);if(nd(t))return _b(t,n);if(rd(t))return md(t,n);if(sd(t))return yb(t,n);if(cd(t))return Cb(t,n)}throw od(t)}function ze(t,n){return n?xb(t,n):Le(t)}function Z(...t){let n=Di(t);return ze(t,n)}function Hs(t,n){let e=me(t)?t:()=>t,i=r=>r.error(e());return new ie(n?r=>n.schedule(i,0,r):i)}function Us(t){return!!t&&(t instanceof ie||me(t.lift)&&me(t.subscribe))}var xo=ua(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function wb(t){return t instanceof Date&&!isNaN(t)}function V(t,n){return de((e,i)=>{let r=0;e.subscribe(pe(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:YI}=Array;function ZI(t,n){return YI(n)?t(...n):t(n)}function fd(t){return V(n=>ZI(t,n))}var{isArray:QI}=Array,{getPrototypeOf:KI,prototype:XI,keys:JI}=Object;function pd(t){if(t.length===1){let n=t[0];if(QI(n))return{args:n,keys:null};if(eM(n)){let e=JI(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function eM(t){return t&&typeof t=="object"&&KI(t)===XI}function hd(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function zs(...t){let n=Di(t),e=ed(t),{args:i,keys:r}=pd(t);if(i.length===0)return ze([],n);let o=new ie(tM(i,n,r?a=>hd(r,a):Sn));return e?o.pipe(fd(e)):o}function tM(t,n,e=Sn){return i=>{Eb(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let l=0;l<r;l++)Eb(n,()=>{let c=ze(t[l],n),f=!1;c.subscribe(pe(i,g=>{o[l]=g,f||(f=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function Eb(t,n,e){t?pn(e,t,n):n()}function Db(t,n,e,i,r,o,a,s){let l=[],c=0,f=0,g=!1,v=()=>{g&&!l.length&&!c&&n.complete()},b=j=>c<i?w(j):l.push(j),w=j=>{o&&n.next(j),c++;let U=!1;Le(e(j,f++)).subscribe(pe(n,Y=>{r?.(Y),o?b(Y):n.next(Y)},()=>{U=!0},void 0,()=>{if(U)try{for(c--;l.length&&c<i;){let Y=l.shift();a?pn(n,a,()=>w(Y)):w(Y)}v()}catch(Y){n.error(Y)}}))};return t.subscribe(pe(n,b,()=>{g=!0,v()})),()=>{s?.()}}function Mt(t,n,e=1/0){return me(n)?Mt((i,r)=>V((o,a)=>n(i,o,r,a))(Le(t(i,r))),e):(typeof n=="number"&&(e=n),de((i,r)=>Db(i,r,t,e)))}function Ar(t=1/0){return Mt(Sn,t)}function Ib(){return Ar(1)}function Ii(...t){return Ib()(ze(t,Di(t)))}function Tn(t){return new ie(n=>{Le(t()).subscribe(n)})}function wo(...t){let n=ed(t),{args:e,keys:i}=pd(t),r=new ie(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let f=0;f<a;f++){let g=!1;Le(e[f]).subscribe(pe(o,v=>{g||(g=!0,c--),s[f]=v},()=>l--,void 0,()=>{(!l||!g)&&(c||o.next(i?hd(i,s):s),o.complete())}))}});return n?r.pipe(fd(n)):r}function gd(t=0,n,e=db){let i=-1;return n!=null&&(Jc(n)?e=n:i=n),new ie(r=>{let o=wb(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function _t(...t){let n=Di(t),e=ub(t,1/0),i=t;return i.length?i.length===1?Le(i[0]):Ar(e)(ze(i,n)):Ye}function ae(t,n){return de((e,i)=>{let r=0;e.subscribe(pe(i,o=>t.call(n,o,r++)&&i.next(o)))})}function Mb(t){return de((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(pe(e,c=>{i=!0,r=c,o||Le(t(c)).subscribe(o=pe(e,s,l))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function vd(t,n=yo){return Mb(()=>gd(t,n))}function kr(t){return de((n,e)=>{let i=null,r=!1,o;i=n.subscribe(pe(e,void 0,void 0,a=>{o=Le(t(a,kr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Eo(t,n){return me(n)?Mt(t,n,1):Mt(t,1)}function Xn(t,n=yo){return de((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=a+t,f=n.now();if(f<c){r=this.schedule(void 0,c-f),i.add(r);return}s()}e.subscribe(pe(i,c=>{o=c,a=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function Sb(t){return de((n,e)=>{let i=!1;n.subscribe(pe(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Oe(t){return t<=0?()=>Ye:de((n,e)=>{let i=0;n.subscribe(pe(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Tb(){return de((t,n)=>{t.subscribe(pe(n,vo))})}function ga(t){return V(()=>t)}function ap(t,n){return n?e=>Ii(n.pipe(Oe(1),Tb()),e.pipe(ap(t))):Mt((e,i)=>Le(t(e,i)).pipe(Oe(1),ga(e)))}function sp(t,n=yo){let e=gd(t,n);return ap(()=>e)}function va(t,n=Sn){return t=t??nM,de((e,i)=>{let r,o=!0;e.subscribe(pe(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function nM(t,n){return t===n}function Ab(t=iM){return de((n,e)=>{let i=!1;n.subscribe(pe(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function iM(){return new xo}function Do(t){return de((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function zi(t,n){let e=arguments.length>=2;return i=>i.pipe(t?ae((r,o)=>t(r,o,i)):Sn,Oe(1),e?Sb(n):Ab(()=>new xo))}function _d(t){return t<=0?()=>Ye:de((n,e)=>{let i=[];n.subscribe(pe(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function bd(){return de((t,n)=>{let e,i=!1;t.subscribe(pe(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function $s(t={}){let{connector:n=()=>new E,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,l,c=0,f=!1,g=!1,v=()=>{s?.unsubscribe(),s=void 0},b=()=>{v(),a=l=void 0,f=g=!1},w=()=>{let j=a;b(),j?.unsubscribe()};return de((j,U)=>{c++,!g&&!f&&v();let Y=l=l??n();U.add(()=>{c--,c===0&&!g&&!f&&(s=lp(w,r))}),Y.subscribe(U),!a&&c>0&&(a=new Ui({next:Ue=>Y.next(Ue),error:Ue=>{g=!0,v(),s=lp(b,e,Ue),Y.error(Ue)},complete:()=>{f=!0,v(),s=lp(b,i),Y.complete()}}),Le(j).subscribe(a))})(o)}}function lp(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new Ui({next:()=>{i.unsubscribe(),t()}});return Le(n(...e)).subscribe(i)}function yd(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,$s({connector:()=>new Tr(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function qs(t){return ae((n,e)=>t<=e)}function Me(...t){let n=Di(t);return de((e,i)=>{(n?Ii(t,e,n):Ii(t,e)).subscribe(i)})}function Ze(t,n){return de((e,i)=>{let r=null,o=0,a=!1,s=()=>a&&!r&&i.complete();e.subscribe(pe(i,l=>{r?.unsubscribe();let c=0,f=o++;Le(t(l,f)).subscribe(r=pe(i,g=>i.next(n?n(l,g,f,c++):g),()=>{r=null,s()}))},()=>{a=!0,s()}))})}function ye(t){return de((n,e)=>{Le(t).subscribe(pe(e,()=>e.complete(),vo)),!e.closed&&n.subscribe(e)})}function cp(t,n=!1){return de((e,i)=>{let r=0;e.subscribe(pe(i,o=>{let a=t(o,r++);(a||n)&&i.next(o),!a&&i.complete()}))})}function ot(t,n,e){let i=me(t)||n||e?{next:t,error:n,complete:e}:t;return i?de((r,o)=>{var a;(a=i.subscribe)===null||a===void 0||a.call(i);let s=!0;r.subscribe(pe(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;s=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;s=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;s&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Sn}var dp;function Cd(){return dp}function Mi(t){let n=dp;return dp=t,n}var kb=Symbol("NotFound");function _a(t){return t===kb||t?.name==="\u0275NotFound"}function up(t,n,e){let i=Object.create(rM);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(po(i),Ir(i),i.value===Ei)throw i.error;return i.value};return o[tt]=i,Fs(i),o}function Ob(t,n){po(t),ho(t,n),la(t)}function Rb(t,n){if(po(t),t.value===Ei)throw t.error;$c(t,n),la(t)}var rM=ee(C({},Dr),{value:mo,dirty:!0,error:null,equal:Ps,kind:"linkedSignal",producerMustRecompute(t){return t.value===mo||t.value===fo},producerRecomputeValue(t){if(t.value===fo)throw new Error("");let n=t.value;t.value=fo;let e=Hi(t),i,r=!1;try{let o=t.source(),a=n!==mo&&n!==Ei,s=a?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,s),t.sourceValue=o,oe(null),r=a&&i!==Ei&&t.equal(n,i)}catch(o){i=Ei,t.error=o}finally{Mr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Nb(t){let n=oe(null);try{return t()}finally{oe(n)}}var Td="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",k=class extends Error{code;constructor(n,e){super(qi(n,e)),this.code=n}};function oM(t){return`NG0${Math.abs(t)}`}function qi(t,n){return`${oM(t)}${n?": "+n:""}`}var Pr=globalThis;function Re(t){for(let n in t)if(t[n]===Re)return n;throw Error("")}function Vb(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function Js(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Js).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Ad(t,n){return t?n?`${t} ${n}`:t:n||""}var aM=Re({__forward_ref__:Re});function hn(t){return t.__forward_ref__=hn,t}function bt(t){return Ep(t)?t():t}function Ep(t){return typeof t=="function"&&t.hasOwnProperty(aM)&&t.__forward_ref__===hn}function x(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function L(t){return{providers:t.providers||[],imports:t.imports||[]}}function el(t){return sM(t,kd)}function Dp(t){return el(t)!==null}function sM(t,n){return t.hasOwnProperty(n)&&t[n]||null}function lM(t){let n=t?.[kd]??null;return n||null}function fp(t){return t&&t.hasOwnProperty(Ed)?t[Ed]:null}var kd=Re({\u0275prov:Re}),Ed=Re({\u0275inj:Re}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=x({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Ip(t){return t&&!!t.\u0275providers}var tl=Re({\u0275cmp:Re}),nl=Re({\u0275dir:Re}),Mp=Re({\u0275pipe:Re}),Sp=Re({\u0275mod:Re}),Ws=Re({\u0275fac:Re}),Ao=Re({__NG_ELEMENT_ID__:Re}),Fb=Re({__NG_ENV_ID__:Re});function Tp(t){return Rd(t,"@NgModule"),t[Sp]||null}function Gi(t){return Rd(t,"@Component"),t[tl]||null}function Od(t){return Rd(t,"@Directive"),t[nl]||null}function Bb(t){return Rd(t,"@Pipe"),t[Mp]||null}function Rd(t,n){if(t==null)throw new k(-919,!1)}function Wi(t){return typeof t=="string"?t:t==null?"":String(t)}var Hb=Re({ngErrorCode:Re}),cM=Re({ngErrorMessage:Re}),dM=Re({ngTokenPath:Re});function Ap(t,n){return Ub("",-200,n)}function Nd(t,n){throw new k(-201,!1)}function Ub(t,n,e){let i=new k(n,t);return i[Hb]=n,i[cM]=t,e&&(i[dM]=e),i}function uM(t){return t[Hb]}var pp;function zb(){return pp}function rn(t){let n=pp;return pp=t,n}function kp(t,n,e){let i=el(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Nd(t,"")}var mM={},Io=mM,fM="__NG_DI_FLAG__",hp=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Mo(e)||0;try{return this.injector.get(n,i&8?null:Io,i)}catch(r){if(_a(r))return r;throw r}}};function pM(t,n=0){let e=Cd();if(e===void 0)throw new k(-203,!1);if(e===null)return kp(t,void 0,n);{let i=hM(n),r=e.retrieve(t,i);if(_a(r)){if(i.optional)return null;throw r}return r}}function Q(t,n=0){return(zb()||pM)(bt(t),n)}function d(t,n){return Q(t,Mo(n))}function Mo(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function hM(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function gp(t){let n=[];for(let e=0;e<t.length;e++){let i=bt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new k(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],l=gM(s);typeof l=="number"?l===-1?r=s.token:o|=l:r=s}n.push(Q(r,o))}else n.push(Q(i))}return n}function gM(t){return t[fM]}function Rr(t,n){let e=t.hasOwnProperty(Ws);return e?t[Ws]:null}function $b(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function qb(t){return t.flat(Number.POSITIVE_INFINITY)}function Fd(t,n){t.forEach(e=>Array.isArray(e)?Fd(e,n):n(e))}function Op(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function il(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function Gb(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function Wb(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Pd(t,n,e){let i=ya(t,n);return i>=0?t[i|1]=e:(i=~i,Wb(t,i,n,e)),i}function Ld(t,n){let e=ya(t,n);if(e>=0)return t[e|1]}function ya(t,n){return vM(t,n,1)}function vM(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var Jn={},qt=[],Lr=new y(""),Rp=new y("",-1),Np=new y(""),Ys=class{get(n,e=Io){if(e===Io){let r=Ub("",-201);throw r.name="\u0275NotFound",r}return e}};function Yi(t){return{\u0275providers:t}}function Yb(t){return Yi([{provide:Lr,multi:!0,useValue:t}])}function Zb(...t){return{\u0275providers:Fp(!0,t),\u0275fromNgModule:!0}}function Fp(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return Fd(n,a=>{let s=a;Dd(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&Qb(r,o),e}function Qb(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Pp(r,o=>{n(o,i)})}}function Dd(t,n,e,i){if(t=bt(t),!t)return!1;let r=null,o=fp(t),a=!o&&Gi(t);if(!o&&!a){let l=t.ngModule;if(o=fp(l),o)r=l;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)Dd(c,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let c;Fd(o.imports,f=>{Dd(f,n,e,i)&&(c||=[],c.push(f))}),c!==void 0&&Qb(c,n)}if(!s){let c=Rr(r)||(()=>new r);n({provide:r,useFactory:c,deps:qt},r),n({provide:Np,useValue:r,multi:!0},r),n({provide:Lr,useValue:()=>Q(r),multi:!0},r)}let l=o.providers;if(l!=null&&!s){let c=t;Pp(l,f=>{n(f,c)})}}else return!1;return r!==t&&t.providers!==void 0}function Pp(t,n){for(let e of t)Ip(e)&&(e=e.\u0275providers),Array.isArray(e)?Pp(e,n):n(e)}var _M=Re({provide:String,useValue:Re});function Kb(t){return t!==null&&typeof t=="object"&&_M in t}function bM(t){return!!(t&&t.useExisting)}function yM(t){return!!(t&&t.useFactory)}function So(t){return typeof t=="function"}function Xb(t){return!!t.useClass}var rl=new y(""),xd={},Pb={},mp;function Ca(){return mp===void 0&&(mp=new Ys),mp}var Ae=class{},To=class extends Ae{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,_p(n,a=>this.processProvider(a)),this.records.set(Rp,ba(void 0,this)),r.has("environment")&&this.records.set(Ae,ba(void 0,this));let o=this.records.get(rl);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Np,qt,{self:!0}))}retrieve(n,e){let i=Mo(e)||0;try{return this.get(n,Io,i)}catch(r){if(_a(r))return r;throw r}}destroy(){Gs(this),this._destroyed=!0;let n=oe(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),oe(n)}}onDestroy(n){return Gs(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Gs(this);let e=Mi(this),i=rn(void 0),r;try{return n()}finally{Mi(e),rn(i)}}get(n,e=Io,i){if(Gs(this),n.hasOwnProperty(Fb))return n[Fb](this);let r=Mo(i),o,a=Mi(this),s=rn(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let f=DM(n)&&el(n);f&&this.injectableDefInScope(f)?c=ba(vp(n),xd):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?Ca():this.parent;return e=r&8&&e===Io?null:e,l.get(n,e)}catch(l){let c=uM(l);throw c===-200||c===-201?new k(c,null):l}finally{rn(s),Mi(a)}}resolveInjectorInitializers(){let n=oe(null),e=Mi(this),i=rn(void 0),r;try{let o=this.get(Lr,qt,{self:!0});for(let a of o)a()}finally{Mi(e),rn(i),oe(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=bt(n);let e=So(n)?n:bt(n&&n.provide),i=xM(n);if(!So(n)&&n.multi===!0){let r=this.records.get(e);r||(r=ba(void 0,xd,!0),r.factory=()=>gp(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=oe(null);try{if(e.value===Pb)throw Ap("");return e.value===xd&&(e.value=Pb,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&EM(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{oe(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=bt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function vp(t){let n=el(t),e=n!==null?n.factory:Rr(t);if(e!==null)return e;if(t instanceof y)throw new k(-204,!1);if(t instanceof Function)return CM(t);throw new k(-204,!1)}function CM(t){if(t.length>0)throw new k(-204,!1);let e=lM(t);return e!==null?()=>e.factory(t):()=>new t}function xM(t){if(Kb(t))return ba(void 0,t.useValue);{let n=Lp(t);return ba(n,xd)}}function Lp(t,n,e){let i;if(So(t)){let r=bt(t);return Rr(r)||vp(r)}else if(Kb(t))i=()=>bt(t.useValue);else if(yM(t))i=()=>t.useFactory(...gp(t.deps||[]));else if(bM(t))i=(r,o)=>Q(bt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=bt(t&&(t.useClass||t.provide));if(wM(t))i=()=>new r(...gp(t.deps));else return Rr(r)||vp(r)}return i}function Gs(t){if(t.destroyed)throw new k(-205,!1)}function ba(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function wM(t){return!!t.deps}function EM(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function DM(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function _p(t,n){for(let e of t)Array.isArray(e)?_p(e,n):e&&Ip(e)?_p(e.\u0275providers,n):n(e)}function Tt(t,n){let e;t instanceof To?(Gs(t),e=t):e=new hp(t);let i,r=Mi(e),o=rn(void 0);try{return n()}finally{Mi(r),rn(o)}}function Jb(){return zb()!==void 0||Cd()!=null}var ei=0,X=1,se=2,St=3,jn=4,an=5,xa=6,wa=7,zt=8,jr=9,ti=10,$e=11,Ea=12,jp=13,ko=14,gn=15,Vr=16,Oo=17,Ti=18,Br=19,Vp=20,$i=21,jd=22,Nr=23,An=24,Ro=25,Da=26,Qe=27,ey=1;var Hr=7,ol=8,No=9,Gt=10;function Zi(t){return Array.isArray(t)&&typeof t[ey]=="object"}function ni(t){return Array.isArray(t)&&t[ey]===!0}function Bp(t){return(t.flags&4)!==0}function Qi(t){return t.componentOffset>-1}function Ia(t){return(t.flags&1)===1}function ii(t){return!!t.template}function Ma(t){return(t[se]&512)!==0}function Fo(t){return(t[se]&256)===256}var Te=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(Te||{}),wd,Zs="svg",Hp="math";function ty(){return wd||(wd={},Or(Te.HTML,void 0,[["iframe",["srcdoc"]],["*",["innerHTML","outerHTML"]]]),Or(Te.STYLE,void 0,[["*",["style"]]]),Or(Te.URL,void 0,[["*",["formAction"]],["area",["href"]],["a",["href","xlink:href"]],["form",["action"]],["img",["src"]],["video",["src"]]]),Or(Te.URL,Hp,[["*",["href","xlink:href"]],["annotation",["href","xlink:href"]],["annotation-xml",["href","xlink:href"]],["maction",["href","xlink:href"]],["malignmark",["href","xlink:href"]],["math",["href","xlink:href"]],["mroot",["href","xlink:href"]],["msqrt",["href","xlink:href"]],["merror",["href","xlink:href"]],["mfrac",["href","xlink:href"]],["mglyph",["href","xlink:href"]],["msub",["href","xlink:href"]],["msup",["href","xlink:href"]],["msubsup",["href","xlink:href"]],["mmultiscripts",["href","xlink:href"]],["mprescripts",["href","xlink:href"]],["mi",["href","xlink:href"]],["mn",["href","xlink:href"]],["mo",["href","xlink:href"]],["mpadded",["href","xlink:href"]],["mphantom",["href","xlink:href"]],["mrow",["href","xlink:href"]],["ms",["href","xlink:href"]],["mspace",["href","xlink:href"]],["mstyle",["href","xlink:href"]],["mtable",["href","xlink:href"]],["mtd",["href","xlink:href"]],["mtr",["href","xlink:href"]],["mtext",["href","xlink:href"]],["mover",["href","xlink:href"]],["munder",["href","xlink:href"]],["munderover",["href","xlink:href"]],["semantics",["href","xlink:href"]],["none",["href","xlink:href"]]]),Or(Te.RESOURCE_URL,void 0,[["base",["href"]],["embed",["src"]],["frame",["src"]],["iframe",["src"]],["link",["href"]],["object",["codebase","data"]]]),Or(Te.URL,Zs,[["a",["href","xlink:href"]]]),Or(Te.ATTRIBUTE_NO_BINDING,Zs,[["animate",["attributeName","values","to","from"]],["set",["to","attributeName"]],["animateMotion",["attributeName"]],["animateTransform",["attributeName"]]]),Or(Te.ATTRIBUTE_NO_BINDING,void 0,[["unknown",["attributeName","values","to","from","sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority"]],["iframe",["sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority"]]])),wd}function Or(t,n,e){for(let[i,r]of e){let o=n&&i!=="unknown"?`:${n}:${i}`:i;o=o.toLowerCase();for(let a of r)wd[`${o}|${a.toLowerCase()}`]=t}}function ny(t,n,e){let i=ty(),r=t.toLowerCase(),o=n.toLowerCase(),a=e&&r!=="*"&&r!=="unknown"?i[`:${e}:${r}|${o}`]:void 0,s=e?i[`:${e}:*|${o}`]:void 0;return a??s??i[`${r}|${o}`]??i[`*|${o}`]??Te.NONE}function Vn(t){for(;Array.isArray(t);)t=t[ei];return t}function Up(t,n){return Vn(n[t])}function Bn(t,n){return Vn(n[t.index])}function Vd(t,n){return t.data[n]}function zp(t,n){return t[n]}function $p(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function Hn(t,n){let e=n[t];return Zi(e)?e:e[ei]}function iy(t){return(t[se]&4)===4}function Bd(t){return(t[se]&128)===128}function ry(t){return ni(t[St])}function ri(t,n){return n==null?null:t[n]}function qp(t){t[Oo]=0}function Gp(t){t[se]&1024||(t[se]|=1024,Bd(t)&&Po(t))}function oy(t,n){for(;t>0;)n=n[ko],t--;return n}function al(t){return!!(t[se]&9216||t[An]?.dirty)}function Hd(t){t[ti].changeDetectionScheduler?.notify(8),t[se]&64&&(t[se]|=1024),al(t)&&Po(t)}function Po(t){t[ti].changeDetectionScheduler?.notify(0);let n=Fr(t);for(;n!==null&&!(n[se]&8192||(n[se]|=8192,!Bd(n)));)n=Fr(n)}function Wp(t,n){if(Fo(t))throw new k(911,!1);t[$i]===null&&(t[$i]=[]),t[$i].push(n)}function ay(t,n){if(t[$i]===null)return;let e=t[$i].indexOf(n);e!==-1&&t[$i].splice(e,1)}function Fr(t){let n=t[St];return ni(n)?n[St]:n}function Yp(t){return t[wa]??=[]}function Zp(t){return t.cleanup??=[]}function sy(t,n,e,i){let r=Yp(n);r.push(e),t.firstCreatePass&&Zp(t).push(i,r.length-1)}var he={lFrame:_y(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var bp=!1;function ly(){return he.lFrame.elementDepthCount}function cy(){he.lFrame.elementDepthCount++}function Qp(){he.lFrame.elementDepthCount--}function Ud(){return he.bindingsEnabled}function Kp(){return he.skipHydrationRootTNode!==null}function Xp(t){return he.skipHydrationRootTNode===t}function Jp(){he.skipHydrationRootTNode=null}function te(){return he.lFrame.lView}function Ke(){return he.lFrame.tView}function we(t){return he.lFrame.contextLView=t,t[zt]}function Ee(t){return he.lFrame.contextLView=null,t}function At(){let t=eh();for(;t!==null&&t.type===64;)t=t.parent;return t}function eh(){return he.lFrame.currentTNode}function dy(){let t=he.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Sa(t,n){let e=he.lFrame;e.currentTNode=t,e.isParent=n}function th(){return he.lFrame.isParent}function nh(){he.lFrame.isParent=!1}function uy(){return he.lFrame.contextLView}function ih(){return bp}function Qs(t){let n=bp;return bp=t,n}function sl(){let t=he.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function rh(){return he.lFrame.bindingIndex}function my(t){return he.lFrame.bindingIndex=t}function Ta(){return he.lFrame.bindingIndex++}function ll(t){let n=he.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function fy(){return he.lFrame.inI18n}function py(t,n){let e=he.lFrame;e.bindingIndex=e.bindingRootIndex=t,zd(n)}function hy(){return he.lFrame.currentDirectiveIndex}function zd(t){he.lFrame.currentDirectiveIndex=t}function gy(t){let n=he.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function $d(){return he.lFrame.currentQueryIndex}function cl(t){he.lFrame.currentQueryIndex=t}function IM(t){let n=t[X];return n.type===2?n.declTNode:n.type===1?t[an]:null}function oh(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=IM(o),r===null||(o=o[ko],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=he.lFrame=vy();return i.currentTNode=n,i.lView=t,!0}function qd(t){let n=vy(),e=t[X];he.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function vy(){let t=he.lFrame,n=t===null?null:t.child;return n===null?_y(t):n}function _y(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function by(){let t=he.lFrame;return he.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var ah=by;function Gd(){let t=by();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function yy(t){return(he.lFrame.contextLView=oy(t,he.lFrame.contextLView))[zt]}function oi(){return he.lFrame.selectedIndex}function Ur(t){he.lFrame.selectedIndex=t}function dl(){let t=he.lFrame;return Vd(t.tView,t.selectedIndex)}function Ai(){he.lFrame.currentNamespace=Zs}function ul(){MM()}function MM(){he.lFrame.currentNamespace=null}function sh(){return he.lFrame.currentNamespace}var Cy=!0;function Wd(){return Cy}function ml(t){Cy=t}function yp(t,n=null,e=null,i){let r=lh(t,n,e,i);return r.resolveInjectorInitializers(),r}function lh(t,n=null,e=null,i,r=new Set){let o=[e||qt,Zb(t)],a;return new To(o,n||Ca(),a||null,r)}var W=class t{static THROW_IF_NOT_FOUND=Io;static NULL=new Ys;static create(n,e){if(Array.isArray(n))return yp({name:""},e,n,"");{let i=n.name??"";return yp({name:i},n.parent,n.providers,i)}}static \u0275prov=x({token:t,providedIn:"any",factory:()=>Q(Rp)});static __NG_ELEMENT_ID__=-1},G=new y(""),sn=(()=>{class t{static __NG_ELEMENT_ID__=SM;static __NG_ENV_ID__=e=>e}return t})(),Id=class extends sn{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Fo(this._lView)}onDestroy(n){let e=this._lView;return Wp(e,n),()=>ay(e,n)}};function SM(){return new Id(te())}var xy=!1,wy=new y(""),Ki=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new vt(!1);debugTaskTracker=d(wy,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ie(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=x({token:t,providedIn:"root",factory:()=>new t})}return t})(),Cp=class extends E{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Jb()&&(this.destroyRef=d(sn,{optional:!0})??void 0,this.pendingTasks=d(Ki,{optional:!0})??void 0)}emit(n){let e=oe(null);try{super.next(n)}finally{oe(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof fe&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},P=Cp;function Md(...t){}function ch(t){let n,e;function i(){t=Md;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function Ey(t){return queueMicrotask(()=>t()),()=>{t=Md}}var dh="isAngularZone",Ks=dh+"_ID",TM=0,F=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new P(!1);onMicrotaskEmpty=new P(!1);onStable=new P(!1);onError=new P(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=xy}=n;if(typeof Zone>"u")throw new k(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,OM(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(dh)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new k(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new k(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,AM,Md,Md);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},AM={};function uh(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function kM(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){ch(()=>{t.callbackScheduled=!1,xp(t),t.isCheckStableRunning=!0,uh(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),xp(t)}function OM(t){let n=()=>{kM(t)},e=TM++;t._inner=t._inner.fork({name:"angular",properties:{[dh]:!0,[Ks]:e,[Ks+e]:!0},onInvokeTask:(i,r,o,a,s,l)=>{if(RM(l))return i.invokeTask(o,a,s,l);try{return Lb(t),i.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),jb(t)}},onInvoke:(i,r,o,a,s,l,c)=>{try{return Lb(t),i.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!NM(l)&&n(),jb(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,xp(t),uh(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function xp(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function Lb(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function jb(t){t._nesting--,uh(t)}var Xs=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new P;onMicrotaskEmpty=new P;onStable=new P;onError=new P;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function RM(t){return Dy(t,"__ignore_ng_zone__")}function NM(t){return Dy(t,"__scheduler_tick__")}function Dy(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var on=class{_console=console;handleError(n){this._console.error("ERROR",n)}},vn=new y("",{factory:()=>{let t=d(F),n=d(Ae),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(on),e.handleError(i))})}}}),Iy={provide:Lr,useValue:()=>{let t=d(on,{optional:!0})},multi:!0},FM=new y("",{factory:()=>{let t=d(G).defaultView;if(!t)return;let n=d(vn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(sn).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function mh(){return Yi([Yb(()=>{d(FM)})])}function ne(t,n){let[e,i,r]=Yf(t,n?.equal),o=e,a=o[tt];return o.set=i,o.update=r,o.asReadonly=Yd.bind(o),o}function Yd(){let t=this[tt];if(t.readonlyFn===void 0){let n=()=>this();n[tt]=t,t.readonlyFn=n}return t.readonlyFn}var Aa=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=PM}return t})();function PM(){return new Aa(te(),At())}var Si=class{},fl=new y("",{factory:()=>!0});var fh=new y(""),pl=(()=>{class t{internalPendingTasks=d(Ki);scheduler=d(Si);errorHandler=d(vn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=x({token:t,providedIn:"root",factory:()=>new t})}return t})(),Zd=(()=>{class t{static \u0275prov=x({token:t,providedIn:"root",factory:()=>new wp})}return t})(),wp=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Sd=class{[tt];constructor(n){this[tt]=n}destroy(){this[tt].destroy()}};function Xi(t,n){let e=n?.injector??d(W),i=n?.manualCleanup!==!0?e.get(sn):null,r,o=e.get(Aa,null,{optional:!0}),a=e.get(Si);return o!==null?(r=VM(o.view,a,t),i instanceof Id&&i._lView===o.view&&(i=null)):r=BM(t,e.get(Zd),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Sd(r)}var My=ee(C({},Zf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=Qs(!1);try{Qf(this)}finally{Qs(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=oe(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],oe(t)}}}),LM=ee(C({},My),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Sr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),jM=ee(C({},My),{consumerMarkedDirty(){this.view[se]|=8192,Po(this.view),this.notifier.notify(13)},destroy(){if(Sr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Nr]?.delete(this)}});function VM(t,n,e){let i=Object.create(jM);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=Sy(i,e),t[Nr]??=new Set,t[Nr].add(i),i.consumerMarkedDirty(i),i}function BM(t,n,e){let i=Object.create(LM);return i.fn=Sy(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function Sy(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function wl(t){return{toString:t}.toString()}function WM(t){return typeof t=="function"}function c0(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var ou=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},je=(()=>{let t=()=>d0;return t.ngInherit=!0,t})();function d0(t){return t.type.prototype.ngOnChanges&&(t.setInput=ZM),YM}function YM(){let t=m0(this),n=t?.current;if(n){let e=t.previous;if(e===Jn)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function ZM(t,n,e,i,r){let o=this.declaredInputs[i],a=m0(t)||QM(t,{previous:Jn,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new ou(c&&c.currentValue,e,l===Jn),c0(t,n,r,e)}var u0="__ngSimpleChanges__";function m0(t){return t[u0]||null}function QM(t,n){return t[u0]=n}var Ty=[];var Ne=function(t,n=null,e){for(let i=0;i<Ty.length;i++){let r=Ty[i];r(t,n,e)}},Ie=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Ie||{});function KM(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=d0(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function f0(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:f}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),f!=null&&(t.destroyHooks??=[]).push(e,f)}}function eu(t,n,e){p0(t,n,3,e)}function tu(t,n,e,i){(t[se]&3)===e&&p0(t,n,e,i)}function ph(t,n){let e=t[se];(e&3)===n&&(e&=16383,e+=1,t[se]=e)}function p0(t,n,e,i){let r=i!==void 0?t[Oo]&65535:0,o=i??-1,a=n.length-1,s=0;for(let l=r;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],i!=null&&s>=i)break}else n[l]<0&&(t[Oo]+=65536),(s<o||o==-1)&&(XM(t,e,n,l),t[Oo]=(t[Oo]&4294901760)+l+2),l++}function Ay(t,n){Ne(Ie.LifecycleHookStart,t,n);let e=oe(null);try{n.call(t)}finally{oe(e),Ne(Ie.LifecycleHookEnd,t,n)}}function XM(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[se]>>14<t[Oo]>>16&&(t[se]&3)===n&&(t[se]+=16384,Ay(s,o)):Ay(s,o)}var Oa=-1,jo=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function JM(t){return(t.flags&8)!==0}function eS(t){return(t.flags&16)!==0}function tS(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];nS(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function h0(t){return t===3||t===4||t===6}function nS(t){return t.charCodeAt(0)===64}function Ra(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?ky(t,e,r,null,n[++i]):ky(t,e,r,null,null))}}return t}function ky(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function g0(t){return t!==Oa}function au(t){return t&32767}function iS(t){return t>>16}function su(t,n){let e=iS(t),i=n;for(;e>0;)i=i[ko],e--;return i}var wh=!0;function lu(t){let n=wh;return wh=t,n}var rS=256,v0=rS-1,_0=5,oS=0,ki={};function aS(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Ao)&&(i=e[Ao]),i==null&&(i=e[Ao]=oS++);let r=i&v0,o=1<<r;n.data[t+(r>>_0)]|=o}function cu(t,n){let e=b0(t,n);if(e!==-1)return e;let i=n[X];i.firstCreatePass&&(t.injectorIndex=n.length,hh(i.data,t),hh(n,null),hh(i.blueprint,null));let r=tg(t,n),o=t.injectorIndex;if(g0(r)){let a=au(r),s=su(r,n),l=s[X].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=r,o}function hh(t,n){t.push(0,0,0,0,0,0,0,0,n)}function b0(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function tg(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=E0(r),i===null)return Oa;if(e++,r=r[ko],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Oa}function Eh(t,n,e){aS(t,n,e)}function sS(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(h0(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function y0(t,n,e){if(e&8||t!==void 0)return t;Nd(n,"NodeInjector")}function C0(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[jr],o=rn(void 0);try{return r?r.get(n,i,e&8):kp(n,i,e&8)}finally{rn(o)}}return y0(i,n,e)}function x0(t,n,e,i=0,r){if(t!==null){if(n[se]&2048&&!(i&2)){let a=uS(t,n,e,i,ki);if(a!==ki)return a}let o=w0(t,n,e,i,ki);if(o!==ki)return o}return C0(n,e,i,r)}function w0(t,n,e,i,r){let o=cS(e);if(typeof o=="function"){if(!oh(n,t,i))return i&1?y0(r,e,i):C0(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))Nd(e);else return a}finally{ah()}}else if(typeof o=="number"){let a=null,s=b0(t,n),l=Oa,c=i&1?n[gn][an]:null;for((s===-1||i&4)&&(l=s===-1?tg(t,n):n[s+8],l===Oa||!Ry(i,!1)?s=-1:(a=n[X],s=au(l),n=su(l,n)));s!==-1;){let f=n[X];if(Oy(o,s,f.data)){let g=lS(s,n,e,a,i,c);if(g!==ki)return g}l=n[s+8],l!==Oa&&Ry(i,n[X].data[s+8]===c)&&Oy(o,s,n)?(a=f,s=au(l),n=su(l,n)):s=-1}}return r}function lS(t,n,e,i,r,o){let a=n[X],s=a.data[t+8],l=i==null?Qi(s)&&wh:i!=a&&(s.type&3)!==0,c=r&1&&o===s,f=nu(s,a,e,l,c);return f!==null?_l(n,a,f,s,r):ki}function nu(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,f=o>>20,g=i?s:s+f,v=r?s+f:c;for(let b=g;b<v;b++){let w=a[b];if(b<l&&e===w||b>=l&&w.type===e)return b}if(r){let b=a[l];if(b&&ii(b)&&b.type===e)return l}return null}function _l(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof jo){let s=o;if(s.resolving)throw Ap("");let l=lu(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],f,g=s.injectImpl?rn(s.injectImpl):null,v=oh(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&KM(e,a[e],n)}finally{g!==null&&rn(g),lu(l),s.resolving=!1,ah()}}return o}function cS(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Ao)?t[Ao]:void 0;return typeof n=="number"?n>=0?n&v0:dS:n}function Oy(t,n,e){let i=1<<t;return!!(e[n+(t>>_0)]&i)}function Ry(t,n){return!(t&2)&&!(t&1&&n)}var Lo=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return x0(this._tNode,this._lView,n,Mo(i),e)}};function dS(){return new Lo(At(),te())}function Ve(t){return wl(()=>{let n=t.prototype.constructor,e=n[Ws]||Dh(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Ws]||Dh(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Dh(t){return Ep(t)?()=>{let n=Dh(bt(t));return n&&n()}:Rr(t)}function uS(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[se]&2048&&!Ma(a);){let s=w0(o,a,e,i|2,ki);if(s!==ki)return s;let l=o.parent;if(!l){let c=a[Vp];if(c){let f=c.get(e,ki,i&-5);if(f!==ki)return f}l=E0(a),a=a[ko]}o=l}return r}function E0(t){let n=t[X],e=n.type;return e===2?n.declTNode:e===1?t[an]:null}function El(t){return sS(At(),t)}function mS(){return La(At(),te())}function La(t,n){return new N(Bn(t,n))}var N=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=mS}return t})();function D0(t){return t instanceof N?t.nativeElement:t}function fS(){return this._results[Symbol.iterator]()}var li=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new E}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=qb(n);(this._changesDetected=!$b(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=fS};function I0(t){return(t.flags&128)===128}var ng=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(ng||{}),M0=new Map,pS=0;function hS(){return pS++}function gS(t){M0.set(t[Br],t)}function Ih(t){M0.delete(t[Br])}var Ny="__ngContext__";function Na(t,n){Zi(n)?(t[Ny]=n[Br],gS(n)):t[Ny]=n}function S0(t){return A0(t[Ea])}function T0(t){return A0(t[jn])}function A0(t){for(;t!==null&&!ni(t);)t=t[jn];return t}var vS;function ig(t){vS=t}var $r=new y("",{factory:()=>_S}),_S="ng";var Cu=new y(""),Ho=new y("",{providedIn:"platform",factory:()=>"unknown"}),Dl=new y(""),Uo=new y("",{factory:()=>d(G).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var k0=!1,O0=new y("",{factory:()=>k0});var Fy=new WeakMap;function bS(t,n){if(t==null||typeof t!="object")return;let e=Fy.get(t);e||(e=new WeakSet,Fy.set(t,e)),e.add(n)}var yS=(t,n,e,i)=>{};function CS(t,n,e,i){yS(t,n,e,i)}function xu(t){return(t.flags&32)===32}var xS=()=>null;function R0(t,n,e=!1){return xS(t,n,e)}function N0(t,n){let e=t.contentQueries;if(e!==null){let i=oe(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];cl(o),s.contentQueries(2,n[a],a)}}}finally{oe(i)}}}function Mh(t,n,e){cl(0);let i=oe(null);try{n(t,e)}finally{oe(i)}}function rg(t,n,e){if(Bp(n)){let i=oe(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{oe(i)}}}var ci=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(ci||{});var wS="svg",ES="math",DS={"http://www.w3.org/2000/svg":wS,"http://www.w3.org/1998/Math/MathML":ES},Qd;function IS(){if(Qd===void 0&&(Qd=null,Pr.trustedTypes))try{Qd=Pr.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Qd}function wu(t){return IS()?.createHTML(t)||t}var Kd;function MS(){if(Kd===void 0&&(Kd=null,Pr.trustedTypes))try{Kd=Pr.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Kd}function Py(t){return MS()?.createScriptURL(t)||t}var Ji=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Td})`}},Sh=class extends Ji{getTypeName(){return"HTML"}},Th=class extends Ji{getTypeName(){return"Style"}},Ah=class extends Ji{getTypeName(){return"Script"}},kh=class extends Ji{getTypeName(){return"URL"}},Oh=class extends Ji{getTypeName(){return"ResourceURL"}};function ui(t){return t instanceof Ji?t.changingThisBreaksApplicationSecurity:t}function er(t,n){let e=F0(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Td})`)}return e===n}function F0(t){return t instanceof Ji&&t.getTypeName()||null}function og(t){return new Sh(t)}function ag(t){return new Th(t)}function sg(t){return new Ah(t)}function lg(t){return new kh(t)}function cg(t){return new Oh(t)}function SS(t){let n=new Nh(t);return TS()?new Rh(n):n}var Rh=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(wu(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Nh=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=wu(n),e}};function TS(){try{return!!new window.DOMParser().parseFromString(wu(""),"text/html")}catch{return!1}}var AS=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Il(t){return t=String(t),t.match(AS)?t:"unsafe:"+t}function tr(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Ml(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var P0=tr("area,br,col,hr,img,wbr"),L0=tr("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),j0=tr("rp,rt"),kS=Ml(j0,L0),OS=Ml(L0,tr("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),RS=Ml(j0,tr("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Ly=Ml(P0,OS,RS,kS),V0=tr("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),NS=tr("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),FS=tr("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),PS=Ml(V0,NS,FS),LS=tr("script,style,template"),Fh=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=BS(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=VS(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=jy(n).toLowerCase();if(!Ly.hasOwnProperty(e))return this.sanitizedSomething=!0,!LS.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!PS.hasOwnProperty(s)){this.sanitizedSomething=!0;continue}let l=o.value;V0[s]&&(l=Il(l)),this.buf.push(" ",a,'="',Vy(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=jy(n).toLowerCase();Ly.hasOwnProperty(e)&&!P0.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(Vy(n))}};function jS(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function VS(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw B0(n);return n}function BS(t){let n=t.firstChild;if(n&&jS(t,n))throw B0(n);return n}function jy(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function B0(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var HS=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,US=/([^\#-~ |!])/g;function Vy(t){return t.replace(/&/g,"&amp;").replace(HS,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(US,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Xd;function dg(t,n){let e=null;try{Xd=Xd||SS(t);let i=n?String(n):"";e=Xd.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Xd.getInertBodyElement(i)}while(i!==o);let s=new Fh().sanitizeChildren(By(e)||e);return wu(s)}finally{if(e){let i=By(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function By(t){return"content"in t&&zS(t)?t.content:null}function zS(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var $S=/^>|^->|<!--|-->|--!>|<!-$/g,qS=/(<|>)/g,GS="\u200B$1\u200B";function WS(t){return t.replace($S,n=>n.replace(qS,GS))}function YS(t,n){return t.createText(n)}function ZS(t,n,e){t.setValue(n,e)}function QS(t,n){return t.createComment(WS(n))}function H0(t,n,e){return t.createElement(n,e)}function du(t,n,e,i,r){t.insertBefore(n,e,i,r)}function U0(t,n,e){t.appendChild(n,e)}function Hy(t,n,e,i,r){i!==null?du(t,n,e,i,r):U0(t,n,e)}function KS(t,n,e,i){t.removeChild(null,n,e,i)}function XS(t,n,e){t.setAttribute(n,"style",e)}function JS(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function z0(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&tS(t,n,i),r!==null&&JS(t,n,r),o!==null&&XS(t,n,o)}function eT(t,n=!0){if(t[0]!=":")return[null,t];let e=t.indexOf(":",1);if(e===-1){if(n)throw new Error(`Unsupported format "${t}" expecting ":namespace:name"`);return[null,t]}return[t.slice(1,e),t.slice(e+1)]}function $0(t){let n=G0();return n?n.sanitize(Te.URL,t)||"":er(t,"URL")?ui(t):Il(Wi(t))}function q0(t){let n=G0();if(n)return Py(n.sanitize(Te.RESOURCE_URL,t)||"");if(er(t,"ResourceURL"))return Py(ui(t));throw new k(904,!1)}function tT(t,n){switch(nT(t,n)){case Te.RESOURCE_URL:return q0;case Te.URL:return $0;default:return null}}function ug(t,n,e){return tT(n,e)?.(t)??t}function G0(){let t=te();return t&&t[ti].sanitizer}function nT(t,n){let[e,i]=iT(t);return ny(i,n,e)}function iT(t){t=t.toLowerCase();let n=eT(t,!1);if(n[0])return n;let i=oi()===-1?null:dl(),r=i?.namespace;if(t==="#host"&&i?.type===2){let o=Bn(i,te());if(o.tagName&&(t=o.tagName.toLowerCase()),r==null){let a=o.namespaceURI;r=a&&DS[a]}}return[r,t]}function W0(t){return t instanceof Function?t():t}function rT(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var Y0="ng-template";function oT(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&rT(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(mg(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function mg(t){return t.type===4&&t.value!==Y0}function aT(t,n,e){let i=t.type===4&&!e?Y0:t.value;return n===i}function sT(t,n,e){let i=4,r=t.attrs,o=r!==null?dT(r):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!ai(i)&&!ai(l))return!1;if(a&&ai(l))continue;a=!1,i=l|i&1;continue}if(!a)if(i&4){if(i=2|i&1,l!==""&&!aT(t,l,e)||l===""&&n.length===1){if(ai(i))return!1;a=!0}}else if(i&8){if(r===null||!oT(t,r,l,e)){if(ai(i))return!1;a=!0}}else{let c=n[++s],f=lT(l,r,mg(t),e);if(f===-1){if(ai(i))return!1;a=!0;continue}if(c!==""){let g;if(f>o?g="":g=r[f+1].toLowerCase(),i&2&&c!==g){if(ai(i))return!1;a=!0}}}}return ai(i)||a}function ai(t){return(t&1)===0}function lT(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return uT(n,t)}function Z0(t,n,e=!1){for(let i=0;i<n.length;i++)if(sT(t,n[i],e))return!0;return!1}function cT(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function dT(t){for(let n=0;n<t.length;n++){let e=t[n];if(h0(e))return n}return t.length}function uT(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function mT(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function Uy(t,n){return t?":not("+n.trim()+")":n}function fT(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!ai(a)&&(n+=Uy(o,r),r=""),i=a,o=o||!ai(i);e++}return r!==""&&(n+=Uy(o,r)),n}function pT(t){return t.map(fT).join(",")}function hT(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!ai(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Ot={};function fg(t,n,e,i,r,o,a,s,l,c,f){let g=Qe+i,v=g+r,b=gT(g,v),w=typeof c=="function"?c():c;return b[X]={type:t,blueprint:b,template:e,queries:null,viewQuery:s,declTNode:n,data:b.slice().fill(null,g),bindingStartIndex:g,expandoStartIndex:v,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:w,incompleteFirstPass:!1,ssrId:f}}function gT(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Ot);return e}function vT(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=fg(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function pg(t,n,e,i,r,o,a,s,l,c,f){let g=n.blueprint.slice();return g[ei]=r,g[se]=i|4|128|8|64|1024,(c!==null||t&&t[se]&2048)&&(g[se]|=2048),qp(g),g[St]=g[ko]=t,g[zt]=e,g[ti]=a||t&&t[ti],g[$e]=s||t&&t[$e],g[jr]=l||t&&t[jr]||null,g[an]=o,g[Br]=hS(),g[xa]=f,g[Vp]=c,g[gn]=n.type==2?t[gn]:g,g}function _T(t,n,e){let i=Bn(n,t),r=vT(e),o=t[ti].rendererFactory,a=hg(t,pg(t,r,null,Q0(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function Q0(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function K0(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function hg(t,n){return t[Ea]?t[jp][jn]=n:t[Ea]=n,t[jp]=n,n}function h(t=1){X0(Ke(),te(),oi()+t,!1)}function X0(t,n,e,i){if(!i)if((n[se]&3)===3){let o=t.preOrderCheckHooks;o!==null&&eu(n,o,e)}else{let o=t.preOrderHooks;o!==null&&tu(n,o,0,e)}Ur(e)}var Eu=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Eu||{});function Ph(t,n,e,i){let r=oe(null);try{let[o,a,s]=t.inputs[e],l=null;(a&Eu.SignalBased)!==0&&(l=n[o][tt]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):c0(n,l,o,i)}finally{oe(r)}}var Oi=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Oi||{}),bT;function gg(t,n){return bT(t,n)}var s8=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Lh=new WeakMap,hl=new WeakSet;function yT(t,n){let e=Lh.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let a=e[o],s=a.parentNode;a===n?(e.splice(o,1),hl.add(a),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&a===r||s&&i&&s!==i)&&(e.splice(o,1),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),a.parentNode?.removeChild(a))}}function CT(t,n){let e=Lh.get(t);e?e.includes(n)||e.push(n):Lh.set(t,[n])}var Fa=new Set,Du=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Du||{}),mi=new y(""),zy=new Set;function zo(t){zy.has(t)||(zy.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Iu=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=x({token:t,providedIn:"root",factory:()=>new t})}return t})(),vg=[0,1,2,3],_g=(()=>{class t{ngZone=d(F);scheduler=d(Si);errorHandler=d(on,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(mi,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Ne(Ie.AfterRenderHooksStart),this.executing=!0;for(let i of vg)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Ne(Ie.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Ro]??=[]).push(e),Po(i),i[se]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Du.AFTER_NEXT_RENDER,e):e()}static \u0275prov=x({token:t,providedIn:"root",factory:()=>new t})}return t})(),bl=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Ro];n&&(this.view[Ro]=n.filter(e=>e!==this))}};function st(t,n){let e=n?.injector??d(W);return zo("NgAfterNextRender"),wT(t,e,n,!0)}function xT(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function wT(t,n,e,i){let r=n.get(Iu);r.impl??=n.get(_g);let o=n.get(mi,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(sn):null,s=n.get(Aa,null,{optional:!0}),l=new bl(r.impl,xT(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(l),l}var ET=new y("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(Ae)})});function J0(t,n,e){let i=t.get(ET);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function DT(t,n){for(let[e,i]of n)J0(t,i.animateFns)}function $y(t,n,e,i){let r=t?.[Da]?.enter;n!==null&&r&&r.has(e.index)&&DT(i,r)}function ka(t,n,e,i,r,o,a,s){if(r!=null){let l,c=!1;ni(r)?l=r:Zi(r)&&(c=!0,r=r[ei]);let f=Vn(r);t===0&&i!==null?($y(s,i,o,e),a==null?U0(n,i,f):du(n,i,f,a||null,!0)):t===1&&i!==null?($y(s,i,o,e),du(n,i,f,a||null,!0),yT(o,f)):t===2?(s?.[Da]?.leave?.has(o.index)&&CT(o,f),hl.delete(f),qy(s,o,e,g=>{if(hl.has(f)){hl.delete(f);return}KS(n,f,c,g)})):t===3&&(hl.delete(f),qy(s,o,e,()=>{n.destroyNode(f)})),l!=null&&PT(n,t,e,l,o,i,a)}}function IT(t,n){eC(t,n),n[ei]=null,n[an]=null}function MT(t,n,e,i,r,o){i[ei]=r,i[an]=n,Mu(t,i,e,1,r,o)}function eC(t,n){n[ti].changeDetectionScheduler?.notify(9),Mu(t,n,n[$e],2,null,null)}function ST(t){let n=t[Ea];if(!n)return gh(t[X],t);for(;n;){let e=null;if(Zi(n))e=n[Ea];else{let i=n[Gt];i&&(e=i)}if(!e){for(;n&&!n[jn]&&n!==t;)Zi(n)&&gh(n[X],n),n=n[St];n===null&&(n=t),Zi(n)&&gh(n[X],n),e=n&&n[jn]}n=e}}function bg(t,n){let e=t[No],i=e.indexOf(n);e.splice(i,1)}function yg(t,n){if(Fo(n))return;let e=n[$e];e.destroyNode&&Mu(t,n,e,3,null,null),ST(n)}function gh(t,n){if(Fo(n))return;let e=oe(null);try{n[se]&=-129,n[se]|=256,n[An]&&Sr(n[An]),kT(t,n),AT(t,n),n[X].type===1&&n[$e].destroy();let i=n[Vr];if(i!==null&&ni(n[St])){i!==n[St]&&bg(i,n);let r=n[Ti];r!==null&&r.detachView(t)}Ih(n)}finally{oe(e)}}function qy(t,n,e,i){let r=t?.[Da];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&Fa.add(t[Br]),J0(e,()=>{if(r.leave&&r.leave.has(n.index)){let a=r.leave.get(n.index),s=[];if(a){for(let l=0;l<a.animateFns.length;l++){let c=a.animateFns[l],{promise:f}=c();s.push(f)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(s),TT(t,i)}else t&&Fa.delete(t[Br]),i(!1)},r)}function TT(t,n){let e=t[Da]?.running;if(e){e.then(()=>{t[Da].running=void 0,Fa.delete(t[Br]),n(!0)});return}n(!1)}function AT(t,n){let e=t.cleanup,i=n[wa];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[wa]=null);let r=n[$i];if(r!==null){n[$i]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[Nr];if(o!==null){n[Nr]=null;for(let a of o)a.destroy()}}function kT(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof jo)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],l=o[a+1];Ne(Ie.LifecycleHookStart,s,l);try{l.call(s)}finally{Ne(Ie.LifecycleHookEnd,s,l)}}else{Ne(Ie.LifecycleHookStart,r,o);try{o.call(r)}finally{Ne(Ie.LifecycleHookEnd,r,o)}}}}}function tC(t,n,e){return OT(t,n.parent,e)}function OT(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[ei];if(Qi(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===ci.None||r===ci.Emulated)return null}return Bn(i,e)}function nC(t,n,e){return NT(t,n,e)}function RT(t,n,e){return t.type&40?Bn(t,e):null}var NT=RT,Gy;function Cg(t,n,e,i){let r=tC(t,i,n),o=n[$e],a=i.parent||n[an],s=nC(a,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)Hy(o,r,e[l],s,!1);else Hy(o,r,e,s,!1);Gy!==void 0&&Gy(o,i,n,e,r)}function gl(t,n){if(n!==null){let e=n.type;if(e&3)return Bn(n,t);if(e&4)return jh(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return gl(t,i);{let r=t[n.index];return ni(r)?jh(-1,r):Vn(r)}}else{if(e&128)return gl(t,n.next);if(e&32)return gg(n,t)()||Vn(t[n.index]);{let i=iC(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Fr(t[gn]);return gl(r,i)}else return gl(t,n.next)}}}return null}function iC(t,n){if(n!==null){let i=t[gn][an],r=n.projection;return i.projection[r]}return null}function jh(t,n){let e=Gt+t+1;if(e<n.length){let i=n[e],r=i[X].firstChild;if(r!==null)return gl(i,r)}return n[Hr]}function xg(t,n,e,i,r,o,a){for(;e!=null;){let s=i[jr];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(a&&n===0&&(l&&Na(Vn(l),i),e.flags|=2),!xu(e))if(c&8)xg(t,n,e.child,i,r,o,!1),ka(n,t,s,r,l,e,o,i);else if(c&32){let f=gg(e,i),g;for(;g=f();)ka(n,t,s,r,g,e,o,i);ka(n,t,s,r,l,e,o,i)}else c&16?rC(t,n,i,e,r,o):ka(n,t,s,r,l,e,o,i);e=a?e.projectionNext:e.next}}function Mu(t,n,e,i,r,o){xg(e,i,t.firstChild,n,r,o,!1)}function FT(t,n,e){let i=n[$e],r=tC(t,e,n),o=e.parent||n[an],a=nC(o,e,n);rC(i,0,n,e,r,a)}function rC(t,n,e,i,r,o){let a=e[gn],l=a[an].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let f=l[c];ka(n,t,e[jr],r,f,i,o,e)}else{let c=l,f=a[St];I0(i)&&(c.flags|=128),xg(t,n,c,f,r,o,!0)}}function PT(t,n,e,i,r,o,a){let s=i[Hr],l=Vn(i);s!==l&&ka(n,t,e,o,s,r,a);for(let c=Gt;c<i.length;c++){let f=i[c];Mu(f[X],f,t,n,o,s)}}function LT(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Oi.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Oi.Important),t.setStyle(e,i,r,o))}}function oC(t,n,e,i,r){let o=oi(),a=i&2;try{Ur(-1),a&&n.length>Qe&&X0(t,n,Qe,!1);let s=a?Ie.TemplateUpdateStart:Ie.TemplateCreateStart;Ne(s,r,e),e(i,r)}finally{Ur(o);let s=a?Ie.TemplateUpdateEnd:Ie.TemplateCreateEnd;Ne(s,r,e)}}function Su(t,n,e){$T(t,n,e),(e.flags&64)===64&&qT(t,n,e)}function Sl(t,n,e=Bn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function jT(t,n,e,i){let o=i.get(O0,k0)||e===ci.ShadowDom||e===ci.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);return VT(a),a}function VT(t){BT(t)}var BT=()=>null;function HT(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function UT(t,n,e,i,r,o){let a=n[X];if(Ig(t,a,n,e,i)){Qi(t)&&zT(n,t.index);return}t.type&3&&(e=HT(e)),aC(t,n,e,i,r,o)}function aC(t,n,e,i,r,o){if(t.type&3){let a=Bn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(a,e,i)}else t.type&12}function zT(t,n){let e=Hn(n,t);e[se]&16||(e[se]|=64)}function $T(t,n,e){let i=e.directiveStart,r=e.directiveEnd;Qi(e)&&_T(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||cu(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],l=_l(n,t,a,e);if(Na(l,n),o!==null&&ZT(n,a-i,l,s,e,o),ii(s)){let c=Hn(e.index,n);c[zt]=_l(n,t,a,e)}}}function qT(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=hy();try{Ur(o);for(let s=i;s<r;s++){let l=t.data[s],c=n[s];zd(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&GT(l,c)}}finally{Ur(-1),zd(a)}}function GT(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function wg(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];Z0(n,o.selectors,!1)&&(i??=[],ii(o)?i.unshift(o):i.push(o))}return i}function WT(t,n,e,i,r,o){let a=Bn(t,n);YT(n[$e],a,o,t.value,e,i,r)}function YT(t,n,e,i,r,o,a){if(o==null)t.removeAttribute(n,r,e);else{let s=a==null?Wi(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function ZT(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];Ph(i,e,l,c)}}function Eg(t,n,e,i,r){let o=Qe+e,a=n[X],s=r(a,n,t,i,e);n[o]=s,Sa(t,!0);let l=t.type===2;return l?(z0(n[$e],s,t),(ly()===0||Ia(t))&&Na(s,n),cy()):Na(s,n),Wd()&&(!l||!xu(t))&&Cg(a,n,s,t),t}function Dg(t){let n=t;return th()?nh():(n=n.parent,Sa(n,!1)),n}function QT(t,n){let e=t[jr];if(!e)return;let i;try{i=e.get(vn,null)}catch{i=null}i?.(n)}function Ig(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],f=a[l+1],g=n.data[c];Ph(g,e[c],f,r),s=!0}if(o)for(let l of o){let c=e[l],f=n.data[l];Ph(f,c,i,r),s=!0}return s}function KT(t,n){let e=Hn(n,t),i=e[X];XT(i,e);let r=e[ei];r!==null&&e[xa]===null&&(e[xa]=R0(r,e[jr])),Ne(Ie.ComponentStart);try{Mg(i,e,e[zt])}finally{Ne(Ie.ComponentEnd,e[zt])}}function XT(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function Mg(t,n,e){qd(n);try{let i=t.viewQuery;i!==null&&Mh(1,i,e);let r=t.template;r!==null&&oC(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Ti]?.finishViewCreation(t),t.staticContentQueries&&N0(t,n),t.staticViewQueries&&Mh(2,t.viewQuery,e);let o=t.components;o!==null&&JT(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[se]&=-5,Gd()}}function JT(t,n){for(let e=0;e<n.length;e++)KT(t,n[e])}function Sg(t,n,e,i){let r=oe(null);try{let o=n.tView,s=t[se]&4096?4096:16,l=pg(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[Vr]=c;let f=t[Ti];return f!==null&&(l[Ti]=f.createEmbeddedView(o)),Mg(o,l,e),l}finally{oe(r)}}function uu(t,n){return!n||n.firstChild===null||I0(t)}function yl(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(Vn(o)),ni(o)&&sC(o,i);let a=e.type;if(a&8)yl(t,n,e.child,i);else if(a&32){let s=gg(e,n),l;for(;l=s();)i.push(l)}else if(a&16){let s=iC(n,e);if(Array.isArray(s))i.push(...s);else{let l=Fr(n[gn]);yl(l[X],l,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function sC(t,n){for(let e=Gt;e<t.length;e++){let i=t[e],r=i[X].firstChild;r!==null&&yl(i[X],i,r,n)}t[Hr]!==t[ei]&&n.push(t[Hr])}function lC(t){if(t[Ro]!==null){for(let n of t[Ro])n.impl.addSequence(n);t[Ro].length=0}}var cC=[];function eA(t){return t[An]??tA(t)}function tA(t){let n=cC.pop()??Object.create(iA);return n.lView=t,n}function nA(t){t.lView[An]!==t&&(t.lView=null,cC.push(t))}var iA=ee(C({},Dr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Po(t.lView)},consumerOnSignalRead(){this.lView[An]=this}});function rA(t){let n=t[An]??Object.create(oA);return n.lView=t,n}var oA=ee(C({},Dr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=Fr(t.lView);for(;n&&!dC(n[X]);)n=Fr(n);n&&Gp(n)},consumerOnSignalRead(){this.lView[An]=this}});function dC(t){return t.type!==2}function uC(t){if(t[Nr]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Nr])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[se]&8192)}}var aA=100;function mC(t,n=0){let i=t[ti].rendererFactory,r=!1;r||i.begin?.();try{sA(t,n)}finally{r||i.end?.()}}function sA(t,n){let e=ih();try{Qs(!0),Vh(t,n);let i=0;for(;al(t);){if(i===aA)throw new k(103,!1);i++,Vh(t,1)}}finally{Qs(e)}}function lA(t,n,e,i){if(Fo(n))return;let r=n[se],o=!1,a=!1;qd(n);let s=!0,l=null,c=null;o||(dC(t)?(c=eA(n),l=Hi(c)):zc()===null?(s=!1,c=rA(n),l=Hi(c)):n[An]&&(Sr(n[An]),n[An]=null));try{qp(n),my(t.bindingStartIndex),e!==null&&oC(t,n,e,2,i);let f=(r&3)===3;if(!o)if(f){let b=t.preOrderCheckHooks;b!==null&&eu(n,b,null)}else{let b=t.preOrderHooks;b!==null&&tu(n,b,0,null),ph(n,0)}if(a||cA(n),uC(n),fC(n,0),t.contentQueries!==null&&N0(t,n),!o)if(f){let b=t.contentCheckHooks;b!==null&&eu(n,b)}else{let b=t.contentHooks;b!==null&&tu(n,b,1),ph(n,1)}uA(t,n);let g=t.components;g!==null&&hC(n,g,0);let v=t.viewQuery;if(v!==null&&Mh(2,v,i),!o)if(f){let b=t.viewCheckHooks;b!==null&&eu(n,b)}else{let b=t.viewHooks;b!==null&&tu(n,b,2),ph(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[jd]){for(let b of n[jd])b();n[jd]=null}o||(lC(n),n[se]&=-73)}catch(f){throw o||Po(n),f}finally{c!==null&&(Mr(c,l),s&&nA(c)),Gd()}}function fC(t,n){for(let e=S0(t);e!==null;e=T0(e))for(let i=Gt;i<e.length;i++){let r=e[i];pC(r,n)}}function cA(t){for(let n=S0(t);n!==null;n=T0(n)){if(!(n[se]&2))continue;let e=n[No];for(let i=0;i<e.length;i++){let r=e[i];Gp(r)}}}function dA(t,n,e){Ne(Ie.ComponentStart);let i=Hn(n,t);try{pC(i,e)}finally{Ne(Ie.ComponentEnd,i[zt])}}function pC(t,n){Bd(t)&&Vh(t,n)}function Vh(t,n){let i=t[X],r=t[se],o=t[An],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&ca(o)),a||=!1,o&&(o.dirty=!1),t[se]&=-9217,a)lA(i,t,i.template,t[zt]);else if(r&8192){let s=oe(null);try{uC(t),fC(t,1);let l=i.components;l!==null&&hC(t,l,1),lC(t)}finally{oe(s)}}}function hC(t,n,e){for(let i=0;i<n.length;i++)dA(t,n[i],e)}function uA(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Ur(~r);else{let o=r,a=e[++i],s=e[++i];py(a,o);let l=n[o];Ne(Ie.HostBindingsUpdateStart,l);try{s(2,l)}finally{Ne(Ie.HostBindingsUpdateEnd,l)}}}}finally{Ur(-1)}}function Tg(t,n){let e=ih()?64:1088;for(t[ti].changeDetectionScheduler?.notify(n);t;){t[se]|=e;let i=Fr(t);if(Ma(t)&&!i)return t;t=i}return null}function gC(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function mA(t,n){let e=Gt+n;if(e<t.length)return t[e]}function Ag(t,n,e,i=!0){let r=n[X];if(pA(r,n,t,e),i){let a=jh(e,t),s=n[$e],l=s.parentNode(t[Hr]);l!==null&&MT(r,t[an],s,n,l,a)}let o=n[xa];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function fA(t,n){let e=mu(t,n);return e!==void 0&&yg(e[X],e),e}function mu(t,n){if(t.length<=Gt)return;let e=Gt+n,i=t[e];if(i){let r=i[Vr];r!==null&&r!==t&&bg(r,i),n>0&&(t[e-1][jn]=i[jn]);let o=il(t,Gt+n);IT(i[X],i);let a=o[Ti];a!==null&&a.detachView(o[X]),i[St]=null,i[jn]=null,i[se]&=-129}return i}function pA(t,n,e,i){let r=Gt+i,o=e.length;i>0&&(e[r-1][jn]=n),i<o-Gt?(n[jn]=e[r],Op(e,Gt+i,n)):(e.push(n),n[jn]=null),n[St]=e;let a=n[Vr];a!==null&&e!==a&&vC(a,n);let s=n[Ti];s!==null&&s.insertView(t),Hd(n),n[se]|=128}function vC(t,n){let e=t[No],i=n[St];if(Zi(i))t[se]|=2;else{let r=i[St][gn];n[gn]!==r&&(t[se]|=2)}e===null?t[No]=[n]:e.push(n)}var zr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[X];return yl(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[zt]}set context(n){this._lView[zt]=n}get destroyed(){return Fo(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[St];if(ni(n)){let e=n[ol],i=e?e.indexOf(this):-1;i>-1&&(mu(n,i),il(e,i))}this._attachedToViewContainer=!1}yg(this._lView[X],this._lView)}onDestroy(n){Wp(this._lView,n)}markForCheck(){Tg(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[se]&=-129}reattach(){Hd(this._lView),this._lView[se]|=128}detectChanges(){this._lView[se]|=1024,mC(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new k(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Ma(this._lView),e=this._lView[Vr];e!==null&&!n&&bg(e,this._lView),eC(this._lView[X],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new k(902,!1);this._appRef=n;let e=Ma(this._lView),i=this._lView[Vr];i!==null&&!e&&vC(i,this._lView),Hd(this._lView)}};var kt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=hA;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Sg(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new zr(o)}}return t})();function hA(){return Tu(At(),te())}function Tu(t,n){return t.type&4?new kt(n,t,La(t,n)):null}function ja(t,n,e,i,r){let o=t.data[n];if(o===null)o=gA(t,n,e,i,r),fy()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=dy();o.injectorIndex=a===null?-1:a.injectorIndex}return Sa(o,!0),o}function gA(t,n,e,i,r){let o=eh(),a=th(),s=a?o:o&&o.parent,l=t.data[n]=_A(t,s,e,n,i,r);return vA(t,l,o,a),l}function vA(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function _A(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return Kp()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:sh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var bA=()=>null,yA=()=>null;function Bh(t,n){return bA(t,n)}function CA(t,n,e){return yA(t,n,e)}var _C=class{},Au=class{},Hh=class{resolveComponentFactory(n){throw new k(917,!1)}},Tl=class{static NULL=new Hh},at=class{},Se=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>xA()}return t})();function xA(){let t=te(),n=At(),e=Hn(n.index,t);return(Zi(e)?e:t)[$e]}var bC=(()=>{class t{static \u0275prov=x({token:t,providedIn:"root",factory:()=>null})}return t})();var iu={},Uh=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,iu,i);return r!==iu||e===iu?r:this.parentInjector.get(n,e,i)}};function fu(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=Ad(r,s);else if(o==2){let l=s,c=n[++a];i=Ad(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function D(t,n=0){let e=te();if(e===null)return Q(t,n);let i=At();return x0(i,e,bt(t),n)}function yC(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,l=null,c=null;for(let f of a)if(f.resolveHostDirectives!==null){[s,l,c]=f.resolveHostDirectives(a);break}DA(t,n,e,s,o,l,c)}o!==null&&i!==null&&wA(e,i,o)}function wA(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new k(-301,!1);i.push(n[r],o)}}function EA(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function DA(t,n,e,i,r,o,a){let s=i.length,l=null;for(let v=0;v<s;v++){let b=i[v];l===null&&ii(b)&&(l=b,EA(t,e,v)),Eh(cu(e,n),t,b.type)}kA(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let v=0;v<s;v++){let b=i[v];b.providersResolver&&b.providersResolver(b)}let c=!1,f=!1,g=K0(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let v=0;v<s;v++){let b=i[v];if(e.mergedAttrs=Ra(e.mergedAttrs,b.hostAttrs),MA(t,e,n,g,b),AA(g,b,r),a!==null&&a.has(b)){let[j,U]=a.get(b);e.directiveToIndex.set(b.type,[g,j+e.directiveStart,U+e.directiveStart])}else(o===null||!o.has(b))&&e.directiveToIndex.set(b.type,g);b.contentQueries!==null&&(e.flags|=4),(b.hostBindings!==null||b.hostAttrs!==null||b.hostVars!==0)&&(e.flags|=64);let w=b.type.prototype;!c&&(w.ngOnChanges||w.ngOnInit||w.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!f&&(w.ngOnChanges||w.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),f=!0),g++}IA(t,e,o)}function IA(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))Wy(0,n,r,i),Wy(1,n,r,i),Zy(n,i,!1);else{let o=e.get(r);Yy(0,n,o,i),Yy(1,n,o,i),Zy(n,i,!0)}}}function Wy(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),CC(n,o)}}function Yy(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),CC(n,a)}}function CC(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Zy(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||mg(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let l=i[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let f of c)if(f===n){a??=[],a.push(l,i[s+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let f=0;f<c.length;f+=2)if(c[f]===n){a??=[],a.push(c[f+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function MA(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Rr(r.type,!0)),a=new jo(o,ii(r),D,null);t.blueprint[i]=a,e[i]=a,SA(t,n,i,K0(t,e,r.hostVars,Ot),r)}function SA(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;TA(a)!=s&&a.push(s),a.push(e,i,o)}}function TA(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function AA(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;ii(n)&&(e[""]=t)}}function kA(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function kg(t,n,e,i,r,o,a,s){let l=n[X],c=l.consts,f=ri(c,a),g=ja(l,t,e,i,f);return o&&yC(l,n,g,ri(c,s),r),g.mergedAttrs=Ra(g.mergedAttrs,g.attrs),g.attrs!==null&&fu(g,g.attrs,!1),g.mergedAttrs!==null&&fu(g,g.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,g),g}function Og(t,n){f0(t,n),Bp(n)&&t.queries.elementEnd(n)}function OA(t,n,e,i,r,o){let a=n.consts,s=ri(a,r),l=ja(n,t,e,i,s);if(l.mergedAttrs=Ra(l.mergedAttrs,l.attrs),o!=null){let c=ri(a,o);l.localNames=[];for(let f=0;f<c.length;f+=2)l.localNames.push(c[f],-1)}return l.attrs!==null&&fu(l,l.attrs,!1),l.mergedAttrs!==null&&fu(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}function Rg(t){return wC(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function xC(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function wC(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function Ng(t,n,e){return t[n]=e}function EC(t,n){return t[n]}function di(t,n,e){if(e===Ot)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function DC(t,n,e,i){let r=di(t,n,e);return di(t,n+1,i)||r}function RA(t,n,e,i,r){let o=DC(t,n,e,i);return di(t,n+2,r)||o}function ru(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&bS(r,o);let a=Qi(t)?Hn(t.index,n):n;Tg(a,5);let s=n[zt],l=Qy(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)l=Qy(n,s,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function Qy(t,n,e,i){let r=oe(null);try{return Ne(Ie.OutputStart,n,e),e(i)!==!1}catch(o){return QT(t,o),!1}finally{Ne(Ie.OutputEnd,n,e),oe(r)}}function IC(t,n,e,i,r,o,a,s){let l=Ia(t),c=!1,f=null;if(!i&&l&&(f=FA(n,e,o,t.index)),f!==null){let g=f.__ngLastListenerFn__||f;g.__ngNextListenerFn__=a,f.__ngLastListenerFn__=a,c=!0}else{let g=Bn(t,e),v=i?i(g):g;CS(e,v,o,s),i||(s.__ngNativeEl__=g);let b=r.listen(v,o,s);if(!NA(o)){let w=i?j=>i(Vn(j[t.index])):t.index;MC(w,n,e,o,s,b,!1)}}return c}function NA(t){return t.startsWith("animation")||t.startsWith("transition")}function FA(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[wa],l=r[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function MC(t,n,e,i,r,o,a){let s=n.firstCreatePass?Zp(n):null,l=Yp(e),c=l.length;l.push(r,o),s&&s.push(i,t,c,(c+1)*(a?-1:1))}function Ky(t,n,e,i,r,o){let a=n[e],s=n[X],c=s.data[e].outputs[i],g=a[c].subscribe(o);MC(t.index,s,n,r,o,g,!0)}var zh=Symbol("BINDING");function SC(t){return t.debugInfo?.className||t.type.name||null}var pu=class extends Tl{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=Gi(n);return new Vo(e,this.ngModule)}};function PA(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Eu.SignalBased)!==0};return r&&(o.transform=r),o})}function LA(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function jA(t,n,e){let i=n instanceof Ae?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Uh(e,i):e}function VA(t){let n=t.get(at,null);if(n===null)throw new k(407,!1);let e=t.get(bC,null),i=t.get(Si,null),r=t.get(mi,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function BA(t,n){let e=TC(t);return H0(n,e,e==="svg"?Zs:e==="math"?Hp:null)}function HA(t){if(t?.toLowerCase()==="script")throw new k(905,!1)}function TC(t){return(t.selectors[0][0]||"div").toLowerCase()}var Vo=class extends Au{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=PA(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=LA(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=pT(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a){Ne(Ie.DynamicComponentStart);let s=oe(null);try{let l=this.componentDef,c=jA(l,r||this.ngModule,n),f=VA(c),g=f.tracingService;return g&&g.componentCreate?g.componentCreate(SC(l),()=>this.createComponentRef(f,c,e,i,o,a)):this.createComponentRef(f,c,e,i,o,a)}finally{oe(s)}}createComponentRef(n,e,i,r,o,a){let s=this.componentDef,l=UA(r,s,a,o),c=n.rendererFactory.createRenderer(null,s),f=r?jT(c,r,s.encapsulation,e):BA(s,c);HA(f?.tagName);let g=a?.some(Xy)||o?.some(w=>typeof w!="function"&&w.bindings.some(Xy)),v=pg(null,l,null,512|Q0(s),null,null,n,c,e,null,R0(f,e,!0));v[Qe]=f,qd(v);let b=null;try{let w=kg(Qe,v,2,"#host",()=>l.directiveRegistry,!0,0);z0(c,f,w),Na(f,v),Su(l,v,w),rg(l,w,v),Og(l,w),i!==void 0&&$A(w,this.ngContentSelectors,i),b=Hn(w.index,v),v[zt]=b[zt],Mg(l,v,null)}catch(w){throw b!==null&&Ih(b),Ih(v),w}finally{Ne(Ie.DynamicComponentEnd),Gd()}return new hu(this.componentType,v,!!g)}};function UA(t,n,e,i){let r=t?["ng-version","21.2.20"]:hT(n.selectors[0]),o=null,a=null,s=0;if(e)for(let f of e)s+=f[zh].requiredVars,f.create&&(f.targetIdx=0,(o??=[]).push(f)),f.update&&(f.targetIdx=0,(a??=[]).push(f));if(i)for(let f=0;f<i.length;f++){let g=i[f];if(typeof g!="function")for(let v of g.bindings){s+=v[zh].requiredVars;let b=f+1;v.create&&(v.targetIdx=b,(o??=[]).push(v)),v.update&&(v.targetIdx=b,(a??=[]).push(v))}}let l=[n];if(i)for(let f of i){let g=typeof f=="function"?f:f.type,v=Od(g);l.push(v)}return fg(0,null,zA(o,a),1,s,l,null,null,null,[r],null)}function zA(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function Xy(t){let n=t[zh].kind;return n==="input"||n==="twoWay"}var hu=class extends _C{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Vd(e[X],Qe),this.location=La(this._tNode,e),this.instance=Hn(this._tNode.index,e)[zt],this.hostView=this.changeDetectorRef=new zr(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=Ig(i,r[X],r,n,e);this.previousInputValues.set(n,e);let a=Hn(i.index,r);Tg(a,1)}get injector(){return new Lo(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function $A(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var nt=(()=>{class t{static __NG_ELEMENT_ID__=qA}return t})();function qA(){let t=At();return AC(t,te())}var $h=class t extends nt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return La(this._hostTNode,this._hostLView)}get injector(){return new Lo(this._hostTNode,this._hostLView)}get parentInjector(){let n=tg(this._hostTNode,this._hostLView);if(g0(n)){let e=su(n,this._hostLView),i=au(n),r=e[X].data[i+8];return new Lo(r,e)}else return new Lo(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Jy(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-Gt}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let a=Bh(this._lContainer,n.ssrId),s=n.createEmbeddedViewImpl(e||{},o,a);return this.insertImpl(s,r,uu(this._hostTNode,a)),s}createComponent(n,e,i,r,o,a,s){let l=n&&!WM(n),c;if(l)c=e;else{let U=e||{};c=U.index,i=U.injector,r=U.projectableNodes,o=U.environmentInjector||U.ngModuleRef,a=U.directives,s=U.bindings}let f=l?n:new Vo(Gi(n)),g=i||this.parentInjector;if(!o&&f.ngModule==null){let Y=(l?g:this.parentInjector).get(Ae,null);Y&&(o=Y)}let v=Gi(f.componentType??{}),b=Bh(this._lContainer,v?.id??null),w=b?.firstChild??null,j=f.create(g,r,w,o,a,s);return this.insertImpl(j.hostView,c,uu(this._hostTNode,b)),j}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(ry(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=r[St],c=new t(l,l[an],l[St]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return Ag(a,r,o,i),n.attachToViewContainerRef(),Op(vh(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Jy(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=mu(this._lContainer,e);i&&(il(vh(this._lContainer),e),yg(i[X],i))}detach(n){let e=this._adjustIndex(n,-1),i=mu(this._lContainer,e);return i&&il(vh(this._lContainer),e)!=null?new zr(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Jy(t){return t[ol]}function vh(t){return t[ol]||(t[ol]=[])}function AC(t,n){let e,i=n[t.index];return ni(i)?e=i:(e=gC(i,n,null,t),n[t.index]=e,hg(n,e)),WA(e,n,t,i),new $h(e,t,n)}function GA(t,n){let e=t[$e],i=e.createComment(""),r=Bn(n,t),o=e.parentNode(r);return du(e,o,i,e.nextSibling(r),!1),i}var WA=QA,YA=()=>!1;function ZA(t,n,e){return YA(t,n,e)}function QA(t,n,e,i){if(t[Hr])return;let r;e.type&8?r=Vn(i):r=GA(n,e),t[Hr]=r}var qh=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Gh=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Pg(n,e).matches!==null&&this.queries[e].setDirty()}},gu=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=tk(n):this.predicate=n}},Wh=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Yh=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,KA(e,o)),this.matchTNodeWithReadOption(n,e,nu(e,n,o,!1,!1))}else i===kt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,nu(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===N||r===nt||r===kt&&e.type&4)this.addMatch(e.index,-2);else{let o=nu(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function KA(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function XA(t,n){return t.type&11?La(t,n):t.type&4?Tu(t,n):null}function JA(t,n,e,i){return e===-1?XA(n,t):e===-2?ek(t,n,i):_l(t,t[X],e,n)}function ek(t,n,e){if(e===N)return La(n,t);if(e===kt)return Tu(n,t);if(e===nt)return AC(n,t)}function kC(t,n,e,i){let r=n[Ti].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let f=o[c];s.push(JA(n,f,a[l+1],e.metadata.read))}}r.matches=s}return r.matches}function Zh(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=kC(t,n,r,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)i.push(a[s/2]);else{let c=o[s+1],f=n[-l];for(let g=Gt;g<f.length;g++){let v=f[g];v[Vr]===v[St]&&Zh(v[X],v,c,i)}if(f[No]!==null){let g=f[No];for(let v=0;v<g.length;v++){let b=g[v];Zh(b[X],b,c,i)}}}}}return i}function Fg(t,n){return t[Ti].queries[n].queryList}function OC(t,n,e){let i=new li((e&4)===4);return sy(t,n,i,i.destroy),(n[Ti]??=new Gh).queries.push(new qh(i))-1}function RC(t,n,e){let i=Ke();return i.firstCreatePass&&(FC(i,new gu(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),OC(i,te(),n)}function NC(t,n,e,i){let r=Ke();if(r.firstCreatePass){let o=At();FC(r,new gu(n,e,i),o.index),nk(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return OC(r,te(),e)}function tk(t){return t.split(",").map(n=>n.trim())}function FC(t,n,e){t.queries===null&&(t.queries=new Wh),t.queries.track(new Yh(n,e))}function nk(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Pg(t,n){return t.queries.getByIndex(n)}function PC(t,n){let e=t[X],i=Pg(e,n);return i.crossesNgTemplate?Zh(e,t,n,[]):kC(e,t,i,n)}function LC(t,n,e){let i,r=Ls(()=>{i._dirtyCounter();let o=ik(i,t);if(n&&o===void 0)throw new k(-951,!1);return o});return i=r[tt],i._dirtyCounter=ne(0),i._flatValue=void 0,r}function Lg(t){return LC(!0,!1,t)}function jg(t){return LC(!0,!0,t)}function jC(t,n){let e=t[tt];e._lView=te(),e._queryIndex=n,e._queryList=Fg(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function ik(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[se]&4)return n?void 0:qt;let r=Fg(e,i),o=PC(e,i);return r.reset(o,D0),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var Ri=class{},ku=class{};var vu=class extends Ri{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new pu(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=Tp(n);this._bootstrapComponents=W0(o.bootstrap),this._r3Injector=lh(n,e,[{provide:Ri,useValue:this},{provide:Tl,useValue:this.componentFactoryResolver},...i],Js(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},_u=class extends ku{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new vu(this.moduleType,n,[])}};var Cl=class extends Ri{injector;componentFactoryResolver=new pu(this);instance=null;constructor(n){super();let e=new To([...n.providers,{provide:Ri,useValue:this},{provide:Tl,useValue:this.componentFactoryResolver}],n.parent||Ca(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Al(t,n,e=null){return new Cl({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var rk=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Fp(!1,e.type),r=i.length>0?Al([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=x({token:t,providedIn:"environment",factory:()=>new t(Q(Ae))})}return t})();function A(t){return wl(()=>{let n=VC(t),e=ee(C({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===ng.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(rk).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||ci.Emulated,styles:t.styles||qt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&zo("NgStandalone"),BC(e);let i=t.dependencies;return e.directiveDefs=e0(i,ok),e.pipeDefs=e0(i,Bb),e.id=lk(e),e})}function ok(t){return Gi(t)||Od(t)}function B(t){return wl(()=>({type:t.type,bootstrap:t.bootstrap||qt,declarations:t.declarations||qt,imports:t.imports||qt,exports:t.exports||qt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function ak(t,n){if(t==null)return Jn;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,a,s,l;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,l=r[3]||null):(o=r,a=r,s=Eu.None,l=null),e[o]=[i,s,l],n[o]=a}return e}function sk(t){if(t==null)return Jn;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function S(t){return wl(()=>{let n=VC(t);return BC(n),n})}function Vg(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function VC(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Jn,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||qt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:ak(t.inputs,n),outputs:sk(t.outputs),debugInfo:null}}function BC(t){t.features?.forEach(n=>n(t))}function e0(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function lk(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function Bg(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=ck,e.hostDirectives=i?t.map(Qh):[t]):i?e.hostDirectives.unshift(...t.map(Qh)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function ck(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let a=t[o];if(a.hostDirectives!==null){let s=n.length;i??=new Map,r??=new Map,HC(a,n,i),r.set(a,[s,n.length-1])}o===0&&ii(a)&&(e=!0,n.push(a))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return[n,i,r]}function HC(t,n,e){if(t.hostDirectives!==null)for(let i of t.hostDirectives)if(typeof i=="function"){let r=i();for(let o of r)t0(Qh(o),n,e)}else t0(i,n,e)}function t0(t,n,e){let i=Od(t.directive);dk(i.declaredInputs,t.inputs),HC(i,n,e),e.set(i,t),n.push(i)}function Qh(t){return typeof t=="function"?{directive:bt(t),inputs:Jn,outputs:Jn}:{directive:bt(t.directive),inputs:n0(t.inputs),outputs:n0(t.outputs)}}function n0(t){if(t===void 0||t.length===0)return Jn;let n={};for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function dk(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function uk(t){return Object.getPrototypeOf(t.prototype).constructor}function be(t){let n=uk(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,tl)?n[tl]:void 0,a=Object.hasOwn(n,nl)?n[nl]:void 0;if(ii(t))r=o??a;else{if(o)throw new k(903,!1);r=a}if(r){if(e){i.push(r);let l=t;l.inputs=_h(t.inputs),l.declaredInputs=_h(t.declaredInputs),l.outputs=_h(t.outputs);let c=r.hostBindings;c&&gk(t,c);let f=r.viewQuery,g=r.contentQueries;if(f&&pk(t,f),g&&hk(t,g),mk(t,r),Vb(t.outputs,r.outputs),ii(r)&&r.data.animation){let v=t.data;v.animation=(v.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let l=0;l<s.length;l++){let c=s[l];c&&c.ngInherit&&c(t),c===be&&(e=!1)}}n=Object.getPrototypeOf(n)}fk(i)}function mk(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function fk(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Ra(r.hostAttrs,e=Ra(e,r.hostAttrs))}}function _h(t){return t===Jn?{}:t===qt?[]:t}function pk(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function hk(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function gk(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function UC(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=Ra(t.mergedAttrs,t.attrs);let f=t.tView=fg(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),f.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),Sa(t,!1);let l=_k(e,n,t,i);Wd()&&Cg(e,n,l,t),Na(l,n);let c=gC(l,n,l,t);n[i+Qe]=c,hg(n,c),ZA(c,t,n)}function vk(t,n,e,i,r,o,a,s,l,c,f){let g=e+Qe,v;return n.firstCreatePass?(v=ja(n,g,4,a||null,s||null),Ud()&&yC(n,t,v,ri(n.consts,c),wg),f0(n,v)):v=n.data[g],UC(v,t,n,e,i,r,o,l),Ia(v)&&Su(n,t,v),c!=null&&Sl(t,v,f),v}function Ou(t,n,e,i,r,o,a,s,l,c,f){let g=e+Qe,v;if(n.firstCreatePass){if(v=ja(n,g,4,a||null,s||null),c!=null){let b=ri(n.consts,c);v.localNames=[];for(let w=0;w<b.length;w+=2)v.localNames.push(b[w],-1)}}else v=n.data[g];return UC(v,t,n,e,i,r,o,l),c!=null&&Sl(t,v,f),v}function M(t,n,e,i,r,o,a,s){let l=te(),c=Ke(),f=ri(c.consts,o);return vk(l,c,t,n,e,i,r,f,void 0,a,s),M}function Ru(t,n,e,i,r,o,a,s){let l=te(),c=Ke(),f=ri(c.consts,o);return Ou(l,c,t,n,e,i,r,f,void 0,a,s),Ru}var _k=bk;function bk(t,n,e,i){return ml(!0),n[$e].createComment("")}var Nu=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function nr(t){return typeof t=="function"&&t[tt]!==void 0}var Hg=new y("");function ir(t){return!!t&&typeof t.then=="function"}function Fu(t){return!!t&&typeof t.subscribe=="function"}var zC=new y("");var Ug=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(zC,{optional:!0})??[];injector=d(W);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Tt(this.injector,r);if(ir(o))e.push(o);else if(Fu(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Pu=new y("");function $C(){Wf(()=>{let t="";throw new k(600,t)})}function qC(t){return t.isBoundToModule}var yk=10;var kn=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(vn);afterRenderManager=d(Iu);zonelessEnabled=d(fl);rootEffectScheduler=d(Zd);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new E;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(Ki);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(V(e=>!e))}constructor(){d(mi,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(Ae);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=W.NULL){return this._injector.get(F).run(()=>{Ne(Ie.BootstrapComponentStart);let a=e instanceof Au;if(!this._injector.get(Ug).done){let w="";throw new k(405,w)}let l;a?l=e:l=this._injector.get(Tl).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=qC(l)?void 0:this._injector.get(Ri),f=i||l.selector,g=l.create(r,[],f,c),v=g.location.nativeElement,b=g.injector.get(Hg,null);return b?.registerApplication(v),g.onDestroy(()=>{this.detachView(g.hostView),vl(this.components,g),b?.unregisterApplication(v)}),this._loadComponent(g),Ne(Ie.BootstrapComponentEnd,g),g})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Ne(Ie.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Du.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Ne(Ie.ChangeDetectionEnd),new k(101,!1);let e=oe(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,oe(e),this.afterTick.next(),Ne(Ie.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(at,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<yk;){Ne(Ie.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Ne(Ie.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!al(r))continue;let o=i&&!this.zonelessEnabled?0:1;mC(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>al(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;vl(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Pu,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>vl(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new k(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function vl(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function ce(t,n,e,i){let r=te(),o=Ta();if(di(r,o,n)){let a=Ke(),s=dl();WT(s,r,t,n,e,i)}return ce}function Fe(t,n,e,i,r,o,a,s){zo("NgControlFlow");let l=te(),c=Ke(),f=ri(c.consts,o);return Ou(l,c,t,n,e,i,r,f,256,a,s),zg}function zg(t,n,e,i,r,o,a,s){zo("NgControlFlow");let l=te(),c=Ke(),f=ri(c.consts,o);return Ou(l,c,t,n,e,i,r,f,512,a,s),zg}function Pe(t,n){zo("NgControlFlow");let e=te(),i=Ta(),r=e[i]!==Ot?e[i]:-1,o=r!==-1?i0(e,Qe+r):void 0,a=0;if(di(e,i,t)){let s=oe(null);try{if(o!==void 0&&fA(o,a),t!==-1){let l=Qe+t,c=i0(e,l),f=Ck(e[X],l),g=CA(c,f,e),v=Sg(e,f,n,{dehydratedView:g});Ag(c,v,a,uu(f,g))}}finally{oe(s)}}else if(o!==void 0){let s=mA(o,a);s!==void 0&&(s[zt]=n)}}function i0(t,n){return t[n]}function Ck(t,n){return Vd(t,n)}function _(t,n,e){let i=te(),r=Ta();if(di(i,r,n)){let o=Ke(),a=dl();UT(a,i,t,n,i[$e],e)}return _}function Kh(t,n,e,i,r){Ig(n,t,e,r?"class":"style",i)}function m(t,n,e,i){let r=te(),o=r[X],a=t+Qe,s=o.firstCreatePass?kg(a,r,2,n,wg,Ud(),e,i):o.data[a];if(Qi(s)){let l=r[ti].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(SC(c),()=>(r0(t,n,r,s,i),m))}}return r0(t,n,r,s,i),m}function r0(t,n,e,i,r){if(Eg(i,e,t,n,GC),Ia(i)){let o=e[X];Su(o,e,i),rg(o,i,e)}r!=null&&Sl(e,i)}function u(){let t=Ke(),n=At(),e=Dg(n);return t.firstCreatePass&&Og(t,e),Xp(e)&&Jp(),Qp(),e.classesWithoutHost!=null&&JM(e)&&Kh(t,e,te(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&eS(e)&&Kh(t,e,te(),e.stylesWithoutHost,!1),u}function re(t,n,e,i){return m(t,n,e,i),u(),re}function lt(t,n,e,i){let r=te(),o=r[X],a=t+Qe,s=o.firstCreatePass?OA(a,o,2,n,e,i):o.data[a];return Eg(s,r,t,n,GC),i!=null&&Sl(r,s),lt}function Rt(){let t=At(),n=Dg(t);return Xp(n)&&Jp(),Qp(),Rt}function _n(t,n,e,i){return lt(t,n,e,i),Rt(),_n}var GC=(t,n,e,i,r)=>(ml(!0),H0(n[$e],i,sh()));function kl(t,n,e){let i=te(),r=i[X],o=t+Qe,a=r.firstCreatePass?kg(o,i,8,"ng-container",wg,Ud(),n,e):r.data[o];if(Eg(a,i,t,"ng-container",xk),Ia(a)){let s=i[X];Su(s,i,a),rg(s,a,i)}return e!=null&&Sl(i,a),kl}function Ol(){let t=Ke(),n=At(),e=Dg(n);return t.firstCreatePass&&Og(t,e),Ol}function Va(t,n,e){return kl(t,n,e),Ol(),Va}var xk=(t,n,e,i,r)=>(ml(!0),QS(n[$e],""));function qe(){return te()}function bn(t,n,e){let i=te(),r=Ta();if(di(i,r,n)){let o=Ke(),a=dl();aC(a,i,t,n,i[$e],e)}return bn}var Rl="en-US";var wk=Rl;function WC(t){typeof t=="string"&&(wk=t.toLowerCase().replace(/_/g,"-"))}function I(t,n,e){let i=te(),r=Ke(),o=At();return Ek(r,i,i[$e],o,t,n,e),I}function Lu(t,n,e){let i=te(),r=Ke(),o=At();return(o.type&3||e)&&IC(o,r,i,e,i[$e],t,n,ru(o,i,n)),Lu}function Ek(t,n,e,i,r,o,a){let s=!0,l=null;if((i.type&3||a)&&(l??=ru(i,n,o),IC(i,t,n,a,e,r,o,l)&&(s=!1)),s){let c=i.outputs?.[r],f=i.hostDirectiveOutputs?.[r];if(f&&f.length)for(let g=0;g<f.length;g+=2){let v=f[g],b=f[g+1];l??=ru(i,n,o),Ky(i,n,v,b,r,l)}if(c&&c.length)for(let g of c)l??=ru(i,n,o),Ky(i,n,g,r,r,l)}}function T(t=1){return yy(t)}function Dk(t,n){let e=null,i=cT(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?Z0(t,o,!0):mT(i,o))return r}return e}function Ce(t){let n=te()[gn][an];if(!n.projection){let e=t?t.length:1,i=n.projection=Gb(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?Dk(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function H(t,n=0,e,i,r,o){let a=te(),s=Ke(),l=i?t+1:null;l!==null&&Ou(a,s,l,i,r,o,null,e);let c=ja(s,Qe+t,16,null,e||null);c.projection===null&&(c.projection=n),nh();let g=!a[xa]||Kp();a[gn][an].projection[c.projection]===null&&l!==null?Ik(a,s,l):g&&!xu(c)&&FT(s,a,c)}function Ik(t,n,e){let i=Qe+e,r=n.data[i],o=t[i],a=Bh(o,r.tView.ssrId),s=Sg(t,r,void 0,{dehydratedView:a});Ag(o,s,0,uu(r,a))}function it(t,n,e,i){return NC(t,n,e,i),it}function Ge(t,n,e){return RC(t,n,e),Ge}function $(t){let n=te(),e=Ke(),i=$d();cl(i+1);let r=Pg(e,i);if(t.dirty&&iy(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=PC(n,i);t.reset(o,D0),t.notifyOnChanges()}return!0}return!1}function q(){return Fg(te(),$d())}function ju(t,n,e,i,r){return jC(n,NC(t,e,i,r)),ju}function Vu(t,n,e,i){return jC(t,RC(n,e,i)),Vu}function Bu(t=1){cl($d()+t)}function yt(t){let n=uy();return zp(n,Qe+t)}function Jd(t,n){return t<<17|n<<2}function Bo(t){return t>>17&32767}function Mk(t){return(t&2)==2}function Sk(t,n){return t&131071|n<<17}function Xh(t){return t|2}function Pa(t){return(t&131068)>>2}function bh(t,n){return t&-131069|n<<2}function Tk(t){return(t&1)===1}function Jh(t){return t|1}function Ak(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=Bo(a),l=Pa(a);t[i]=e;let c=!1,f;if(Array.isArray(e)){let g=e;f=g[1],(f===null||ya(g,f)>0)&&(c=!0)}else f=e;if(r)if(l!==0){let v=Bo(t[s+1]);t[i+1]=Jd(v,s),v!==0&&(t[v+1]=bh(t[v+1],i)),t[s+1]=Sk(t[s+1],i)}else t[i+1]=Jd(s,0),s!==0&&(t[s+1]=bh(t[s+1],i)),s=i;else t[i+1]=Jd(l,0),s===0?s=i:t[l+1]=bh(t[l+1],i),l=i;c&&(t[i+1]=Xh(t[i+1])),o0(t,f,i,!0),o0(t,f,i,!1),kk(n,f,t,i,o),a=Jd(s,l),o?n.classBindings=a:n.styleBindings=a}function kk(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&ya(o,n)>=0&&(e[i+1]=Jh(e[i+1]))}function o0(t,n,e,i){let r=t[e+1],o=n===null,a=i?Bo(r):Pa(r),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];Ok(l,n)&&(s=!0,t[a+1]=i?Jh(c):Xh(c)),a=i?Bo(c):Pa(c)}s&&(t[e+1]=i?Xh(r):Jh(r))}function Ok(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?ya(t,n)>=0:!1}var si={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function Rk(t){return t.substring(si.key,si.keyEnd)}function Nk(t){return Fk(t),YC(t,ZC(t,0,si.textEnd))}function YC(t,n){let e=si.textEnd;return e===n?-1:(n=si.keyEnd=Pk(t,si.key=n,e),ZC(t,n,e))}function Fk(t){si.key=0,si.keyEnd=0,si.value=0,si.valueEnd=0,si.textEnd=t.length}function ZC(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function Pk(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function On(t,n,e){return QC(t,n,e,!1),On}function R(t,n){return QC(t,n,null,!0),R}function ln(t){jk($k,Lk,t,!0)}function Lk(t,n){for(let e=Nk(n);e>=0;e=YC(n,e))Pd(t,Rk(n),!0)}function QC(t,n,e,i){let r=te(),o=Ke(),a=ll(2);if(o.firstUpdatePass&&XC(o,t,a,i),n!==Ot&&di(r,a,n)){let s=o.data[oi()];JC(o,s,r,r[$e],t,r[a+1]=Gk(n,e),i,a)}}function jk(t,n,e,i){let r=Ke(),o=ll(2);r.firstUpdatePass&&XC(r,null,o,i);let a=te();if(e!==Ot&&di(a,o,e)){let s=r.data[oi()];if(ex(s,i)&&!KC(r,o)){let l=i?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=Ad(l,e||"")),Kh(r,s,a,e,i)}else qk(r,s,a,a[$e],a[o+1],a[o+1]=zk(t,n,e),i,o)}}function KC(t,n){return n>=t.expandoStartIndex}function XC(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[oi()],a=KC(t,e);ex(o,i)&&n===null&&!a&&(n=!1),n=Vk(r,o,n,i),Ak(r,o,n,e,a,i)}}function Vk(t,n,e,i){let r=gy(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=yh(null,t,n,e,i),e=xl(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=yh(r,t,n,e,i),o===null){let l=Bk(t,n,i);l!==void 0&&Array.isArray(l)&&(l=yh(null,t,n,l[1],i),l=xl(l,n.attrs,i),Hk(t,n,i,l))}else o=Uk(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function Bk(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Pa(i)!==0)return t[Bo(i)]}function Hk(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Bo(r)]=i}function Uk(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=xl(i,a,e)}return xl(i,n.attrs,e)}function yh(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=xl(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function xl(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Pd(t,a,e?!0:n[++o]))}return t===void 0?null:t}function zk(t,n,e){if(e==null||e==="")return qt;let i=[],r=ui(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function $k(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Pd(t,i,e)}function qk(t,n,e,i,r,o,a,s){r===Ot&&(r=qt);let l=0,c=0,f=0<r.length?r[0]:null,g=0<o.length?o[0]:null;for(;f!==null||g!==null;){let v=l<r.length?r[l+1]:void 0,b=c<o.length?o[c+1]:void 0,w=null,j;f===g?(l+=2,c+=2,v!==b&&(w=g,j=b)):g===null||f!==null&&f<g?(l+=2,w=f):(c+=2,w=g,j=b),w!==null&&JC(t,n,e,i,w,j,a,s),f=l<r.length?r[l]:null,g=c<o.length?o[c]:null}}function JC(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],f=Tk(c)?a0(l,n,e,r,Pa(c),a):void 0;if(!bu(f)){bu(o)||Mk(c)&&(o=a0(l,null,e,r,s,a));let g=Up(oi(),e);LT(i,a,g,r,o)}}function a0(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let l=t[r],c=Array.isArray(l),f=c?l[1]:l,g=f===null,v=e[r+1];v===Ot&&(v=g?qt:void 0);let b=g?Ld(v,i):f===i?v:void 0;if(c&&!bu(b)&&(b=Ld(l,i)),bu(b)&&(s=b,a))return s;let w=t[r+1];r=a?Bo(w):Pa(w)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=Ld(l,i))}return s}function bu(t){return t!==void 0}function Gk(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=Js(ui(t)))),t}function ex(t,n){return(t.flags&(n?8:16))!==0}function p(t,n=""){let e=te(),i=Ke(),r=t+Qe,o=i.firstCreatePass?ja(i,r,1,n,null):i.data[r],a=Wk(i,e,o,n);e[r]=a,Wd()&&Cg(i,e,a,o),Sa(o,!1)}var Wk=(t,n,e,i)=>(ml(!0),YS(n[$e],i));function Yk(t,n,e,i=""){return di(t,Ta(),e)?n+Wi(e)+i:Ot}function Zk(t,n,e,i,r,o=""){let a=rh(),s=DC(t,a,e,r);return ll(2),s?n+Wi(e)+i+Wi(r)+o:Ot}function Qk(t,n,e,i,r,o,a,s=""){let l=rh(),c=RA(t,l,e,r,a);return ll(3),c?n+Wi(e)+i+Wi(r)+o+Wi(a)+s:Ot}function z(t){return K("",t),z}function K(t,n,e){let i=te(),r=Yk(i,t,n,e);return r!==Ot&&$g(i,oi(),r),K}function ct(t,n,e,i,r){let o=te(),a=Zk(o,t,n,e,i,r);return a!==Ot&&$g(o,oi(),a),ct}function Hu(t,n,e,i,r,o,a){let s=te(),l=Qk(s,t,n,e,i,r,o,a);return l!==Ot&&$g(s,oi(),l),Hu}function $g(t,n,e){let i=Up(n,t);ZS(t[$e],i,e)}function Ba(t,n,e){let i=sl()+t,r=te();return r[i]===Ot?Ng(r,i,n(e,r)):EC(r,i)}function s0(t,n,e){let i=Ke();i.firstCreatePass&&tx(n,i.data,i.blueprint,ii(t),e)}function tx(t,n,e,i,r){if(t=bt(t),Array.isArray(t))for(let o=0;o<t.length;o++)tx(t[o],n,e,i,r);else{let o=Ke(),a=te(),s=At(),l=So(t)?t:bt(t.provide),c=Lp(t),f=s.providerIndexes&1048575,g=s.directiveStart,v=s.providerIndexes>>20;if(So(t)||!t.multi){let b=new jo(c,r,D,null),w=xh(l,n,r?f:f+v,g);w===-1?(Eh(cu(s,a),o,l),Ch(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(b),a.push(b)):(e[w]=b,a[w]=b)}else{let b=xh(l,n,f+v,g),w=xh(l,n,f,f+v),j=b>=0&&e[b],U=w>=0&&e[w];if(r&&!U||!r&&!j){Eh(cu(s,a),o,l);let Y=Jk(r?Xk:Kk,e.length,r,i,c,t);!r&&U&&(e[w].providerFactory=Y),Ch(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(Y),a.push(Y)}else{let Y=nx(e[r?w:b],c,!r&&i);Ch(o,t,b>-1?b:w,Y)}!r&&i&&U&&e[w].componentProviders++}}}function Ch(t,n,e,i){let r=So(n),o=Xb(n);if(r||o){let l=(o?bt(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let f=c.indexOf(e);f===-1?c.push(e,[i,l]):c[f+1].push(i,l)}else c.push(e,l)}}}function nx(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function xh(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function Kk(t,n,e,i,r){return eg(this.multi,[])}function Xk(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=_l(i,i[X],this.providerFactory.index,r);a=l.slice(0,s),eg(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],eg(o,a);return a}function eg(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function Jk(t,n,e,i,r,o){let a=new jo(t,e,D,null);return a.multi=[],a.index=n,a.componentProviders=0,nx(a,r,i&&!e),a}function xe(t,n){return e=>{e.providersResolver=(i,r)=>s0(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>s0(i,r?r(n):n,!0))}}function qg(t,n){let e=sl()+t,i=te();return i[e]===Ot?Ng(i,e,n()):EC(i,e)}function Gg(t,n,e){return ix(te(),sl(),t,n,e)}function eO(t,n){let e=t[n];return e===Ot?void 0:e}function ix(t,n,e,i,r,o){let a=n+e;return di(t,a,r)?Ng(t,a+1,o?i.call(o,r):i(r)):eO(t,a+1)}function rr(t,n){let e=Ke(),i,r=t+Qe;e.firstCreatePass?(i=tO(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=Rr(i.type,!0)),a,s=rn(D);try{let l=lu(!1),c=o();return lu(l),$p(e,te(),r,c),c}finally{rn(s)}}function tO(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function or(t,n,e){let i=t+Qe,r=te(),o=zp(r,i);return nO(r,i)?ix(r,sl(),n,o.transform,e,o):o.transform(e)}function nO(t,n){return t[X].data[n].pure}function Ha(t,n){return Tu(t,n)}var yu=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},Wg=(()=>{class t{compileModuleSync(e){return new _u(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=Tp(e),o=W0(r.declarations).reduce((a,s)=>{let l=Gi(s);return l&&a.push(new Vo(l)),a},[]);return new yu(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var rx=(()=>{class t{applicationErrorHandler=d(vn);appRef=d(kn);taskService=d(Ki);ngZone=d(F);zonelessEnabled=d(fl);tracing=d(mi,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new fe;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ks):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(fh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Ey:ch;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ks+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ox(){return[{provide:Si,useExisting:rx},{provide:F,useClass:Xs},{provide:fl,useValue:!0}]}function iO(){return typeof $localize<"u"&&$localize.locale||Rl}var Uu=new y("",{factory:()=>d(Uu,{optional:!0,skipSelf:!0})||iO()});function De(t){return Nb(t)}function cn(t,n){return Ls(t,n?.equal)}var rO=t=>t;function Yg(t,n){if(typeof t=="function"){let e=up(t,rO,n?.equal);return ax(e,n?.debugName)}else{let e=up(t.source,t.computation,t.equal);return ax(e,t.debugName)}}function ax(t,n){let e=t[tt],i=t;return i.set=r=>Ob(e,r),i.update=r=>Rb(e,r),i.asReadonly=Yd.bind(t),i}var px=Symbol("InputSignalNode#UNSET"),CO=ee(C({},js),{transformFn:void 0,applyValueToInputSignal(t,n){ho(t,n)}});function hx(t,n){let e=Object.create(CO);e.value=t,e.transformFn=n?.transform;function i(){if(Ir(e),e.value===px){let r=null;throw new k(-950,r)}return e.value}return i[tt]=e,i}var Ni=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>El(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function sx(t,n){return hx(t,n)}function xO(t){return hx(px,t)}var gx=(sx.required=xO,sx);function lx(t,n){return Lg(n)}function wO(t,n){return jg(n)}var Fl=(lx.required=wO,lx);function cx(t,n){return Lg(n)}function EO(t,n){return jg(n)}var vx=(cx.required=EO,cx);var Qg=new y(""),DO=new y("");function Nl(t){return!t.moduleRef}function IO(t){let n=Nl(t)?t.r3Injector:t.moduleRef.injector,e=n.get(F);return e.run(()=>{Nl(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(vn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Nl(t)){let o=()=>n.destroy(),a=t.platformInjector.get(Qg);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(Qg);a.add(o),t.moduleRef.onDestroy(()=>{vl(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return SO(i,e,()=>{let o=n.get(Ki),a=o.add(),s=n.get(Ug);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(Uu,Rl);if(WC(l||Rl),!n.get(DO,!0))return Nl(t)?n.get(kn):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Nl(t)){let f=n.get(kn);return t.rootComponent!==void 0&&f.bootstrap(t.rootComponent),f}else return MO?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var MO;function SO(t,n,e){try{let i=e();return ir(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var zu=null;function TO(t=[],n){return W.create({name:n,providers:[{provide:rl,useValue:"platform"},{provide:Qg,useValue:new Set([()=>zu=null])},...t]})}function AO(t=[]){if(zu)return zu;let n=TO(t);return zu=n,$C(),kO(n),n}function kO(t){let n=t.get(Cu,null);Tt(t,()=>{n?.forEach(e=>e())})}var OO=1e4;var o9=OO-1e3;var ge=(()=>{class t{static __NG_ELEMENT_ID__=RO}return t})();function RO(t){return NO(At(),te(),(t&16)===16)}function NO(t,n,e){if(Qi(t)&&!e){let i=Hn(t.index,n);return new zr(i,i)}else if(t.type&175){let i=n[gn];return new zr(i,n)}return null}var Kg=class{supports(n){return Rg(n)}create(n){return new Xg(n)}},FO=(t,n)=>n,Xg=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||FO}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let a=!i||e&&e.currentIndex<dx(i,r,o)?e:i,s=dx(a,r,o),l=a.currentIndex;if(a===i)r--,i=i._nextRemoved;else if(e=e._next,a.previousIndex==null)r++;else{o||(o=[]);let c=s-r,f=l-r;if(c!=f){for(let v=0;v<c;v++){let b=v<o.length?o[v]:o[v]=0,w=b+v;f<=w&&w<c&&(o[v]=b+1)}let g=a.previousIndex;o[g]=f-c}}s!==l&&n(a,s,l)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!Rg(n))throw new k(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,a;if(Array.isArray(n)){this.length=n.length;for(let s=0;s<this.length;s++)o=n[s],a=this._trackByFn(s,o),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,o,a,s),i=!0):(i&&(e=this._verifyReinsertion(e,o,a,s)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,xC(n,s=>{a=this._trackByFn(r,s),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,s,a,r),i=!0):(i&&(e=this._verifyReinsertion(e,s,a,r)),Object.is(e.item,s)||this._addIdentityChange(e,s)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new Jg(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new $u),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new $u),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},Jg=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},ev=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},$u=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new ev,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function dx(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function ux(){return new qu([new Kg])}var qu=(()=>{class t{factories;static \u0275prov=x({token:t,providedIn:"root",factory:ux});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=d(t,{optional:!0,skipSelf:!0});return t.create(e,i||ux())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new k(901,!1)}}return t})();function _x(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Ne(Ie.BootstrapApplicationStart);try{let o=r?.injector??AO(i),a=[ox(),Iy,...e||[]],s=new Cl({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return IO({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Ne(Ie.BootstrapApplicationEnd)}}function J(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function pi(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Zg=Symbol("NOT_SET"),bx=new Set,PO=ee(C({},js),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Zg,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Zg&&!ca(this))return this.signal;try{for(let r of this.cleanup??bx)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=Hi(this),i;try{i=this.userFn.apply(null,n)}finally{Mr(this,e)}return(this.value===Zg||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),tv=class extends bl{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(sn),a),this.scheduler=r;for(let s of vg){let l=e[s];if(l===void 0)continue;let c=Object.create(PO);c.sequence=this,c.phase=s,c.userFn=l,c.dirty=!0,c.signal=()=>(Ir(c),c.value),c.signal[tt]=c,c.registerCleanupFn=f=>(c.cleanup??=new Set).add(f),this.nodes[s]=c,this.hooks[s]=f=>c.phaseFn(f)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??bx)e()}finally{Sr(n)}}};function yx(t,n){let e=n?.injector??d(W),i=e.get(Si),r=e.get(Iu),o=e.get(mi,null,{optional:!0});r.impl??=e.get(_g);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(Aa,null,{optional:!0}),l=new tv(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function Gu(t,n){let e=Gi(t),i=n.elementInjector||Ca();return new Vo(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var Cx=null;function Un(){return Cx}function nv(t){Cx??=t}var Pl=class{},Ua=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(xx),providedIn:"platform"})}return t})();var xx=(()=>{class t extends Ua{_location;_history;_doc=d(G);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Un().getBaseHref(this._doc)}onPopState(e){let i=Un().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Un().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function Dx(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function wx(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function qr(t){return t&&t[0]!=="?"?`?${t}`:t}var za=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(jO),providedIn:"root"})}return t})(),LO=new y(""),jO=(()=>{class t extends za{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(G).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return Dx(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+qr(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+qr(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+qr(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(Q(Ua),Q(LO,8))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Gr=(()=>{class t{_subject=new E;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=HO(wx(Ex(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+qr(i))}normalize(e){return t.stripTrailingSlash(BO(this._basePath,Ex(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+qr(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+qr(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=qr;static joinWithSlash=Dx;static stripTrailingSlash=wx;static \u0275fac=function(i){return new(i||t)(Q(za))};static \u0275prov=x({token:t,factory:()=>VO(),providedIn:"root"})}return t})();function VO(){return new Gr(Q(za))}function BO(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function Ex(t){return t.replace(/\/index\.html$/,"")}function HO(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Wu=class{$implicit;ngForOf;index;count;constructor(n,e,i,r){this.$implicit=n,this.ngForOf=e,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},dt=(()=>{class t{_viewContainer;_template;_differs;set ngForOf(e){this._ngForOf=e,this._ngForOfDirty=!0}set ngForTrackBy(e){this._trackByFn=e}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(e,i,r){this._viewContainer=e,this._template=i,this._differs=r}set ngForTemplate(e){e&&(this._template=e)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let e=this._ngForOf;!this._differ&&e&&(this._differ=this._differs.find(e).create(this.ngForTrackBy))}if(this._differ){let e=this._differ.diff(this._ngForOf);e&&this._applyChanges(e)}}_applyChanges(e){let i=this._viewContainer;e.forEachOperation((r,o,a)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new Wu(r.item,this._ngForOf,-1,-1),a===null?void 0:a);else if(a==null)i.remove(o===null?void 0:o);else if(o!==null){let s=i.get(o);i.move(s,a),Ix(s,r)}});for(let r=0,o=i.length;r<o;r++){let s=i.get(r).context;s.index=r,s.count=o,s.ngForOf=this._ngForOf}e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);Ix(o,r)})}static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(D(nt),D(kt),D(qu))};static \u0275dir=S({type:t,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return t})();function Ix(t,n){t.context.$implicit=n.item}var Ct=(()=>{class t{_viewContainer;_context=new Yu;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){Mx(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){Mx(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(D(nt),D(kt))};static \u0275dir=S({type:t,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return t})(),Yu=class{$implicit=null;ngIf=null};function Mx(t,n){if(t&&!t.createEmbeddedView)throw new k(2020,!1)}var Ll=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(W);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(D(nt))};static \u0275dir=S({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[je]})}return t})();function UO(t,n){return new k(2100,!1)}var iv=class{createSubscription(n,e,i){return De(()=>n.subscribe({next:e,error:i}))}dispose(n){De(()=>n.unsubscribe())}},rv=class{createSubscription(n,e,i){return n.then(r=>e?.(r),r=>i?.(r)),{unsubscribe:()=>{e=null,i=null}}}dispose(n){n.unsubscribe()}},zO=new rv,$O=new iv,jl=(()=>{class t{_ref;_latestValue=null;markForCheckOnValueUpdate=!0;_subscription=null;_obj=null;_strategy=null;applicationErrorHandler=d(vn);constructor(e){this._ref=e}ngOnDestroy(){this._subscription&&this._dispose(),this._ref=null}transform(e){if(!this._obj){if(e)try{this.markForCheckOnValueUpdate=!1,this._subscribe(e)}finally{this.markForCheckOnValueUpdate=!0}return this._latestValue}return e!==this._obj?(this._dispose(),this.transform(e)):this._latestValue}_subscribe(e){this._obj=e,this._strategy=this._selectStrategy(e),this._subscription=this._strategy.createSubscription(e,i=>this._updateLatestValue(e,i),i=>this.applicationErrorHandler(i))}_selectStrategy(e){if(ir(e))return zO;if(Fu(e))return $O;throw UO(t,e)}_dispose(){this._strategy.dispose(this._subscription),this._latestValue=null,this._subscription=null,this._obj=null}_updateLatestValue(e,i){e===this._obj&&(this._latestValue=i,this.markForCheckOnValueUpdate&&this._ref?.markForCheck())}static \u0275fac=function(i){return new(i||t)(D(ge,16))};static \u0275pipe=Vg({name:"async",type:t,pure:!1})}return t})();var ut=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({})}return t})();function Vl(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var $o=class{};var ov="browser";function Sx(t){return t===ov}var Bl=class{_doc;constructor(n){this._doc=n}manager},Zu=(()=>{class t extends Bl{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(Q(G))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),Xu=new y(""),cv=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof Zu));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof Zu);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new k(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(Q(Xu),Q(F))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),av="ng-app-id";function Tx(t){for(let n of t)n.remove()}function Ax(t,n){let e=n.createElement("style");return e.textContent=t,e}function qO(t,n,e,i){let r=t.head?.querySelectorAll(`style[${av}="${n}"],link[${av}="${n}"]`);if(r)for(let o of r)o.removeAttribute(av),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function lv(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var dv=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,qO(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,Ax);i?.forEach(r=>this.addUsage(r,this.external,lv))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(Tx(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])Tx(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,Ax(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,lv(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(Q(G),Q($r),Q(Uo,8),Q(Ho))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),sv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},uv=/%COMP%/g;var Ox="%COMP%",GO=`_nghost-${Ox}`,WO=`_ngcontent-${Ox}`,YO=!0,ZO=new y("",{factory:()=>YO});function QO(t){return WO.replace(uv,t)}function KO(t){return GO.replace(uv,t)}function Rx(t,n){return n.map(e=>e.replace(uv,t))}var mv=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,a,s,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.defaultRenderer=new Hl(e,a,s,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Ku?r.applyToHost(e):r instanceof Ul&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,f=this.removeStylesOnCompDestroy,g=this.tracingService;switch(i.encapsulation){case ci.Emulated:o=new Ku(l,c,i,this.appId,f,a,s,g);break;case ci.ShadowDom:return new Qu(l,e,i,a,s,this.nonce,g,c);case ci.ExperimentalIsolatedShadowDom:return new Qu(l,e,i,a,s,this.nonce,g);default:o=new Ul(l,c,i,f,a,s,g);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(Q(cv),Q(dv),Q($r),Q(ZO),Q(G),Q(F),Q(Uo),Q(mi,8))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),Hl=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(sv[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(kx(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(kx(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new k(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=sv[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=sv[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Oi.DashCase|Oi.Important)?n.style.setProperty(e,i,r&Oi.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Oi.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=Un().getGlobalEventTarget(this.doc,n),!n))throw new k(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function kx(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Qu=class extends Hl{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,l){super(n,r,o,s),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=Rx(i.id,c);for(let g of c){let v=document.createElement("style");a&&v.setAttribute("nonce",a),v.textContent=g,this.shadowRoot.appendChild(v)}let f=i.getExternalStyles?.();if(f)for(let g of f){let v=lv(g,r);a&&v.setAttribute("nonce",a),this.shadowRoot.appendChild(v)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Ul=class extends Hl{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,l){super(n,o,a,s),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?Rx(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Fa.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Ku=class extends Ul{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,l){let c=r+"-"+i.id;super(n,e,i,o,a,s,l,c),this.contentAttr=QO(c),this.hostAttr=KO(c)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Ju=class t extends Pl{supportsDOMEvents=!0;static makeCurrent(){nv(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=XO();return e==null?null:JO(e)}resetBaseElement(){zl=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Vl(document.cookie,n)}},zl=null;function XO(){return zl=zl||document.head.querySelector("base"),zl?zl.getAttribute("href"):null}function JO(t){return new URL(t,document.baseURI).pathname}var e1=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})(),Nx=["alt","control","meta","shift"],t1={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},n1={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},Fx=(()=>{class t extends Bl{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Un().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),Nx.forEach(c=>{let f=i.indexOf(c);f>-1&&(i.splice(f,1),a+=c+".")}),a+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=a,l}static matchEventFullKeyCode(e,i){let r=t1[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Nx.forEach(a=>{if(a!==r){let s=n1[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(Q(G))};static \u0275prov=x({token:t,factory:t.\u0275fac})}return t})();async function fv(t,n,e){let i=C({rootComponent:t},i1(n,e));return _x(i)}function i1(t,n){return{platformRef:n?.platformRef,appProviders:[...l1,...t?.providers??[]],platformProviders:s1}}function r1(){Ju.makeCurrent()}function o1(){return new on}function a1(){return ig(document),document}var s1=[{provide:Ho,useValue:ov},{provide:Cu,useValue:r1,multi:!0},{provide:G,useFactory:a1}];var l1=[{provide:rl,useValue:"root"},{provide:on,useFactory:o1},{provide:Xu,useClass:Zu,multi:!0},{provide:Xu,useClass:Fx,multi:!0},mv,dv,cv,{provide:at,useExisting:mv},{provide:$o,useClass:e1},[]];var Wr=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init();for(let[e,i]of n.headers.entries())this.headers.set(e,i),this.normalizedNames.set(e,n.normalizedNames.get(e))}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=n.op==="a"?(this.headers.get(e)||[]).slice():[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(o===void 0)this.headers.delete(e),this.normalizedNames.delete(e);else{let a=Array.isArray(o)?o:[o],s=this.headers.get(e);if(!s)return;s=s.filter(l=>a.indexOf(l)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var tm=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},nm=class{encodeKey(n){return Px(n)}encodeValue(n){return Px(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function c1(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[a,s]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(a)||[];l.push(s),e.set(a,l)}),e}var d1=/%(\d[a-f0-9])/gi,u1={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function Px(t){return encodeURIComponent(t).replace(d1,(n,e)=>u1[e]??n)}function em(t){return`${t}`}var ar=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new nm,n.fromString){if(n.fromObject)throw new k(2805,!1);this.map=c1(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(em):[em(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){if(this.map===null&&(this.map=new Map),this.cloneFrom!==null){this.cloneFrom.init();for(let[n,e]of this.cloneFrom.map.entries())this.map.set(n,e);this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=n.op==="a"?(this.map.get(n.param)||[]).slice():[];e.push(em(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=(this.map.get(n.param)||[]).slice(),r=i.indexOf(em(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null}}};function m1(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function Lx(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function jx(t){return typeof Blob<"u"&&t instanceof Blob}function Vx(t){return typeof FormData<"u"&&t instanceof FormData}function f1(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var Bx="Content-Type",Hx="Accept",Ux="text/plain",zx="application/json",p1=`${zx}, ${Ux}, */*`,$a=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(m1(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new k(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Wr,this.context??=new tm,!this.params)this.params=new ar,this.urlWithParams=e;else{let a=this.params.toString();if(a.length===0)this.urlWithParams=e;else{let s=e.indexOf("?"),l=s===-1?"?":s<e.length-1?"&":"";this.urlWithParams=e+l+a}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||Lx(this.body)||jx(this.body)||Vx(this.body)||f1(this.body)?this.body:this.body instanceof ar?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||Vx(this.body)?null:jx(this.body)?this.body.type||null:Lx(this.body)?null:typeof this.body=="string"?Ux:this.body instanceof ar?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?zx:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,a=n.priority||this.priority,s=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,f=n.credentials||this.credentials,g=n.referrer??this.referrer,v=n.integrity||this.integrity,b=n.referrerPolicy||this.referrerPolicy,w=n.transferCache??this.transferCache,j=n.timeout??this.timeout,U=n.body!==void 0?n.body:this.body,Y=n.withCredentials??this.withCredentials,Ue=n.reportProgress??this.reportProgress,fn=n.headers||this.headers,It=n.params||this.params,Rs=n.context??this.context;return n.setHeaders!==void 0&&(fn=Object.keys(n.setHeaders).reduce((Ns,uo)=>Ns.set(uo,n.setHeaders[uo]),fn)),n.setParams&&(It=Object.keys(n.setParams).reduce((Ns,uo)=>Ns.set(uo,n.setParams[uo]),It)),new t(e,i,U,{params:It,headers:fn,context:Rs,reportProgress:Ue,responseType:r,withCredentials:Y,transferCache:w,keepalive:o,cache:s,priority:a,timeout:j,mode:l,redirect:c,credentials:f,referrer:g,integrity:v,referrerPolicy:b})}},qo=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(qo||{}),Ga=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new Wr,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},im=class t extends Ga{constructor(n={}){super(n)}type=qo.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},$l=class t extends Ga{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=qo.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},qa=class extends Ga{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},h1=200,g1=204;var v1=new y("");var _1=/^\)\]\}',?\n/;var hv=(()=>{class t{xhrFactory;tracingService=d(mi,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new k(-2800,!1);let i=this.xhrFactory;return Z(null).pipe(Ze(()=>new ie(o=>{let a=i.build();if(a.open(e.method,e.urlWithParams),e.withCredentials&&(a.withCredentials=!0),e.headers.forEach((U,Y)=>a.setRequestHeader(U,Y.join(","))),e.headers.has(Hx)||a.setRequestHeader(Hx,p1),!e.headers.has(Bx)){let U=e.detectContentTypeHeader();U!==null&&a.setRequestHeader(Bx,U)}if(e.timeout&&(a.timeout=e.timeout),e.responseType){let U=e.responseType.toLowerCase();a.responseType=U!=="json"?U:"text"}let s=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let U=a.statusText||"OK",Y=new Wr(a.getAllResponseHeaders()),Ue=a.responseURL||e.url;return l=new im({headers:Y,status:a.status,statusText:U,url:Ue}),l},f=this.maybePropagateTrace(()=>{let{headers:U,status:Y,statusText:Ue,url:fn}=c(),It=null;Y!==g1&&(It=typeof a.response>"u"?a.responseText:a.response),Y===0&&(Y=It?h1:0);let Rs=Y>=200&&Y<300;if(e.responseType==="json"&&typeof It=="string"){let Ns=It;It=It.replace(_1,"");try{It=It!==""?JSON.parse(It):null}catch(uo){It=Ns,Rs&&(Rs=!1,It={error:uo,text:It})}}Rs?(o.next(new $l({body:It,headers:U,status:Y,statusText:Ue,url:fn||void 0})),o.complete()):o.error(new qa({error:It,headers:U,status:Y,statusText:Ue,url:fn||void 0}))}),g=this.maybePropagateTrace(U=>{let{url:Y}=c(),Ue=new qa({error:U,status:a.status||0,statusText:a.statusText||"Unknown Error",url:Y||void 0});o.error(Ue)}),v=g;e.timeout&&(v=this.maybePropagateTrace(U=>{let{url:Y}=c(),Ue=new qa({error:new DOMException("Request timed out","TimeoutError"),status:a.status||0,statusText:a.statusText||"Request timeout",url:Y||void 0});o.error(Ue)}));let b=!1,w=this.maybePropagateTrace(U=>{b||(o.next(c()),b=!0);let Y={type:qo.DownloadProgress,loaded:U.loaded};U.lengthComputable&&(Y.total=U.total),e.responseType==="text"&&a.responseText&&(Y.partialText=a.responseText),o.next(Y)}),j=this.maybePropagateTrace(U=>{let Y={type:qo.UploadProgress,loaded:U.loaded};U.lengthComputable&&(Y.total=U.total),o.next(Y)});return a.addEventListener("load",f),a.addEventListener("error",g),a.addEventListener("timeout",v),a.addEventListener("abort",g),e.reportProgress&&(a.addEventListener("progress",w),s!==null&&a.upload&&a.upload.addEventListener("progress",j)),a.send(s),o.next({type:qo.Sent}),()=>{a.removeEventListener("error",g),a.removeEventListener("abort",g),a.removeEventListener("load",f),a.removeEventListener("timeout",v),e.reportProgress&&(a.removeEventListener("progress",w),s!==null&&a.upload&&a.upload.removeEventListener("progress",j)),a.readyState!==a.DONE&&a.abort()}})))}static \u0275fac=function(i){return new(i||t)(Q($o))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),b1=new y("",{factory:()=>!0}),y1="XSRF-TOKEN",C1=new y("",{factory:()=>y1}),x1="X-XSRF-TOKEN",w1=new y("",{factory:()=>x1}),E1=(()=>{class t{cookieName=d(C1);doc=d(G);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=Vl(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),$x=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=Q(E1),r},providedIn:"root"})}return t})();function qx(t,n){if(!d(b1)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(Ua).href,{origin:o}=new URL(r),{origin:a}=new URL(t.url,o);if(o!==a)return n(t)}catch{return n(t)}let e=d($x).getToken(),i=d(w1);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}function D1(t,n){return n(t)}function I1(t,n,e){return(i,r)=>Tt(e,()=>n(i,o=>t(o,r)))}var Gx=new y("",{factory:()=>[qx]}),Wx=new y(""),Yx=new y("",{factory:()=>!0});var gv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=Q(hv),r},providedIn:"root"})}return t})();var rm=(()=>{class t{backend;injector;chain=null;pendingTasks=d(pl);contributeToStability=d(Yx);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=this.injector.get(om,null,{skipSelf:!0}),r=i!==null&&this.backend===i,o=this.injector.get(Wx,[],r?{self:!0}:void 0),a=Array.from(new Set([...this.injector.get(Gx),...o]));this.chain=a.reduceRight((s,l)=>I1(s,l,this.injector),D1)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Do(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(Q(gv),Q(Ae))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),om=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=Q(rm),r},providedIn:"root"})}return t})();function pv(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var Je=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof $a)o=e;else{let l;r.headers instanceof Wr?l=r.headers:l=new Wr(r.headers);let c;r.params&&(r.params instanceof ar?c=r.params:c=new ar({fromObject:r.params})),o=new $a(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let a=Z(o).pipe(Eo(l=>this.handler.handle(l)));if(e instanceof $a||r.observe==="events")return a;let s=a.pipe(ae(l=>l instanceof $l));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return s.pipe(V(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new k(2806,!1);return l.body}));case"blob":return s.pipe(V(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new k(2807,!1);return l.body}));case"text":return s.pipe(V(l=>{if(l.body!==null&&typeof l.body!="string")throw new k(2808,!1);return l.body}));default:return s.pipe(V(l=>l.body))}case"response":return s;default:throw new k(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new ar().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,pv(r,i))}post(e,i,r={}){return this.request("POST",e,pv(r,i))}put(e,i,r={}){return this.request("PUT",e,pv(r,i))}static \u0275fac=function(i){return new(i||t)(Q(om))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function vv(...t){let n=[Je,rm,{provide:om,useExisting:rm},{provide:gv,useFactory:()=>d(v1,{optional:!0})??d(hv)},{provide:Gx,useValue:qx,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return Yi(n)}var Zx=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(Q(G))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ql=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=Q(M1),r},providedIn:"root"})}return t})(),M1=(()=>{class t extends ql{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case Te.NONE:return i;case Te.HTML:return er(i,"HTML")?ui(i):dg(this._doc,String(i)).toString();case Te.STYLE:return er(i,"Style")?ui(i):i;case Te.SCRIPT:if(er(i,"Script"))return ui(i);throw new k(5200,!1);case Te.URL:return er(i,"URL")?ui(i):Il(String(i));case Te.RESOURCE_URL:if(er(i,"ResourceURL"))return ui(i);throw new k(5201,!1);default:throw new k(5202,!1)}}bypassSecurityTrustHtml(e){return og(e)}bypassSecurityTrustStyle(e){return ag(e)}bypassSecurityTrustScript(e){return sg(e)}bypassSecurityTrustUrl(e){return lg(e)}bypassSecurityTrustResourceUrl(e){return cg(e)}static \u0275fac=function(i){return new(i||t)(Q(G))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ue="primary",ac=Symbol("RouteTitle"),wv=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function Wo(t){return new wv(t)}function bv(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function iw(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return bv(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),a=i.slice(r+1);if(o.length+a.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let s={};return!bv(o,t.slice(0,o.length),s)||!bv(a,t.slice(t.length-a.length),s)?null:{consumed:t,posParams:s}}function um(t){return new Promise((n,e)=>{t.pipe(zi()).subscribe({next:i=>n(i),error:i=>e(i)})})}function S1(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Pi(t[e],n[e]))return!1;return!0}function Pi(t,n){let e=t?Ev(t):void 0,i=n?Ev(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!rw(t[r],n[r]))return!1;return!0}function Ev(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function rw(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function T1(t){return t.length>0?t[t.length-1]:null}function Qo(t){return Us(t)?t:ir(t)?ze(Promise.resolve(t)):Z(t)}function ow(t){return Us(t)?um(t):Promise.resolve(t)}var A1={exact:sw,subset:lw},aw={exact:k1,subset:O1,ignored:()=>!0},jv={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Kl={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function Vv(t,n,e){let i=t instanceof dn?t:n.parseUrl(t);return cn(()=>Dv(n.lastSuccessfulNavigation()?.finalUrl??new dn,i,C(C({},Kl),e)))}function Dv(t,n,e){return A1[e.paths](t.root,n.root,e.matrixParams)&&aw[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function k1(t,n){return Pi(t,n)}function sw(t,n,e){if(!Go(t.segments,n.segments)||!lm(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!sw(t.children[i],n.children[i],e))return!1;return!0}function O1(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>rw(t[e],n[e]))}function lw(t,n,e){return cw(t,n,n.segments,e)}function cw(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Go(r,e)||n.hasChildren()||!lm(r,e,i))}else if(t.segments.length===e.length){if(!Go(t.segments,e)||!lm(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!lw(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Go(t.segments,r)||!lm(t.segments,r,i)||!t.children[ue]?!1:cw(t.children[ue],n,o,i)}}function lm(t,n,e){return n.every((i,r)=>aw[e](t[r].parameters,i.parameters))}var dn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new ke([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Wo(this.queryParams),this._queryParamMap}toString(){return F1.serialize(this)}},ke=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return cm(this)}},Yr=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=Wo(this.parameters),this._parameterMap}toString(){return uw(this)}};function R1(t,n){return Go(t,n)&&t.every((e,i)=>Pi(e.parameters,n[i].parameters))}function Go(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function N1(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===ue&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==ue&&(e=e.concat(n(r,i)))}),e}var ts=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>new Zr,providedIn:"root"})}return t})(),Zr=class{parse(n){let e=new Mv(n);return new dn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Gl(n.root,!0)}`,i=j1(n.queryParams),r=typeof n.fragment=="string"?`#${P1(n.fragment)}`:"";return`${e}${i}${r}`}},F1=new Zr;function cm(t){return t.segments.map(n=>uw(n)).join("/")}function Gl(t,n){if(!t.hasChildren())return cm(t);if(n){let e=t.children[ue]?Gl(t.children[ue],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==ue&&i.push(`${r}:${Gl(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=N1(t,(i,r)=>r===ue?[Gl(t.children[ue],!1)]:[`${r}:${Gl(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[ue]!=null?`${cm(t)}/${e[0]}`:`${cm(t)}/(${e.join("//")})`}}function dw(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function am(t){return dw(t).replace(/%3B/gi,";")}function P1(t){return encodeURI(t)}function Iv(t){return dw(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function dm(t){return decodeURIComponent(t)}function Kx(t){return dm(t.replace(/\+/g,"%20"))}function uw(t){return`${Iv(t.path)}${L1(t.parameters)}`}function L1(t){return Object.entries(t).map(([n,e])=>`;${Iv(n)}=${Iv(e)}`).join("")}function j1(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${am(e)}=${am(r)}`).join("&"):`${am(e)}=${am(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var V1=/^[^\/()?;#]+/;function yv(t){let n=t.match(V1);return n?n[0]:""}var B1=/^[^\/()?;=#]+/;function H1(t){let n=t.match(B1);return n?n[0]:""}var U1=/^[^=?&#]+/;function z1(t){let n=t.match(U1);return n?n[0]:""}var $1=/^[^&#]+/;function q1(t){let n=t.match($1);return n?n[0]:""}var Mv=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new ke([],{}):new ke([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new k(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[ue]=new ke(e,i)),r}parseSegment(){let n=yv(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new k(4009,!1);return this.capture(n),new Yr(dm(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=H1(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=yv(this.remaining);r&&(i=r,this.capture(i))}n[dm(e)]=dm(i)}parseQueryParam(n){let e=z1(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let a=q1(this.remaining);a&&(i=a,this.capture(i))}let r=Kx(e),o=Kx(i);if(n.hasOwnProperty(r)){let a=n[r];Array.isArray(a)||(a=[a],n[r]=a),a.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=yv(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new k(4010,!1);let a;r.indexOf(":")>-1?(a=r.slice(0,r.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=ue);let s=this.parseChildren(e+1);i[a??ue]=Object.keys(s).length===1&&s[ue]?s[ue]:new ke([],s),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new k(4011,!1)}};function mw(t){return t.segments.length>0?new ke([],{[ue]:t}):t}function fw(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=fw(r);if(i===ue&&o.segments.length===0&&o.hasChildren())for(let[a,s]of Object.entries(o.children))n[a]=s;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new ke(t.segments,n);return G1(e)}function G1(t){if(t.numberOfChildren===1&&t.children[ue]){let n=t.children[ue];return new ke(t.segments.concat(n.segments),n.children)}return t}function Qr(t){return t instanceof dn}function pw(t,n,e=null,i=null,r=new Zr){let o=hw(t);return gw(o,n,e,i,r)}function hw(t){let n;function e(o){let a={};for(let l of o.children){let c=e(l);a[l.outlet]=c}let s=new ke(o.url,a);return o===t&&(n=s),s}let i=e(t.root),r=mw(i);return n??r}function gw(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return Cv(o,o,o,e,i,r);let a=W1(n);if(a.toRoot())return Cv(o,o,new ke([],{}),e,i,r);let s=Y1(a,o,t),l=s.processChildren?Yl(s.segmentGroup,s.index,a.commands):_w(s.segmentGroup,s.index,a.commands);return Cv(o,s.segmentGroup,l,e,i,r)}function mm(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Xl(t){return typeof t=="object"&&t!=null&&t.outlets}function Xx(t,n,e){t||="\u0275";let i=new dn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function Cv(t,n,e,i,r,o){let a={};for(let[c,f]of Object.entries(i??{}))a[c]=Array.isArray(f)?f.map(g=>Xx(c,g,o)):Xx(c,f,o);let s;t===n?s=e:s=vw(t,n,e);let l=mw(fw(s));return new dn(l,a,r)}function vw(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=vw(o,n,e)}),new ke(t.segments,i)}var fm=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&mm(i[0]))throw new k(4003,!1);let r=i.find(Xl);if(r&&r!==T1(i))throw new k(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function W1(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new fm(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,a)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let s={};return Object.entries(o.outlets).forEach(([l,c])=>{s[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:s}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:a===0?(o.split("/").forEach((s,l)=>{l==0&&s==="."||(l==0&&s===""?e=!0:s===".."?n++:s!=""&&r.push(s))}),r):[...r,o]},[]);return new fm(e,n,i)}var Ya=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function Y1(t,n,e){if(t.isAbsolute)return new Ya(n,!0,0);if(!e)return new Ya(n,!1,NaN);if(e.parent===null)return new Ya(e,!0,0);let i=mm(t.commands[0])?0:1,r=e.segments.length-1+i;return Z1(e,r,t.numberOfDoubleDots)}function Z1(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new k(4005,!1);r=i.segments.length}return new Ya(i,!1,r-o)}function Q1(t){return Xl(t[0])?t[0].outlets:{[ue]:t}}function _w(t,n,e){if(t??=new ke([],{}),t.segments.length===0&&t.hasChildren())return Yl(t,n,e);let i=K1(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new ke(t.segments.slice(0,i.pathIndex),{});return o.children[ue]=new ke(t.segments.slice(i.pathIndex),t.children),Yl(o,0,r)}else return i.match&&r.length===0?new ke(t.segments,{}):i.match&&!t.hasChildren()?Sv(t,n,e):i.match?Yl(t,0,r):Sv(t,n,e)}function Yl(t,n,e){if(e.length===0)return new ke(t.segments,{});{let i=Q1(e),r={};if(Object.keys(i).some(o=>o!==ue)&&t.children[ue]&&t.numberOfChildren===1&&t.children[ue].segments.length===0){let o=Yl(t.children[ue],n,e);return new ke(t.segments,o.children)}return Object.entries(i).forEach(([o,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(r[o]=_w(t.children[o],n,a))}),Object.entries(t.children).forEach(([o,a])=>{i[o]===void 0&&(r[o]=a)}),new ke(t.segments,r)}}function K1(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let a=t.segments[r],s=e[i];if(Xl(s))break;let l=`${s}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!ew(l,c,a))return o;i+=2}else{if(!ew(l,{},a))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Sv(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Xl(o)){let l=X1(o.outlets);return new ke(i,l)}if(r===0&&mm(e[0])){let l=t.segments[n];i.push(new Yr(l.path,Jx(e[0]))),r++;continue}let a=Xl(o)?o.outlets[ue]:`${o}`,s=r<e.length-1?e[r+1]:null;a&&s&&mm(s)?(i.push(new Yr(a,Jx(s))),r+=2):(i.push(new Yr(a,{})),r++)}return new ke(i,{})}function X1(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=Sv(new ke([],{}),0,i))}),n}function Jx(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function ew(t,n,e){return t==e.path&&Pi(n,e.parameters)}var Zl="imperative",Nt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Nt||{}),Nn=class{id;url;constructor(n,e){this.id=n,this.url=e}},Yo=class extends Nn{type=Nt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},gi=class extends Nn{urlAfterRedirects;type=Nt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Wt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(Wt||{}),Jl=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(Jl||{}),zn=class extends Nn{reason;code;type=Nt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function bw(t){return t instanceof zn&&(t.code===Wt.Redirect||t.code===Wt.SupersededByNewNavigation)}var lr=class extends Nn{reason;code;type=Nt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},Zo=class extends Nn{error;target;type=Nt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},ec=class extends Nn{urlAfterRedirects;state;type=Nt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},pm=class extends Nn{urlAfterRedirects;state;type=Nt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},hm=class extends Nn{urlAfterRedirects;state;shouldActivate;type=Nt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},gm=class extends Nn{urlAfterRedirects;state;type=Nt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},vm=class extends Nn{urlAfterRedirects;state;type=Nt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},_m=class{route;type=Nt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},bm=class{route;type=Nt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},ym=class{snapshot;type=Nt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Cm=class{snapshot;type=Nt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},xm=class{snapshot;type=Nt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},wm=class{snapshot;type=Nt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Qa=class{},tc=class{},Ka=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function J1(t){return!(t instanceof Qa)&&!(t instanceof Ka)&&!(t instanceof tc)}var Em=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new ns(this.rootInjector)}},ns=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Em(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(Q(Ae))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Dm=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=Tv(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=Tv(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=Av(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Av(n,this._root).map(e=>e.value)}};function Tv(t,n){if(t===n.value)return n;for(let e of n.children){let i=Tv(t,e);if(i)return i}return null}function Av(t,n){if(t===n.value)return[n];for(let e of n.children){let i=Av(t,e);if(i.length)return i.unshift(n),i}return[]}var Rn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function Wa(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var nc=class extends Dm{snapshot;constructor(n,e){super(n),this.snapshot=e,Hv(this,n)}toString(){return this.snapshot.toString()}};function yw(t,n){let e=eR(t,n),i=new vt([new Yr("",{})]),r=new vt({}),o=new vt({}),a=new vt({}),s=new vt(""),l=new vi(i,r,a,s,o,ue,t,e.root);return l.snapshot=e.root,new nc(new Rn(l,[]),e)}function eR(t,n){let e={},i={},r={},a=new Xa([],e,r,"",i,ue,t,null,{},n);return new ic("",new Rn(a,[]))}var vi=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,a,s,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=a,this.component=s,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(V(c=>c[ac]))??Z(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(V(n=>Wo(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(V(n=>Wo(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Bv(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:C(C({},n.params),t.params),data:C(C({},n.data),t.data),resolve:C(C(C(C({},t.data),n.data),r?.data),t._resolvedData)}:i={params:C({},t.params),data:C({},t.data),resolve:C(C({},t.data),t._resolvedData??{})},r&&xw(r)&&(i.resolve[ac]=r.title),i}var Xa=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[ac]}constructor(n,e,i,r,o,a,s,l,c,f){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=a,this.component=s,this.routeConfig=l,this._resolve=c,this._environmentInjector=f}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Wo(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Wo(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},ic=class extends Dm{url;constructor(n,e){super(e),this.url=n,Hv(this,e)}toString(){return Cw(this._root)}};function Hv(t,n){n.value._routerState=t,n.children.forEach(e=>Hv(t,e))}function Cw(t){let n=t.children.length>0?` { ${t.children.map(Cw).join(", ")} } `:"";return`${t.value}${n}`}function xv(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Pi(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Pi(n.params,e.params)||t.paramsSubject.next(e.params),S1(n.url,e.url)||t.urlSubject.next(e.url),Pi(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function kv(t,n){let e=Pi(t.params,n.params)&&R1(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||kv(t.parent,n.parent))}function xw(t){return typeof t.title=="string"||t.title===null}var ww=new y(""),Ko=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=ue;activateEvents=new P;deactivateEvents=new P;attachEvents=new P;detachEvents=new P;routerOutletData=gx();parentContexts=d(ns);location=d(nt);changeDetector=d(ge);inputBinder=d(Tm,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new k(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new k(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new k(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new k(4013,!1);this._activatedRoute=e;let r=this.location,a=e.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,l=new Ov(e,s,r.injector,this.routerOutletData);this.activated=r.createComponent(a,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[je]})}return t})(),Ov=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===vi?this.route:n===ns?this.childContexts:n===ww?this.outletData:this.parent.get(n,e)}},Tm=new y("");var Uv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&re(0,"router-outlet")},dependencies:[Ko],encapsulation:2})}return t})();function zv(t){let n=t.children&&t.children.map(zv),e=n?ee(C({},t),{children:n}):C({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==ue&&(e.component=Uv),e}function tR(t,n,e){let i=rc(t,n._root,e?e._root:void 0);return new nc(i,n)}function rc(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=nR(t,n,e);return new Rn(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let a=o.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(s=>rc(t,s)),a}}let i=iR(n.value),r=n.children.map(o=>rc(t,o));return new Rn(i,r)}}function nR(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return rc(t,i,r);return rc(t,i)})}function iR(t){return new vi(new vt(t.url),new vt(t.params),new vt(t.queryParams),new vt(t.fragment),new vt(t.data),t.outlet,t.component,t)}var Ja=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},Ew="ngNavigationCancelingError";function Im(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=Qr(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=Dw(!1,Wt.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function Dw(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[Ew]=!0,e.cancellationCode=n,e}function rR(t){return Iw(t)&&Qr(t.url)}function Iw(t){return!!t&&t[Ew]}var Rv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),xv(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=Wa(e);n.children.forEach(o=>{let a=o.value.outlet;this.deactivateRoutes(o,r[a],i),delete r[a]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let a=i.getContext(r.outlet);a&&this.deactivateChildRoutes(n,e,a.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Wa(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);if(i&&i.outlet){let a=i.outlet.detach(),s=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Wa(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=Wa(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new wm(o.value.snapshot))}),n.children.length&&this.forwardEvent(new Cm(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(xv(r),r===o)if(r.component){let a=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,a.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let a=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let s=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),xv(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=r,a.outlet&&a.outlet.activateWith(r,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,i)}},Mm=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},Za=class{component;route;constructor(n,e){this.component=n,this.route=e}};function oR(t,n,e){let i=t._root,r=n?n._root:null;return Wl(i,r,e,[i.value])}function aR(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function is(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Dp(t)?t:n.get(t):i}function Wl(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=Wa(n);return t.children.forEach(a=>{sR(a,o[a.value.outlet],e,i.concat([a.value]),r),delete o[a.value.outlet]}),Object.entries(o).forEach(([a,s])=>Ql(s,e.getContext(a),r)),r}function sR(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,a=n?n.value:null,s=e?e.getContext(t.value.outlet):null;if(a&&o.routeConfig===a.routeConfig){let l=lR(a,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new Mm(i)):(o.data=a.data,o._resolvedData=a._resolvedData),o.component?Wl(t,n,s?s.children:null,i,r):Wl(t,n,e,i,r),l&&s&&s.outlet&&s.outlet.isActivated&&r.canDeactivateChecks.push(new Za(s.outlet.component,a))}else a&&Ql(n,s,r),r.canActivateChecks.push(new Mm(i)),o.component?Wl(t,null,s?s.children:null,i,r):Wl(t,null,e,i,r);return r}function lR(t,n,e){if(typeof e=="function")return Tt(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Go(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Go(t.url,n.url)||!Pi(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!kv(t,n)||!Pi(t.queryParams,n.queryParams);default:return!kv(t,n)}}function Ql(t,n,e){let i=Wa(t),r=t.value;Object.entries(i).forEach(([o,a])=>{r.component?n?Ql(a,n.children.getContext(o),e):Ql(a,null,e):Ql(a,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new Za(n.outlet.component,r)):e.canDeactivateChecks.push(new Za(null,r)):e.canDeactivateChecks.push(new Za(null,r))}function sc(t){return typeof t=="function"}function cR(t){return typeof t=="boolean"}function dR(t){return t&&sc(t.canLoad)}function uR(t){return t&&sc(t.canActivate)}function mR(t){return t&&sc(t.canActivateChild)}function fR(t){return t&&sc(t.canDeactivate)}function pR(t){return t&&sc(t.canMatch)}function Mw(t){return t instanceof xo||t?.name==="EmptyError"}var sm=Symbol("INITIAL_VALUE");function es(){return Ze(t=>zs(t.map(n=>n.pipe(Oe(1),Me(sm)))).pipe(V(n=>{for(let e of n)if(e!==!0){if(e===sm)return sm;if(e===!1||hR(e))return e}return!0}),ae(n=>n!==sm),Oe(1)))}function hR(t){return Qr(t)||t instanceof Ja}function Sw(t){return t.aborted?Z(void 0).pipe(Oe(1)):new ie(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function Tw(t){return ye(Sw(t))}function gR(t){return Mt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?Z(ee(C({},n),{guardsResult:!0})):vR(o,e,i).pipe(Mt(a=>a&&cR(a)?_R(e,r,t):Z(a)),V(a=>ee(C({},n),{guardsResult:a})))})}function vR(t,n,e){return ze(t).pipe(Mt(i=>wR(i.component,i.route,e,n)),zi(i=>i!==!0,!0))}function _R(t,n,e){return ze(n).pipe(Eo(i=>Ii(yR(i.route.parent,e),bR(i.route,e),xR(t,i.path),CR(t,i.route))),zi(i=>i!==!0,!0))}function bR(t,n){return t!==null&&n&&n(new xm(t)),Z(!0)}function yR(t,n){return t!==null&&n&&n(new ym(t)),Z(!0)}function CR(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return Z(!0);let i=e.map(r=>Tn(()=>{let o=n._environmentInjector,a=is(r,o),s=uR(a)?a.canActivate(n,t):Tt(o,()=>a(n,t));return Qo(s).pipe(zi())}));return Z(i).pipe(es())}function xR(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>aR(o)).filter(o=>o!==null).map(o=>Tn(()=>{let a=o.guards.map(s=>{let l=o.node._environmentInjector,c=is(s,l),f=mR(c)?c.canActivateChild(e,t):Tt(l,()=>c(e,t));return Qo(f).pipe(zi())});return Z(a).pipe(es())}));return Z(r).pipe(es())}function wR(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return Z(!0);let o=r.map(a=>{let s=n._environmentInjector,l=is(a,s),c=fR(l)?l.canDeactivate(t,n,e,i):Tt(s,()=>l(t,n,e,i));return Qo(c).pipe(zi())});return Z(o).pipe(es())}function ER(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return Z(!0);let a=o.map(s=>{let l=is(s,t),c=dR(l)?l.canLoad(n,e):Tt(t,()=>l(n,e)),f=Qo(c);return r?f.pipe(Tw(r)):f});return Z(a).pipe(es(),Aw(i))}function Aw(t){return np(ot(n=>{if(typeof n!="boolean")throw Im(t,n)}),V(n=>n===!0))}function DR(t,n,e,i,r,o){let a=n.canMatch;if(!a||a.length===0)return Z(!0);let s=a.map(l=>{let c=is(l,t),f=pR(c)?c.canMatch(n,e,r):Tt(t,()=>c(n,e,r));return Qo(f).pipe(Tw(o))});return Z(s).pipe(es(),Aw(i))}var sr=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},oc=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function IR(t){throw new k(4e3,!1)}function MR(t){throw Dw(!1,Wt.GuardRejected)}var Nv=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[ue])throw IR(`${n.redirectTo}`);r=r.children[ue]}}async applyRedirectCommands(n,e,i,r,o){let a=await SR(e,r,o);if(a instanceof dn)throw new oc(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,i);if(a[0]==="/")throw new oc(s);return s}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new dn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let s=o.substring(1);i[r]=e[s]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),a={};return Object.entries(e.children).forEach(([s,l])=>{a[s]=this.createSegmentGroup(n,l,i,r)}),new ke(o,a)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new k(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function SR(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return um(Qo(Tt(e,()=>i(n))))}function TR(t,n){return t.providers&&!t._injector&&(t._injector=Al(t.providers,n,`Route: ${t.path}`)),t._injector??n}function hi(t){return t.outlet||ue}function AR(t,n){let e=t.filter(i=>hi(i)===n);return e.push(...t.filter(i=>hi(i)!==n)),e}var Fv={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function kw(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function kR(t,n,e,i,r,o,a){let s=Ow(t,n,e);if(!s.matched)return Z(s);let l=kw(o(s));return i=TR(n,i),DR(i,n,e,r,l,a).pipe(V(c=>c===!0?s:C({},Fv)))}function Ow(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?C({},Fv):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||iw)(e,t,n);if(!r)return C({},Fv);let o={};Object.entries(r.posParams??{}).forEach(([s,l])=>{o[s]=l.path});let a=r.consumed.length>0?C(C({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:a,positionalParamSegments:r.posParams??{}}}function tw(t,n,e,i,r){return e.length>0&&NR(t,e,i,r)?{segmentGroup:new ke(n,RR(i,new ke(e,t.children))),slicedSegments:[]}:e.length===0&&FR(t,e,i)?{segmentGroup:new ke(t.segments,OR(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new ke(t.segments,t.children),slicedSegments:e}}function OR(t,n,e,i){let r={};for(let o of e)if(Am(t,n,o)&&!i[hi(o)]){let a=new ke([],{});r[hi(o)]=a}return C(C({},i),r)}function RR(t,n){let e={};e[ue]=n;for(let i of t)if(i.path===""&&hi(i)!==ue){let r=new ke([],{});e[hi(i)]=r}return e}function NR(t,n,e,i){return e.some(r=>!Am(t,n,r)||!(hi(r)!==ue)?!1:!(i!==void 0&&hi(r)===i))}function FR(t,n,e){return e.some(i=>Am(t,n,i))}function Am(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function PR(t,n,e){return n.length===0&&!t.children[e]}var Pv=class{};async function LR(t,n,e,i,r,o,a="emptyOnly",s){return new Lv(t,n,e,i,r,a,o,s).recognize()}var jR=31,Lv=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,a,s,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=l,this.applyRedirects=new Nv(this.urlSerializer,this.urlTree)}noMatchError(n){return new k(4002,`'${n.segmentGroup}'`)}async recognize(){let n=tw(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Rn(i,e),o=new ic("",r),a=pw(i,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(a),{state:o,tree:a}}async match(n){let e=new Xa([],Object.freeze({}),Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),ue,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,ue,e),rootSnapshot:e}}catch(i){if(i instanceof oc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof sr?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let a=await this.processSegment(n,e,i,i.segments,r,!0,o);return a instanceof Rn?[a]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let a=[];for(let l of o){let c=i.children[l],f=AR(e,l),g=await this.processSegmentGroup(n,f,c,l,r);a.push(...g)}let s=Rw(a);return VR(s),s}async processSegment(n,e,i,r,o,a,s){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,a,s)}catch(c){if(c instanceof sr||Mw(c))continue;throw c}if(PR(i,r,o))return new Pv;throw new sr(i)}async processSegmentAgainstRoute(n,e,i,r,o,a,s,l){if(hi(i)!==a&&(a===ue||!Am(r,o,i)))throw new sr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,a,l);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,a,l);throw new sr(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,a,s){let{matched:l,parameters:c,consumedSegments:f,positionalParamSegments:g,remainingSegments:v}=Ow(e,r,o);if(!l)throw new sr(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>jR&&(this.allowRedirects=!1));let b=this.createSnapshot(n,r,o,c,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let w=await this.applyRedirects.applyRedirectCommands(f,r.redirectTo,g,kw(b),n),j=await this.applyRedirects.lineralizeSegments(r,w);return this.processSegment(n,i,e,j.concat(v),a,!1,s)}createSnapshot(n,e,i,r,o){let a=new Xa(i,r,Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,HR(e),hi(e),e.component??e._loadedComponent??null,e,UR(e),n),s=Bv(a,o,this.paramsInheritanceStrategy);return a.params=Object.freeze(s.params),a.data=Object.freeze(s.data),a}async matchSegmentAgainstRoute(n,e,i,r,o,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=fn=>this.createSnapshot(n,i,fn.consumedSegments,fn.parameters,a),l=await um(kR(e,i,r,n,this.urlSerializer,s,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new sr(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),f=i._loadedInjector??n,{parameters:g,consumedSegments:v,remainingSegments:b}=l,w=this.createSnapshot(n,i,v,g,a),{segmentGroup:j,slicedSegments:U}=tw(e,v,b,c,o);if(U.length===0&&j.hasChildren()){let fn=await this.processChildren(f,c,j,w);return new Rn(w,fn)}if(c.length===0&&U.length===0)return new Rn(w,[]);let Y=hi(i)===o,Ue=await this.processSegment(f,c,j,U,Y?ue:o,!0,w);return new Rn(w,Ue instanceof Rn?[Ue]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await um(ER(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw MR(e)}return{routes:[],injector:n}}};function VR(t){t.sort((n,e)=>n.value.outlet===ue?-1:e.value.outlet===ue?1:n.value.outlet.localeCompare(e.value.outlet))}function BR(t){let n=t.value.routeConfig;return n&&n.path===""}function Rw(t){let n=[],e=new Set;for(let i of t){if(!BR(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=Rw(i.children);n.push(new Rn(i.value,r))}return n.filter(i=>!e.has(i))}function HR(t){return t.data||{}}function UR(t){return t.resolve||{}}function zR(t,n,e,i,r,o,a){return Mt(async s=>{let{state:l,tree:c}=await LR(t,n,e,i,s.extractedUrl,r,o,a);return ee(C({},s),{targetSnapshot:l,urlAfterRedirects:c})})}function $R(t){return Mt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return Z(n);let r=new Set(i.map(s=>s.route)),o=new Set;for(let s of r)if(!o.has(s))for(let l of Nw(s))o.add(l);let a=0;return ze(o).pipe(Eo(s=>r.has(s)?qR(s,e,t):(s.data=Bv(s,s.parent,t).resolve,Z(void 0))),ot(()=>a++),_d(1),Mt(s=>a===o.size?Z(n):Ye))})}function Nw(t){let n=t.children.map(e=>Nw(e)).flat();return[t,...n]}function qR(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!xw(i)&&(r[ac]=i.title),Tn(()=>(t.data=Bv(t,t.parent,e).resolve,GR(r,t,n).pipe(V(o=>(t._resolvedData=o,t.data=C(C({},t.data),o),null)))))}function GR(t,n,e){let i=Ev(t);if(i.length===0)return Z({});let r={};return ze(i).pipe(Mt(o=>WR(t[o],n,e).pipe(zi(),ot(a=>{if(a instanceof Ja)throw Im(new Zr,a);r[o]=a}))),_d(1),V(()=>r),kr(o=>Mw(o)?Ye:Hs(o)))}function WR(t,n,e){let i=n._environmentInjector,r=is(t,i),o=r.resolve?r.resolve(n,e):Tt(i,()=>r(n,e));return Qo(o)}function nw(t){return Ze(n=>{let e=t(n);return e?ze(e).pipe(V(()=>n)):Z(n)})}var $v=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===ue);return i}getResolvedTitleForRoute(e){return e.data[ac]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(Fw),providedIn:"root"})}return t})(),Fw=(()=>{class t extends $v{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(Q(Zx))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),rs=new y("",{factory:()=>({})}),lc=new y(""),Pw=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(Wg);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await ow(Tt(e,()=>i.loadComponent())),a=await Vw(jw(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=a,a}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await Lw(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function Lw(t,n,e,i){let r=await ow(Tt(e,()=>t.loadChildren())),o=await Vw(jw(r)),a;o instanceof ku||Array.isArray(o)?a=o:a=await n.compileModuleAsync(o),i&&i(t);let s,l,c=!1,f;return Array.isArray(a)?(l=a,c=!0):(s=a.create(e).injector,f=a,l=s.get(lc,[],{optional:!0,self:!0}).flat()),{routes:l.map(zv),injector:s,factory:f}}function YR(t){return t&&typeof t=="object"&&"default"in t}function jw(t){return YR(t)?t.default:t}async function Vw(t){return t}var km=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(ZR),providedIn:"root"})}return t})(),ZR=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Bw=new y("");var QR=()=>{},Hw=new y(""),Uw=(()=>{class t{currentNavigation=ne(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=ne(null);events=new E;transitionAbortWithErrorSubject=new E;configLoader=d(Pw);environmentInjector=d(Ae);destroyRef=d(sn);urlSerializer=d(ts);rootContexts=d(ns);location=d(Gr);inputBindingEnabled=d(Tm,{optional:!0})!==null;titleStrategy=d($v);options=d(rs,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(km);createViewTransition=d(Bw,{optional:!0});navigationErrorHandler=d(Hw,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Z(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new _m(r)),i=r=>this.events.next(new bm(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;De(()=>{this.transitions?.next(ee(C({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new vt(null),this.transitions.pipe(ae(i=>i!==null),Ze(i=>{let r=!1,o=new AbortController,a=()=>!r&&this.currentTransition?.id===i.id;return Z(i).pipe(Ze(s=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",Wt.SupersededByNewNavigation),Ye;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:s.id,initialUrl:s.rawUrl,extractedUrl:s.extractedUrl,targetBrowserUrl:typeof s.extras.browserUrl=="string"?this.urlSerializer.parse(s.extras.browserUrl):s.extras.browserUrl,trigger:s.source,extras:s.extras,previousNavigation:l?ee(C({},l),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:s.routesRecognizeHandler,beforeActivateHandler:s.beforeActivateHandler});let c=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=s.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!c&&f!=="reload")return this.events.next(new lr(s.id,this.urlSerializer.serialize(s.rawUrl),"",Jl.IgnoredSameUrlNavigation)),s.resolve(!1),Ye;if(this.urlHandlingStrategy.shouldProcessUrl(s.rawUrl))return Z(s).pipe(Ze(g=>(this.events.next(new Yo(g.id,this.urlSerializer.serialize(g.extractedUrl),g.source,g.restoredState)),g.id!==this.navigationId?Ye:Promise.resolve(g))),zR(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),ot(g=>{i.targetSnapshot=g.targetSnapshot,i.urlAfterRedirects=g.urlAfterRedirects,this.currentNavigation.update(v=>(v.finalUrl=g.urlAfterRedirects,v)),this.events.next(new tc)}),Ze(g=>ze(i.routesRecognizeHandler.deferredHandle??Z(void 0)).pipe(V(()=>g))),ot(()=>{let g=new ec(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(g)}));if(c&&this.urlHandlingStrategy.shouldProcessUrl(s.currentRawUrl)){let{id:g,extractedUrl:v,source:b,restoredState:w,extras:j}=s,U=new Yo(g,this.urlSerializer.serialize(v),b,w);this.events.next(U);let Y=yw(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=ee(C({},s),{targetSnapshot:Y,urlAfterRedirects:v,extras:ee(C({},j),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Ue=>(Ue.finalUrl=v,Ue)),Z(i)}else return this.events.next(new lr(s.id,this.urlSerializer.serialize(s.extractedUrl),"",Jl.IgnoredByUrlHandlingStrategy)),s.resolve(!1),Ye}),V(s=>{let l=new pm(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);return this.events.next(l),this.currentTransition=i=ee(C({},s),{guards:oR(s.targetSnapshot,s.currentSnapshot,this.rootContexts)}),i}),gR(s=>this.events.next(s)),Ze(s=>{if(i.guardsResult=s.guardsResult,s.guardsResult&&typeof s.guardsResult!="boolean")throw Im(this.urlSerializer,s.guardsResult);let l=new hm(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot,!!s.guardsResult);if(this.events.next(l),!a())return Ye;if(!s.guardsResult)return this.cancelNavigationTransition(s,"",Wt.GuardRejected),Ye;if(s.guards.canActivateChecks.length===0)return Z(s);let c=new gm(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);if(this.events.next(c),!a())return Ye;let f=!1;return Z(s).pipe($R(this.paramsInheritanceStrategy),ot({next:()=>{f=!0;let g=new vm(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(g)},complete:()=>{f||this.cancelNavigationTransition(s,"",Wt.NoDataFromResolver)}}))}),nw(s=>{let l=f=>{let g=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let v=f._environmentInjector;g.push(this.configLoader.loadComponent(v,f.routeConfig).then(b=>{f.component=b}))}for(let v of f.children)g.push(...l(v));return g},c=l(s.targetSnapshot.root);return c.length===0?Z(s):ze(Promise.all(c).then(()=>s))}),nw(()=>this.afterPreactivation()),Ze(()=>{let{currentSnapshot:s,targetSnapshot:l}=i,c=this.createViewTransition?.(this.environmentInjector,s.root,l.root);return c?ze(c).pipe(V(()=>i)):Z(i)}),Oe(1),Ze(s=>{let l=tR(e.routeReuseStrategy,s.targetSnapshot,s.currentRouterState);this.currentTransition=i=s=ee(C({},s),{targetRouterState:l}),this.currentNavigation.update(f=>(f.targetRouterState=l,f)),this.events.next(new Qa);let c=i.beforeActivateHandler.deferredHandle;return c?ze(c.then(()=>s)):Z(s)}),ot(s=>{new Rv(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),a()&&(r=!0,this.currentNavigation.update(l=>(l.abort=QR,l)),this.lastSuccessfulNavigation.set(De(this.currentNavigation)),this.events.next(new gi(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects))),this.titleStrategy?.updateTitle(s.targetRouterState.snapshot),s.resolve(!0))}),ye(Sw(o.signal).pipe(ae(()=>!r&&!i.targetRouterState),ot(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",Wt.Aborted)}))),ot({complete:()=>{r=!0}}),ye(this.transitionAbortWithErrorSubject.pipe(ot(s=>{throw s}))),Do(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",Wt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),kr(s=>{if(r=!0,this.destroyed)return i.resolve(!1),Ye;if(Iw(s))this.events.next(new zn(i.id,this.urlSerializer.serialize(i.extractedUrl),s.message,s.cancellationCode)),rR(s)?this.events.next(new Ka(s.url,s.navigationBehaviorOptions)):i.resolve(!1);else{let l=new Zo(i.id,this.urlSerializer.serialize(i.extractedUrl),s,i.targetSnapshot??void 0);try{let c=Tt(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(c instanceof Ja){let{message:f,cancellationCode:g}=Im(this.urlSerializer,c);this.events.next(new zn(i.id,this.urlSerializer.serialize(i.extractedUrl),f,g)),this.events.next(new Ka(c.redirectTo,c.navigationBehaviorOptions))}else throw this.events.next(l),s}catch(c){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(c)}}return Ye}))}))}cancelNavigationTransition(e,i,r){let o=new zn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=De(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function KR(t){return t!==Zl}var zw=new y("");var $w=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(XR),providedIn:"root"})}return t})(),Sm=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},XR=(()=>{class t extends Sm{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Om=(()=>{class t{urlSerializer=d(ts);options=d(rs,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(Gr);urlHandlingStrategy=d(km);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new dn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,a=r??o;return a instanceof dn?this.urlSerializer.serialize(a):a}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=yw(null,d(Ae));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:()=>d(JR),providedIn:"root"})}return t})(),JR=(()=>{class t extends Om{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof Yo?this.updateStateMemento():e instanceof lr?this.commitTransition(i):e instanceof ec?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Qa?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof zn&&!bw(e)?this.restoreHistory(i):e instanceof Zo?this.restoreHistory(i,!0):e instanceof gi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:a,state:s}=r;if(this.location.isCurrentPathEqualTo(e)||a){let l=this.browserPageId,c=C(C({},s),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=C(C({},s),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?C({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):C({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function qv(t,n){t.events.pipe(ae(e=>e instanceof gi||e instanceof zn||e instanceof Zo||e instanceof lr),V(e=>e instanceof gi||e instanceof lr?0:(e instanceof zn?e.code===Wt.Redirect||e.code===Wt.SupersededByNewNavigation:!1)?2:1),ae(e=>e!==2),Oe(1)).subscribe(()=>{n()})}var Xo=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(Nu);stateManager=d(Om);options=d(rs,{optional:!0})||{};pendingTasks=d(Ki);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(Uw);urlSerializer=d(ts);location=d(Gr);urlHandlingStrategy=d(km);injector=d(Ae);_events=new E;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d($w);injectorCleanup=d(zw,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(lc,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(Tm,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new fe;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=De(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof zn&&i.code!==Wt.Redirect&&i.code!==Wt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof gi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Ka){let a=i.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=C({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||KR(r.source)},a);this.scheduleNavigation(s,Zl,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}J1(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Zl,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let a=r?.navigationId?r:null,s=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=ee(C({},o),{browserUrl:e})),r){let c=C({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(s);this.scheduleNavigation(l,i,a,o).catch(c=>{this.disposed||this.injector.get(vn)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return De(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(zv),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:a,queryParamsHandling:s,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:a,f=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":f=C(C({},this.currentUrlTree.queryParams),o);break;case"preserve":f=this.currentUrlTree.queryParams;break;default:f=o||null}f!==null&&(f=this.removeEmptyProps(f));let g;try{let v=r?r.snapshot:this.routerState.snapshot.root;g=hw(v)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),g=this.currentUrlTree.root}return gw(g,e,f,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=Qr(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Zl,null,i)}navigate(e,i={skipLocationChange:!1}){return eN(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(qi(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=C({},jv):i===!1?r=C({},Kl):r=C(C({},Kl),i),Qr(e))return Dv(this.currentUrlTree,e,r);let o=this.parseUrl(e);return Dv(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,a){if(this.disposed)return Promise.resolve(!1);let s,l,c;a?(s=a.resolve,l=a.reject,c=a.promise):c=new Promise((g,v)=>{s=g,l=v});let f=this.pendingTasks.add();return qv(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(f))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:s,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function eN(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new k(4008,!1)}var nN=(()=>{class t{router=d(Xo);stateManager=d(Om);fragment=ne("");queryParams=ne({});path=ne("");serializer=d(ts);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof gi&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new dn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),$n=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new Ni("href"),{optional:!0});reactiveHref=Yg(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return De(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return De(this._target)}_target=ne(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return De(this._queryParams)}_queryParams=ne(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return De(this._fragment)}_fragment=ne(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return De(this._queryParamsHandling)}_queryParamsHandling=ne(void 0);set state(e){this._state.set(e)}get state(){return De(this._state)}_state=ne(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return De(this._info)}_info=ne(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return De(this._relativeTo)}_relativeTo=ne(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return De(this._preserveFragment)}_preserveFragment=ne(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return De(this._skipLocationChange)}_skipLocationChange=ne(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return De(this._replaceUrl)}_replaceUrl=ne(!1);isAnchorElement;onChanges=new E;applicationErrorHandler=d(vn);options=d(rs,{optional:!0});reactiveRouterState=d(nN);constructor(e,i,r,o,a,s){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=a,this.locationStrategy=s;let l=a.nativeElement.tagName?.toLowerCase();this.isAnchorElement=l==="a"||l==="area"||!!(typeof customElements=="object"&&customElements.get(l)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=ne(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(Qr(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,a){let s=this._urlTree();if(s===null||this.isAnchorElement&&(e!==0||i||r||o||a||typeof this.target=="string"&&this.target!="_self"))return!0;let l={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(s,l)?.catch(c=>{this.applicationErrorHandler(c)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=cn(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:Qr(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return De(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(D(Xo),D(vi),El("tabindex"),D(Se),D(N),D(za))};static \u0275dir=S({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&I("click",function(a){return r.onClick(a.button,a.ctrlKey,a.shiftKey,a.altKey,a.metaKey)}),i&2&&ce("href",r.reactiveHref(),ug)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",J],skipLocationChange:[2,"skipLocationChange","skipLocationChange",J],replaceUrl:[2,"replaceUrl","replaceUrl",J],routerLink:"routerLink"},features:[je]})}return t})(),Gv=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new P;link=d($n,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(a=>{a instanceof gi&&this.update()})}ngAfterContentInit(){Z(this.links.changes,Z(null)).pipe(Ar()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=ze(e).pipe(Ar()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=iN(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?C({},jv):C({},Kl);return r=>{let o=r.urlTree;return o?De(Vv(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(D(Xo),D(N),D(Se),D(ge))};static \u0275dir=S({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&it(o,$n,5),i&2){let a;$(a=q())&&(r.links=a)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[je]})}return t})();function iN(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var rN=new y("");function Wv(t,...n){return Yi([{provide:lc,multi:!0,useValue:t},[],{provide:vi,useFactory:oN},{provide:Pu,multi:!0,useFactory:aN},n.map(e=>e.\u0275providers)])}function oN(){return d(Xo).routerState.root}function aN(){let t=d(W);return n=>{let e=t.get(kn);if(n!==e.components[0])return;let i=t.get(Xo),r=t.get(sN);t.get(lN)===1&&i.initialNavigation(),t.get(cN,null,{optional:!0})?.setUpPreloading(),t.get(rN,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var sN=new y("",{factory:()=>new E}),lN=new y("",{factory:()=>1});var cN=new y("");var Kw=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(D(Se),D(N))};static \u0275dir=S({type:t})}return t})(),uN=(()=>{class t extends Kw{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,features:[be]})}return t})(),hc=new y("");var mN={provide:hc,useExisting:hn(()=>Ft),multi:!0};function fN(){let t=Un()?Un().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var pN=new y(""),Ft=(()=>{class t extends Kw{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!fN())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(D(Se),D(N),D(pN,8))};static \u0275dir=S({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&I("input",function(a){return r._handleInput(a.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(a){return r._compositionEnd(a.target.value)})},standalone:!1,features:[xe([mN]),be]})}return t})();function Kv(t){return t==null||Xv(t)===0}function Xv(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var zm=new y(""),$m=new y(""),hN=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,le=class{static min(n){return gN(n)}static max(n){return vN(n)}static required(n){return _N(n)}static requiredTrue(n){return bN(n)}static email(n){return yN(n)}static minLength(n){return CN(n)}static maxLength(n){return xN(n)}static pattern(n){return wN(n)}static nullValidator(n){return Xw()}static compose(n){return rE(n)}static composeAsync(n){return oE(n)}};function gN(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function vN(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function _N(t){return Kv(t.value)?{required:!0}:null}function bN(t){return t.value===!0?null:{required:!0}}function yN(t){return Kv(t.value)||hN.test(t.value)?null:{email:!0}}function CN(t){return n=>{let e=n.value?.length??Xv(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function xN(t){return n=>{let e=n.value?.length??Xv(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function wN(t){if(!t)return Xw;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(Kv(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function Xw(t){return null}function Jw(t){return t!=null}function eE(t){return ir(t)?ze(t):t}function tE(t){let n={};return t.forEach(e=>{n=e!=null?C(C({},n),e):n}),Object.keys(n).length===0?null:n}function nE(t,n){return n.map(e=>e(t))}function EN(t){return!t.validate}function iE(t){return t.map(n=>EN(n)?n:e=>n.validate(e))}function rE(t){if(!t)return null;let n=t.filter(Jw);return n.length==0?null:function(e){return tE(nE(e,n))}}function Jv(t){return t!=null?rE(iE(t)):null}function oE(t){if(!t)return null;let n=t.filter(Jw);return n.length==0?null:function(e){let i=nE(e,n).map(eE);return wo(i).pipe(V(tE))}}function e_(t){return t!=null?oE(iE(t)):null}function qw(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function aE(t){return t._rawValidators}function sE(t){return t._rawAsyncValidators}function Yv(t){return t?Array.isArray(t)?t:[t]:[]}function Nm(t,n){return Array.isArray(t)?t.includes(n):t===n}function Gw(t,n){let e=Yv(n);return Yv(t).forEach(r=>{Nm(e,r)||e.push(r)}),e}function Ww(t,n){return Yv(n).filter(e=>!Nm(t,e))}var Fm=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Jv(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=e_(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Kr=class extends Fm{name;get formDirective(){return null}get path(){return null}},_i=class extends Fm{_parent=null;name=null;valueAccessor=null},Pm=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Yt=(()=>{class t extends Pm{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(D(_i,2))};static \u0275dir=S({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&R("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[be]})}return t})(),yn=(()=>{class t extends Pm{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(D(Kr,10))};static \u0275dir=S({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&R("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[be]})}return t})();var cc="VALID",Rm="INVALID",os="PENDING",dc="DISABLED",Xr=class{},Lm=class extends Xr{value;source;constructor(n,e){super(),this.value=n,this.source=e}},mc=class extends Xr{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},fc=class extends Xr{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},as=class extends Xr{status;source;constructor(n,e){super(),this.status=n,this.source=e}},jm=class extends Xr{source;constructor(n){super(),this.source=n}},pc=class extends Xr{source;constructor(n){super(),this.source=n}};function t_(t){return(qm(t)?t.validators:t)||null}function DN(t){return Array.isArray(t)?Jv(t):t||null}function n_(t,n){return(qm(n)?n.asyncValidators:t)||null}function IN(t){return Array.isArray(t)?e_(t):t||null}function qm(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function lE(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new k(1e3,"");if(!i[e])throw new k(1001,"")}function cE(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new k(-1002,"")})}var ls=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return De(this.statusReactive)}set status(n){De(()=>this.statusReactive.set(n))}_status=cn(()=>this.statusReactive());statusReactive=ne(void 0);get valid(){return this.status===cc}get invalid(){return this.status===Rm}get pending(){return this.status===os}get disabled(){return this.status===dc}get enabled(){return this.status!==dc}errors;get pristine(){return De(this.pristineReactive)}set pristine(n){De(()=>this.pristineReactive.set(n))}_pristine=cn(()=>this.pristineReactive());pristineReactive=ne(!0);get dirty(){return!this.pristine}get touched(){return De(this.touchedReactive)}set touched(n){De(()=>this.touchedReactive.set(n))}_touched=cn(()=>this.touchedReactive());touchedReactive=ne(!1);get untouched(){return!this.touched}_events=new E;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(Gw(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(Gw(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(Ww(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(Ww(n,this._rawAsyncValidators))}hasValidator(n){return Nm(this._rawValidators,n)}hasAsyncValidator(n){return Nm(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(ee(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new fc(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new fc(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(ee(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new mc(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new mc(!0,i))}markAsPending(n={}){this.status=os;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new as(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(ee(C({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=dc,this.errors=null,this._forEachChild(r=>{r.disable(ee(C({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Lm(this.value,i)),this._events.next(new as(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(ee(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=cc,this._forEachChild(i=>{i.enable(ee(C({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(ee(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===cc||this.status===os)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Lm(this.value,e)),this._events.next(new as(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(ee(C({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?dc:cc}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=os,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=eE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new as(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new P,this.statusChanges=new P}_calculateStatus(){return this._allControlsDisabled()?dc:this.errors?Rm:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(os)?os:this._anyControlsHaveStatus(Rm)?Rm:cc}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new mc(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new fc(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){qm(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=DN(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=IN(this._rawAsyncValidators)}},cs=class extends ls{constructor(n,e,i){super(t_(e),n_(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){cE(this,!0,n),Object.keys(n).forEach(i=>{lE(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,ee(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new pc(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var Zv=class extends cs{};var Gm=new y("",{factory:()=>i_}),i_="always";function MN(t,n){return[...n.path,t]}function Vm(t,n,e=i_){r_(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),TN(t,n),kN(t,n),AN(t,n),SN(t,n)}function Bm(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Um(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Hm(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function SN(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function r_(t,n){let e=aE(t);n.validator!==null?t.setValidators(qw(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=sE(t);n.asyncValidator!==null?t.setAsyncValidators(qw(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Hm(n._rawValidators,r),Hm(n._rawAsyncValidators,r)}function Um(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=aE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=sE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Hm(n._rawValidators,i),Hm(n._rawAsyncValidators,i),e}function TN(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&dE(t,n)})}function AN(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&dE(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function dE(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function kN(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function uE(t,n){t==null,r_(t,n)}function ON(t,n){return Um(t,n)}function mE(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function RN(t){return Object.getPrototypeOf(t.constructor)===uN}function fE(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function pE(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Ft?e=o:RN(o)?i=o:r=o}),r||i||e||null}function NN(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var FN={provide:Kr,useExisting:hn(()=>gc)},uc=Promise.resolve(),gc=(()=>{class t extends Kr{callSetDisabledState;get submitted(){return De(this.submittedReactive)}_submitted=cn(()=>this.submittedReactive());submittedReactive=ne(!1);_directives=new Set;form;ngSubmit=new P;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new cs({},Jv(e),e_(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){uc.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),Vm(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){uc.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){uc.then(()=>{let i=this._findContainer(e.path),r=new cs({});uE(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){uc.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){uc.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),fE(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new jm(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(D(zm,10),D($m,10),D(Gm,8))};static \u0275dir=S({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&I("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[xe([FN]),be]})}return t})();function Yw(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function Zw(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var ss=class extends ls{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(t_(e),n_(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),qm(e)&&(e.nonNullable||e.initialValueIsDefault)&&(Zw(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new pc(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){Yw(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){Yw(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){Zw(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var PN=t=>t instanceof ss;var Cn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var Qv=class extends ls{constructor(n,e,i){super(t_(e),n_(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){cE(this,!1,n),n.forEach((i,r)=>{lE(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],ee(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new pc(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var LN=(()=>{class t extends Kr{callSetDisabledState;get submitted(){return De(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=cn(()=>this._submittedReactive());_submittedReactive=ne(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Um(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return Vm(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){Bm(e.control||null,e,!1),NN(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,fE(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new jm(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(Bm(i||null,e),PN(r)&&(Vm(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);uE(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&ON(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){r_(this.form,this),this._oldForm&&Um(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(D(zm,10),D($m,10),D(Gm,8))};static \u0275dir=S({type:t,features:[be,je]})}return t})();var o_=new y(""),jN={provide:_i,useExisting:hn(()=>vc)},vc=(()=>{class t extends _i{_ngModelWarningConfig;callSetDisabledState;viewModel;form;set isDisabled(e){}model;update=new P;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,a){super(),this._ngModelWarningConfig=o,this.callSetDisabledState=a,this._setValidators(e),this._setAsyncValidators(i),this.valueAccessor=pE(this,r)}ngOnChanges(e){if(this._isControlChanged(e)){let i=e.form.previousValue;i&&Bm(i,this,!1),Vm(this.form,this,this.callSetDisabledState),this.form.updateValueAndValidity({emitEvent:!1})}mE(e,this.viewModel)&&(this.form.setValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.form&&Bm(this.form,this,!1)}get path(){return[]}get control(){return this.form}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_isControlChanged(e){return e.hasOwnProperty("form")}static \u0275fac=function(i){return new(i||t)(D(zm,10),D($m,10),D(hc,10),D(o_,8),D(Gm,8))};static \u0275dir=S({type:t,selectors:[["","formControl",""]],inputs:{form:[0,"formControl","form"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},exportAs:["ngForm"],standalone:!1,features:[xe([jN]),be,je]})}return t})();var VN={provide:_i,useExisting:hn(()=>un)},un=(()=>{class t extends _i{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new P;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,a){super(),this._ngModelWarningConfig=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=pE(this,o)}ngOnChanges(e){this._added||this._setUpControl(),mE(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return MN(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||t)(D(Kr,13),D(zm,10),D($m,10),D(hc,10),D(o_,8))};static \u0275dir=S({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[xe([VN]),be,je]})}return t})();var BN={provide:Kr,useExisting:hn(()=>xt)},xt=(()=>{class t extends LN{form=null;ngSubmit=new P;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&I("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[xe([BN]),be]})}return t})();var HN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({})}return t})();function Qw(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var xn=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return Qw(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new cs(r,o)}record(e,i=null){let r=this._reduceControls(e);return new Zv(r,i)}control(e,i,r){let o={};return this.useNonNullable?(Qw(i)?o=i:(o.validators=i,o.asyncValidators=r),new ss(e,ee(C({},o),{nonNullable:!0}))):new ss(e,i,r)}array(e,i,r){let o=e.map(a=>this._createControl(a));return new Qv(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof ss)return e;if(e instanceof ls)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Zt=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:o_,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:Gm,useValue:e.callSetDisabledState??i_}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[HN]})}return t})();function _c(t){return t.buttons===0||t.detail===0}function bc(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var a_;function hE(){if(a_==null){let t=typeof document<"u"?document.head:null;a_=!!(t&&(t.createShadowRoot||t.attachShadow))}return a_}function s_(t){if(hE()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function eo(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function wt(t){return t.composedPath?t.composedPath()[0]:t.target}var l_;try{l_=typeof Intl<"u"&&Intl.v8BreakIterator}catch{l_=!1}var ve=(()=>{class t{_platformId=d(Ho);isBrowser=this._platformId?Sx(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||l_)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var yc;function gE(){if(yc==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>yc=!0}))}finally{yc=yc||!1}return yc}function ds(t){return gE()?t:!!t.capture}function Qt(t,n=0){return vE(t)?Number(t):arguments.length===2?n:0}function vE(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function mn(t){return t instanceof N?t.nativeElement:t}var _E=new y("cdk-input-modality-detector-options"),bE={ignoreKeys:[18,17,224,91,16]},yE=650,c_={passive:!0,capture:!0},CE=(()=>{class t{_platform=d(ve);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new vt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=wt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<yE||(this._modality.next(_c(e)?"keyboard":"mouse"),this._mostRecentTarget=wt(e))};_onTouchstart=e=>{if(bc(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=wt(e)};constructor(){let e=d(F),i=d(G),r=d(_E,{optional:!0});if(this._options=C(C({},bE),r),this.modalityDetected=this._modality.pipe(qs(1)),this.modalityChanged=this.modalityDetected.pipe(va()),this._platform.isBrowser){let o=d(at).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,c_),o.listen(i,"mousedown",this._onMousedown,c_),o.listen(i,"touchstart",this._onTouchstart,c_)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Cc=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Cc||{}),xE=new y("cdk-focus-monitor-default-options"),Wm=ds({passive:!0,capture:!0}),bi=(()=>{class t{_ngZone=d(F);_platform=d(ve);_inputModalityDetector=d(CE);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(G);_stopInputModalityDetector=new E;constructor(){let e=d(xE,{optional:!0});this._detectionMode=e?.detectionMode||Cc.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=wt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=mn(e);if(!this._platform.isBrowser||r.nodeType!==1)return Z();let o=s_(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new E,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=mn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=mn(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Cc.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Cc.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?yE:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=wt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Wm),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Wm)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(ye(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Wm),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Wm),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ym=new WeakMap,mt=(()=>{class t{_appRef;_injector=d(W);_environmentInjector=d(Ae);load(e){let i=this._appRef=this._appRef||this._injector.get(kn),r=Ym.get(i);r||(r={loaders:new Set,refs:[]},Ym.set(i,r),i.onDestroy(()=>{Ym.get(i)?.refs.forEach(o=>o.destroy()),Ym.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Gu(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var us=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2,changeDetection:0})}return t})(),Zm;function UN(){if(Zm===void 0&&(Zm=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(Zm=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Zm}function Jo(t){return UN()?.createHTML(t)||t}function wE(t,n,e){let i=e.sanitize(Te.HTML,n);t.innerHTML=Jo(i||"")}function cr(t){return Array.isArray(t)?t:[t]}var EE=new Set,ea,ms=(()=>{class t{_platform=d(ve);_nonce=d(Uo,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):$N}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&zN(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function zN(t,n){if(!EE.has(t))try{ea||(ea=document.createElement("style"),n&&ea.setAttribute("nonce",n),ea.setAttribute("type","text/css"),document.head.appendChild(ea)),ea.sheet&&(ea.sheet.insertRule(`@media ${t} {body{ }}`,0),EE.add(t))}catch(e){console.error(e)}}function $N(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var xc=(()=>{class t{_mediaMatcher=d(ms);_zone=d(F);_queries=new Map;_destroySubject=new E;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return DE(cr(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=DE(cr(e)).map(a=>this._registerQuery(a).observable),o=zs(r);return o=Ii(o.pipe(Oe(1)),o.pipe(qs(1),Xn(0))),o.pipe(V(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ie(a=>{let s=l=>this._zone.run(()=>a.next(l));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(Me(i),V(({matches:a})=>({query:e,matches:a})),ye(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function DE(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}function qN(t){if(t.type==="characterData"&&t.target instanceof Comment)return!0;if(t.type==="childList"){for(let n=0;n<t.addedNodes.length;n++)if(!(t.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<t.removedNodes.length;n++)if(!(t.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var IE=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ME=(()=>{class t{_mutationObserverFactory=d(IE);_observedElements=new Map;_ngZone=d(F);constructor(){}ngOnDestroy(){this._observedElements.forEach((e,i)=>this._cleanupObserver(i))}observe(e){let i=mn(e);return new ie(r=>{let a=this._observeElement(i).pipe(V(s=>s.filter(l=>!qN(l))),ae(s=>!!s.length)).subscribe(s=>{this._ngZone.run(()=>{r.next(s)})});return()=>{a.unsubscribe(),this._unobserveElement(i)}})}_observeElement(e){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(e))this._observedElements.get(e).count++;else{let i=new E,r=this._mutationObserverFactory.create(o=>i.next(o));r&&r.observe(e,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(e,{observer:r,stream:i,count:1})}return this._observedElements.get(e).stream})}_unobserveElement(e){this._observedElements.has(e)&&(this._observedElements.get(e).count--,this._observedElements.get(e).count||this._cleanupObserver(e))}_cleanupObserver(e){if(this._observedElements.has(e)){let{observer:i,stream:r}=this._observedElements.get(e);i&&i.disconnect(),r.complete(),this._observedElements.delete(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),SE=(()=>{class t{_contentObserver=d(ME);_elementRef=d(N);event=new P;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(e){this._debounce=Qt(e),this._subscribe()}_debounce;_currentSubscription=null;constructor(){}ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let e=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?e.pipe(Xn(this.debounce)):e).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",J],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return t})(),fs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({providers:[IE]})}return t})();var ps=(()=>{class t{_platform=d(ve);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return WN(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=GN(tF(e));if(i&&(TE(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=TE(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!JN(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return eF(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function GN(t){try{return t.frameElement}catch{return null}}function WN(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function YN(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function ZN(t){return KN(t)&&t.type=="hidden"}function QN(t){return XN(t)&&t.hasAttribute("href")}function KN(t){return t.nodeName.toLowerCase()=="input"}function XN(t){return t.nodeName.toLowerCase()=="a"}function OE(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function TE(t){if(!OE(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function JN(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function eF(t){return ZN(t)?!1:YN(t)||QN(t)||t.hasAttribute("contenteditable")||OE(t)}function tF(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var Qm=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?st(n,{injector:this._injector}):setTimeout(n)}},wc=(()=>{class t{_checker=d(ps);_ngZone=d(F);_document=d(G);_injector=d(W);constructor(){d(mt).load(us)}create(e,i=!1){return new Qm(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var RE=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),NE=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),nF=0,u_=(()=>{class t{_ngZone=d(F);_defaultOptions=d(NE,{optional:!0});_liveElement;_document=d(G);_sanitizer=d(ql);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(RE,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,a;return i.length===1&&typeof i[0]=="number"?a=i[0]:[o,a]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),a==null&&r&&(a=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:wE(this._liveElement,e,this._sanitizer),typeof a=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),a)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${nF++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var to=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(to||{}),AE="cdk-high-contrast-black-on-white",kE="cdk-high-contrast-white-on-black",d_="cdk-high-contrast-active",FE=(()=>{class t{_platform=d(ve);_hasCheckedHighContrastMode=!1;_document=d(G);_breakpointSubscription;constructor(){this._breakpointSubscription=d(xc).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return to.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return to.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return to.BLACK_ON_WHITE}return to.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(d_,AE,kE),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===to.BLACK_ON_WHITE?e.add(d_,AE):i===to.WHITE_ON_BLACK&&e.add(d_,kE)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ec=(()=>{class t{constructor(){d(FE)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[fs]})}return t})();var iF=200,Km=class{_letterKeyStream=new E;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new E;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:iF;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(ot(e=>this._pressedLetters.push(e)),Xn(n),ae(()=>this._pressedLetters.length>0),V(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function We(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var hs=class{_items;_activeItemIndex=ne(-1);_activeItem=ne(null);_wrap=!1;_typeaheadSubscription=fe.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof li?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):nr(n)&&(this._effectRef=Xi(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new E;change=new E;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Km(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,a=this._getItemsArray().length;this._setActiveItemByIndex(o<a?o:a-1,-1);break}else return;default:(r||We(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return nr(this._items)?this._items():this._items instanceof li?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var ta=class extends hs{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Ic=class extends hs{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var m_={},Be=class t{_appId=d($r);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),m_.hasOwnProperty(n)||(m_[n]=0),`${n}${e?t._infix+"-":""}${m_[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};var HE=" ";function vs(t,n,e){let i=Jm(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(HE)))}function no(t,n,e){let i=Jm(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(HE)):t.removeAttribute(n)}function Jm(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var UE="cdk-describedby-message",Xm="cdk-describedby-host",p_=0,zE=(()=>{class t{_platform=d(ve);_document=d(G);_messageRegistry=new Map;_messagesContainer=null;_id=`${p_++}`;constructor(){d(mt).load(us),this._id=d($r)+"-"+p_++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=f_(i,r);typeof i!="string"?(BE(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=f_(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let a=this._messageRegistry.get(o);a&&a.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${Xm}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(Xm);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");BE(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(f_(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=Jm(e,"aria-describedby").filter(r=>r.indexOf(UE)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);vs(e,"aria-describedby",r.messageElement.id),e.setAttribute(Xm,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,no(e,"aria-describedby",r.messageElement.id),e.removeAttribute(Xm)}_isElementDescribedByMessage(e,i){let r=Jm(e,"aria-describedby"),o=this._messageRegistry.get(i),a=o&&o.messageElement.id;return!!a&&r.indexOf(a)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function f_(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function BE(t,n){t.id||(t.id=`${UE}-${n}-${p_++}`)}var yi=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(yi||{}),ef,na;function tf(){if(na==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return na=!1,na;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)na=!0;else{let t=Element.prototype.scrollTo;t?na=!/\{\s*\[native code\]\s*\}/.test(t.toString()):na=!1}}return na}function _s(){if(typeof document!="object"||!document)return yi.NORMAL;if(ef==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),ef=yi.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,ef=t.scrollLeft===0?yi.NEGATED:yi.INVERTED),t.remove()}return ef}function h_(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var bs,$E=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function g_(){if(bs)return bs;if(typeof document!="object"||!document)return bs=new Set($E),bs;let t=document.createElement("input");return bs=new Set($E.filter(n=>(t.setAttribute("type",n),t.type===n))),bs}var qE={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var rF=new y("MATERIAL_ANIMATIONS"),GE=null;function v_(){return d(rF,{optional:!0})?.animationsDisabled||d(Dl,{optional:!0})==="NoopAnimations"?"di-disabled":(GE??=d(ms).matchMedia("(prefers-reduced-motion)").matches,GE?"reduced-motion":"enabled")}function He(){return v_()!=="enabled"}function rt(t){return t==null?"":typeof t=="string"?t:`${t}px`}function et(t){return t!=null&&`${t}`!="false"}var qn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(qn||{}),__=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=qn.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},WE=ds({passive:!0,capture:!0}),b_=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,WE)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,WE)))}_delegateEventHandler=n=>{let e=wt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Mc={enterDuration:225,exitDuration:150},oF=800,YE=ds({passive:!0,capture:!0}),ZE=["mousedown","touchstart"],QE=["mouseup","mouseleave","touchend","touchcancel"],aF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2,changeDetection:0})}return t})(),ia=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new b_;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=mn(i)),o&&o.get(mt).load(aF)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=C(C({},Mc),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||sF(n,e,r),s=n-r.left,l=e-r.top,c=o.enterDuration,f=document.createElement("div");f.classList.add("mat-ripple-element"),f.style.left=`${s-a}px`,f.style.top=`${l-a}px`,f.style.height=`${a*2}px`,f.style.width=`${a*2}px`,i.color!=null&&(f.style.backgroundColor=i.color),f.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(f);let g=window.getComputedStyle(f),v=g.transitionProperty,b=g.transitionDuration,w=v==="none"||b==="0s"||b==="0s, 0s"||r.width===0&&r.height===0,j=new __(this,f,i,w);f.style.transform="scale3d(1, 1, 1)",j.state=qn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=j);let U=null;return!w&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let Y=()=>{U&&(U.fallbackTimer=null),clearTimeout(fn),this._finishRippleTransition(j)},Ue=()=>this._destroyRipple(j),fn=setTimeout(Ue,c+100);f.addEventListener("transitionend",Y),f.addEventListener("transitioncancel",Ue),U={onTransitionEnd:Y,onTransitionCancel:Ue,fallbackTimer:fn}}),this._activeRipples.set(j,U),(w||!c)&&this._finishRippleTransition(j),j}fadeOutRipple(n){if(n.state===qn.FADING_OUT||n.state===qn.HIDDEN)return;let e=n.element,i=C(C({},Mc),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=qn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=mn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,ZE.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{QE.forEach(e=>{this._triggerElement.addEventListener(e,this,YE)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===qn.FADING_IN?this._startFadeOutTransition(n):n.state===qn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=qn.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=qn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=_c(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+oF;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!bc(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===qn.VISIBLE||n.config.terminateOnPointerUp&&n.state===qn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(ZE.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(QE.forEach(e=>n.removeEventListener(e,this,YE)),this._pointerUpEventsRegistered=!1))}};function sF(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Sc=new y("mat-ripple-global-options"),KE=(()=>{class t{_elementRef=d(N);_animationsDisabled=He();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(F),i=d(ve),r=d(Sc,{optional:!0}),o=d(W);this._globalOptions=r||{},this._rippleRenderer=new ia(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:C(C(C({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,C(C({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,C(C({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var XE=(()=>{class t{_animationsDisabled=He();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&R("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
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
`],encapsulation:2,changeDetection:0})}return t})();var io=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2,changeDetection:0})}return t})();var lF=["text"],cF=[[["mat-icon"]],"*"],dF=["mat-icon","*"];function uF(t,n){if(t&1&&re(0,"mat-pseudo-checkbox",1),t&2){let e=T();_("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function mF(t,n){if(t&1&&re(0,"mat-pseudo-checkbox",3),t&2){let e=T();_("disabled",e.disabled)}}function fF(t,n){if(t&1&&(m(0,"span",4),p(1),u()),t&2){let e=T();h(),K("(",e.group.label,")")}}var Ac=new y("MAT_OPTION_PARENT_COMPONENT"),kc=new y("MatOptgroup");var Tc=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},wn=(()=>{class t{_element=d(N);_changeDetectorRef=d(ge);_parent=d(Ac,{optional:!0});group=d(kc,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Be).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=ne(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new P;_text;_stateChanges=new E;constructor(){let e=d(mt);e.load(io),e.load(us),this._signalDisableRipple=!!this._parent&&nr(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!We(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new Tc(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Ge(lF,7),i&2){let o;$(o=q())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&I("click",function(){return r._selectViaInteraction()})("keydown",function(a){return r._handleKeydown(a)}),i&2&&(bn("id",r.id),ce("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),R("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",J]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:dF,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Ce(cF),Fe(0,uF,1,2,"mat-pseudo-checkbox",1),H(1),m(2,"span",2,0),H(4,1),u(),Fe(5,mF,1,1,"mat-pseudo-checkbox",3),Fe(6,fF,2,1,"span",4),re(7,"div",5)),i&2&&(Pe(r.multiple?0:-1),h(5),Pe(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),h(),Pe(r.group&&r.group._inert?6:-1),h(),_("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[XE,KE],styles:[`.mat-mdc-option {
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
`],encapsulation:2,changeDetection:0})}return t})();function nf(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let a=0;a<t+1;a++)i[a].group&&i[a].group===r[o]&&o++;return o}return 0}function rf(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var pF=new y("cdk-dir-doc",{providedIn:"root",factory:()=>d(G)}),hF=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function JE(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?hF.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Et=(()=>{class t{get value(){return this.valueSignal()}valueSignal=ne("ltr");change=new P;constructor(){let e=d(pF,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(JE(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _e=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({})}return t})();var gF=20,dr=(()=>{class t{_ngZone=d(F);_platform=d(ve);_renderer=d(at).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new E;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=gF){return this._platform.isBrowser?new ie(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(vd(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Z()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(ae(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=mn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ur=(()=>{class t{elementRef=d(N);scrollDispatcher=d(dr);ngZone=d(F);dir=d(Et,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new E;_renderer=d(Se);_cleanupScroll;_elementScrolled=new E;constructor(){}ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&_s()!=yi.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),_s()==yi.INVERTED?e.left=e.right:_s()==yi.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;tf()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let a=this.dir&&this.dir.value=="rtl";return e=="start"?e=a?r:i:e=="end"&&(e=a?i:r),a&&_s()==yi.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:a&&_s()==yi.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),vF=20,Wn=(()=>{class t{_platform=d(ve);_listeners;_viewportSize=null;_change=new E;_document=d(G);constructor(){let e=d(F),i=d(at).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=vF){return e>0?this._change.pipe(vd(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Gn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({})}return t})(),y_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[_e,Gn,_e,Gn]})}return t})();var Oc=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},oo=class extends Oc{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},Ci=class extends Oc{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},C_=class extends Oc{element;constructor(n){super(),this.element=n instanceof N?n.nativeElement:n}},Cs=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof oo)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Ci)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof C_)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},of=class extends Cs{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Ri,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||W.NULL,o=r.get(Ae,i.injector);e=Gu(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var ra=(()=>{class t extends Cs{_moduleRef=d(Ri,{optional:!0});_document=d(G);_viewContainerRef=d(nt);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new P;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[be]})}return t})(),mr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({})}return t})();var eD=tf();function Es(t){return new af(t.get(Wn),t.get(G))}var af=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=rt(-this._previousScrollPosition.left),n.style.top=rt(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),eD&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),eD&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function sD(t,n){return new sf(t.get(dr),t.get(F),t.get(Wn),n)}var sf=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(ae(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Rc=class{enable(){}disable(){}attach(){}};function x_(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,a=t.left>e.right;return i||r||o||a})}function tD(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,a=t.right>e.right;return i||r||o||a})}function pr(t,n){return new lf(t.get(dr),t.get(Wn),t.get(F),n)}var lf=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();x_(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},lD=(()=>{class t{_injector=d(W);constructor(){}noop=()=>new Rc;close=e=>sD(this._injector,e);block=()=>Es(this._injector);reposition=e=>pr(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),fr=class{positionStrategy;scrollStrategy=new Rc;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var cf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var cD=(()=>{class t{_attachedOverlays=[];_document=d(G);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),dD=(()=>{class t extends cD{_ngZone=d(F);_renderer=d(at).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),uD=(()=>{class t extends cD{_platform=d(ve);_ngZone=d(F);_renderer=d(at).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=wt(e)};_clickListener=e=>{let i=wt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if(nD(s.overlayElement,i)||nD(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function nD(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var mD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
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
`],encapsulation:2,changeDetection:0})}return t})(),mf=(()=>{class t{_platform=d(ve);_containerElement;_document=d(G);_styleLoader=d(mt);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||h_()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),h_()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(mD)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),w_=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function E_(t){return t&&t.nodeType===1}var xs=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new E;_attachments=new E;_detachments=new E;_positionStrategy;_scrollStrategy;_locationChanges=fe.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new E;_outsidePointerEvents=new E;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,l,c,f=!1,g,v){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=f,this._injector=g,this._renderer=v,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=st(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=C(C({},this._config),n),this._updateElementSize()}setDirection(n){this._config=ee(C({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=rt(this._config.width),n.height=rt(this._config.height),n.minWidth=rt(this._config.minWidth),n.minHeight=rt(this._config.minHeight),n.maxWidth=rt(this._config.maxWidth),n.maxHeight=rt(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;E_(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new w_(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=cr(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=st(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},iD="cdk-overlay-connected-position-bounding-box",bF=/([A-Za-z%]+)$/;function aa(t,n){return new df(n,t.get(Wn),t.get(G),t.get(ve),t.get(mf))}var df=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new E;_resizeSubscription=fe.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(iD),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],a;for(let s of this._preferredPositions){let l=this._getOriginPoint(n,r,s),c=this._getOverlayPoint(l,e,s),f=this._getOverlayFit(c,e,i,s);if(f.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,l);return}if(this._canFitWithFlexibleDimensions(f,c,i)){o.push({position:s,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,s)});continue}(!a||a.overlayFit.visibleArea<f.visibleArea)&&(a={overlayFit:f,overlayPoint:c,originPoint:l,position:s,overlayRect:e})}if(o.length){let s=null,l=-1;for(let c of o){let f=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);f>l&&(l=f,s=c)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(a.position,a.originPoint);return}this._applyPosition(a.position,a.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&oa(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(iD),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof N?this._origin.nativeElement:E_(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let a=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;r=i.originX=="start"?a:s}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=oD(e),{x:a,y:s}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(a+=l),c&&(s+=c);let f=0-a,g=a+o.width-i.width,v=0-s,b=s+o.height-i.height,w=this._subtractOverflows(o.width,f,g),j=this._subtractOverflows(o.height,v,b),U=w*j;return{visibleArea:U,isCompletelyWithinViewport:o.width*o.height===U,fitsInViewportVertically:j===o.height,fitsInViewportHorizontally:w==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,a=rD(this._overlayRef.getConfig().minHeight),s=rD(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||a!=null&&a<=r,c=n.fitsInViewportHorizontally||s!=null&&s<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=oD(e),o=this._viewportRect,a=Math.max(n.x+r.width-o.width,0),s=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),f=0,g=0;return r.width<=o.width?f=c||-a:f=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?g=l||-s:g=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:f,y:g},{x:n.x+f,y:n.y+g}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!yF(this._lastScrollVisibility,i)){let r=new cf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,a,s;if(e.overlayY==="top")a=n.y,o=i.height-a+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-s+this._getViewportMarginTop();else{let b=Math.min(i.bottom-n.y+i.top,n.y),w=this._lastBoundingBoxSize.height;o=b*2,a=n.y-b,o>w&&!this._isInitialRender&&!this._growAfterOpen&&(a=n.y-w/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,f,g,v;if(c)v=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),f=n.x-this._getViewportMarginStart();else if(l)g=n.x,f=i.right-n.x-this._getViewportMarginEnd();else{let b=Math.min(i.right-n.x+i.left,n.x),w=this._lastBoundingBoxSize.width;f=b*2,g=n.x-b,f>w&&!this._isInitialRender&&!this._growAfterOpen&&(g=n.x-w/2)}return{top:a,left:g,bottom:s,right:v,width:f,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,a=this._overlayRef.getConfig().maxWidth;r.width=rt(i.width),r.height=rt(i.height),r.top=rt(i.top)||"auto",r.bottom=rt(i.bottom)||"auto",r.left=rt(i.left)||"auto",r.right=rt(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=rt(o)),a&&(r.maxWidth=rt(a))}this._lastBoundingBoxSize=i,oa(this._boundingBox.style,r)}_resetBoundingBoxStyles(){oa(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){oa(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,a=this._overlayRef.getConfig();if(r){let f=this._viewportRuler.getViewportScrollPosition();oa(i,this._getExactOverlayY(e,n,f)),oa(i,this._getExactOverlayX(e,n,f))}else i.position="static";let s="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(s+=`translateX(${l}px) `),c&&(s+=`translateY(${c}px)`),i.transform=s.trim(),a.maxHeight&&(r?i.maxHeight=rt(a.maxHeight):o&&(i.maxHeight="")),a.maxWidth&&(r?i.maxWidth=rt(a.maxWidth):o&&(i.maxWidth="")),oa(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let a=this._document.documentElement.clientHeight;r.bottom=`${a-(o.y+this._overlayRect.height)}px`}else r.top=rt(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let a;if(this._isRtl()?a=n.overlayX==="end"?"left":"right":a=n.overlayX==="end"?"right":"left",a==="right"){let s=this._document.documentElement.clientWidth;r.right=`${s-(o.x+this._overlayRect.width)}px`}else r.left=rt(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:tD(n,i),isOriginOutsideView:x_(n,i),isOverlayClipped:tD(e,i),isOverlayOutsideView:x_(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&cr(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof N)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function oa(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function rD(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(bF);return!e||e==="px"?parseFloat(n):null}return t||null}function oD(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function yF(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var aD="cdk-global-overlay-wrapper";function Ds(t){return new uf}var uf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(aD),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,l=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),f=this._xPosition,g=this._xOffset,v=this._overlayRef.getConfig().direction==="rtl",b="",w="",j="";l?j="flex-start":f==="center"?(j="center",v?w=g:b=g):v?f==="left"||f==="end"?(j="flex-end",b=g):(f==="right"||f==="start")&&(j="flex-start",w=g):f==="left"||f==="start"?(j="flex-start",b=g):(f==="right"||f==="end")&&(j="flex-end",w=g),n.position=this._cssPosition,n.marginLeft=l?"0":b,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":w,e.justifyContent=j,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(aD),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},fD=(()=>{class t{_injector=d(W);constructor(){}global(){return Ds()}flexibleConnectedTo(e){return aa(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Nc=new y("OVERLAY_DEFAULT_CONFIG");function hr(t,n){t.get(mt).load(mD);let e=t.get(mf),i=t.get(G),r=t.get(Be),o=t.get(kn),a=t.get(Et),s=t.get(Se,null,{optional:!0})||t.get(at).createRenderer(null,null),l=new fr(n),c=t.get(Nc,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,"showPopover"in i.body?l.usePopover=n?.usePopover??c:l.usePopover=!1;let f=i.createElement("div"),g=i.createElement("div");f.id=r.getId("cdk-overlay-"),f.classList.add("cdk-overlay-pane"),g.appendChild(f),l.usePopover&&(g.setAttribute("popover","manual"),g.classList.add("cdk-overlay-popover"));let v=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return E_(v)?v.after(g):v?.type==="parent"?v.element.appendChild(g):e.getContainerElement().appendChild(g),new xs(new of(f,o,t),g,f,l,t.get(F),t.get(dD),i,t.get(Gr),t.get(uD),n?.disableAnimations??t.get(Dl,null,{optional:!0})==="NoopAnimations",t.get(Ae),s)}var pD=(()=>{class t{scrollStrategies=d(lD);_positionBuilder=d(fD);_injector=d(W);constructor(){}create(e){return hr(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),CF=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],xF=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>pr(t)}}),ws=(()=>{class t{elementRef=d(N);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),hD=new y("cdk-connected-overlay-default-config"),ff=(()=>{class t{_dir=d(Et,{optional:!0});_injector=d(W);_overlayRef;_templatePortal;_backdropSubscription=fe.EMPTY;_attachSubscription=fe.EMPTY;_detachSubscription=fe.EMPTY;_positionSubscription=fe.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(xF);_ngZone=d(F);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new P;positionChange=new P;attach=new P;detach=new P;overlayKeydown=new P;overlayOutsideClick=new P;constructor(){let e=d(kt),i=d(nt),r=d(hD,{optional:!0}),o=d(Nc,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Ci(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=CF);let e=this._overlayRef=hr(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!We(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=wt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new fr({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=aa(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof ws?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof ws?this.origin.elementRef.nativeElement:this.origin instanceof N?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(cp(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",J],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",J],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",J],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",J],push:[2,"cdkConnectedOverlayPush","push",J],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",J],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",J],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[je]})}return t})(),xi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({providers:[pD],imports:[_e,mr,y_,y_]})}return t})();var D_=class{_box;_destroyed=new E;_resizeSubject=new E;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ie(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(ae(e=>e.some(i=>i.target===n)),yd({bufferSize:1,refCount:!0}),ye(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},gD=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(F);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new D_(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var wF=["notch"],EF=["matFormFieldNotchedOutline",""],DF=["*"],vD=["iconPrefixContainer"],_D=["textPrefixContainer"],bD=["iconSuffixContainer"],yD=["textSuffixContainer"],IF=["textField"],MF=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],SF=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function TF(t,n){t&1&&re(0,"span",21)}function AF(t,n){if(t&1&&(m(0,"label",20),H(1,1),Fe(2,TF,1,0,"span",21),u()),t&2){let e=T(2);_("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),ce("for",e._control.disableAutomaticLabeling?null:e._control.id),h(2),Pe(!e.hideRequiredMarker&&e._control.required?2:-1)}}function kF(t,n){if(t&1&&Fe(0,AF,3,5,"label",20),t&2){let e=T();Pe(e._hasFloatingLabel()?0:-1)}}function OF(t,n){t&1&&re(0,"div",7)}function RF(t,n){}function NF(t,n){if(t&1&&M(0,RF,0,0,"ng-template",13),t&2){T(2);let e=yt(1);_("ngTemplateOutlet",e)}}function FF(t,n){if(t&1&&(m(0,"div",9),Fe(1,NF,1,1,null,13),u()),t&2){let e=T();_("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),h(),Pe(e._forceDisplayInfixLabel()?-1:1)}}function PF(t,n){t&1&&(m(0,"div",10,2),H(2,2),u())}function LF(t,n){t&1&&(m(0,"div",11,3),H(2,3),u())}function jF(t,n){}function VF(t,n){if(t&1&&M(0,jF,0,0,"ng-template",13),t&2){T();let e=yt(1);_("ngTemplateOutlet",e)}}function BF(t,n){t&1&&(m(0,"div",14,4),H(2,4),u())}function HF(t,n){t&1&&(m(0,"div",15,5),H(2,5),u())}function UF(t,n){t&1&&re(0,"div",16)}function zF(t,n){t&1&&(m(0,"div",18),H(1,6),u())}function $F(t,n){if(t&1&&(m(0,"mat-hint",22),p(1),u()),t&2){let e=T(2);_("id",e._hintLabelId),h(),z(e.hintLabel)}}function qF(t,n){if(t&1&&(m(0,"div",19),Fe(1,$F,2,2,"mat-hint",22),H(2,7),re(3,"div",23),H(4,8),u()),t&2){let e=T();h(),Pe(e.hintLabel?1:-1)}}var ft=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-label"]]})}return t})(),MD=new y("MatError"),Kt=(()=>{class t{id=d(Be).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&bn("id",r.id)},inputs:{id:"id"},features:[xe([{provide:MD,useExisting:t}])]})}return t})(),hf=(()=>{class t{align="start";id=d(Be).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(bn("id",r.id),ce("align",null),R("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),GF=new y("MatPrefix");var SD=new y("MatSuffix"),Xt=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[xe([{provide:SD,useExisting:t}])]})}return t})(),TD=new y("FloatingLabelParent"),CD=(()=>{class t{_elementRef=d(N);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(gD);_ngZone=d(F);_parent=d(TD);_resizeSubscription=new fe;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return WF(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&R("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function WF(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var xD="mdc-line-ripple--active",pf="mdc-line-ripple--deactivating",wD=(()=>{class t{_elementRef=d(N);_cleanupTransitionEnd;constructor(){let e=d(F),i=d(Se);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(pf),e.add(xD)}deactivate(){this._elementRef.nativeElement.classList.add(pf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(pf);e.propertyName==="opacity"&&r&&i.remove(xD,pf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),ED=(()=>{class t{_elementRef=d(N);_ngZone=d(F);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Ge(wF,5),i&2){let o;$(o=q())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&R("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:EF,ngContentSelectors:DF,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Ce(),_n(0,"div",1),lt(1,"div",2,0),H(3),Rt(),_n(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),Fc=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t})}return t})();var sa=new y("MatFormField"),YF=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),DD="fill",ZF="auto",ID="fixed",QF="translateY(-50%)",Dt=(()=>{class t{_elementRef=d(N);_changeDetectorRef=d(ge);_platform=d(ve);_idGenerator=d(Be);_ngZone=d(F);_defaults=d(YF,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Fl("iconPrefixContainer");_textPrefixContainerSignal=Fl("textPrefixContainer");_iconSuffixContainerSignal=Fl("iconSuffixContainer");_textSuffixContainerSignal=Fl("textSuffixContainer");_prefixSuffixContainers=cn(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=vx(ft);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=et(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||ZF}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||DD;this._appearanceSignal.set(i)}_appearanceSignal=ne(DD);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||ID}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||ID}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new E;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=He();constructor(){let e=this._defaults,i=d(Et);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Xi(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=cn(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(Me([void 0,void 0]),V(()=>[i.errorState,i.userAriaDescribedBy]),bd(),ae(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(ye(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),_t(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){yx({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=cn(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,f=this._currentDirection==="rtl"?"-1":"1",g=`${a+s}px`,b=`calc(${f} * (${g} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,w=`var(--mat-mdc-form-field-label-transform, ${QF} translateX(${b}))`,j=a+s+l+c;return[w,j]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(ju(o,r._labelChild,ft,5),it(o,Fc,5)(o,GF,5)(o,SD,5)(o,MD,5)(o,hf,5)),i&2){Bu();let a;$(a=q())&&(r._formFieldControl=a.first),$(a=q())&&(r._prefixChildren=a),$(a=q())&&(r._suffixChildren=a),$(a=q())&&(r._errorChildren=a),$(a=q())&&(r._hintChildren=a)}},viewQuery:function(i,r){if(i&1&&(Vu(r._iconPrefixContainerSignal,vD,5)(r._textPrefixContainerSignal,_D,5)(r._iconSuffixContainerSignal,bD,5)(r._textSuffixContainerSignal,yD,5),Ge(IF,5)(vD,5)(_D,5)(bD,5)(yD,5)(CD,5)(ED,5)(wD,5)),i&2){Bu(4);let o;$(o=q())&&(r._textField=o.first),$(o=q())&&(r._iconPrefixContainer=o.first),$(o=q())&&(r._textPrefixContainer=o.first),$(o=q())&&(r._iconSuffixContainer=o.first),$(o=q())&&(r._textSuffixContainer=o.first),$(o=q())&&(r._floatingLabel=o.first),$(o=q())&&(r._notchedOutline=o.first),$(o=q())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&R("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[xe([{provide:sa,useExisting:t},{provide:TD,useExisting:t}])],ngContentSelectors:SF,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Ce(MF),M(0,kF,1,1,"ng-template",null,0,Ha),m(2,"div",6,1),I("click",function(a){return r._control.onContainerClick(a)}),Fe(4,OF,1,0,"div",7),m(5,"div",8),Fe(6,FF,2,2,"div",9),Fe(7,PF,3,0,"div",10),Fe(8,LF,3,0,"div",11),m(9,"div",12),Fe(10,VF,1,1,null,13),H(11),u(),Fe(12,BF,3,0,"div",14),Fe(13,HF,3,0,"div",15),u(),Fe(14,UF,1,0,"div",16),u(),m(15,"div",17),Fe(16,zF,2,0,"div",18)(17,qF,5,1,"div",19),u()),i&2){let o;h(2),R("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),h(2),Pe(!r._hasOutline()&&!r._control.disabled?4:-1),h(2),Pe(r._hasOutline()?6:-1),h(),Pe(r._hasIconPrefix?7:-1),h(),Pe(r._hasTextPrefix?8:-1),h(2),Pe(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),h(2),Pe(r._hasTextSuffix?12:-1),h(),Pe(r._hasIconSuffix?13:-1),h(),Pe(r._hasOutline()?-1:14),h(),R("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let a=r._getSubscriptMessageType();h(),Pe((o=a)==="error"?16:o==="hint"?17:-1)}},dependencies:[CD,ED,Ll,wD,hf],styles:[`.mdc-text-field {
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
`],encapsulation:2,changeDetection:0})}return t})();var Is=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[_e]})}return t})();var gf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[_e]})}return t})();var Ms=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[Is,gf,wn,_e]})}return t})();var KF=["panel"],XF=["*"];function JF(t,n){if(t&1&&(lt(0,"div",1,0),H(2),Rt()),t&2){let e=n.id,i=T();ln(i._classList),R("mat-mdc-autocomplete-visible",i.showPanel)("mat-mdc-autocomplete-hidden",!i.showPanel)("mat-autocomplete-panel-animations-enabled",!i._animationsDisabled)("mat-primary",i._color==="primary")("mat-accent",i._color==="accent")("mat-warn",i._color==="warn"),bn("id",i.id),ce("aria-label",i.ariaLabel||null)("aria-labelledby",i._getPanelAriaLabelledby(e))}}var I_=class{source;option;constructor(n,e){this.source=n,this.option=e}},AD=new y("mat-autocomplete-default-options",{providedIn:"root",factory:()=>({autoActiveFirstOption:!1,autoSelectActiveOption:!1,hideSingleSelectionIndicator:!1,requireSelection:!1,hasBackdrop:!1})}),vf=(()=>{class t{_changeDetectorRef=d(ge);_elementRef=d(N);_defaults=d(AD);_animationsDisabled=He();_activeOptionChanges=fe.EMPTY;_keyManager;showPanel=!1;get isOpen(){return this._isOpen&&this.showPanel}_isOpen=!1;_latestOpeningTrigger;_setColor(e){this._color=e,this._changeDetectorRef.markForCheck()}_color;template;panel;options;optionGroups;ariaLabel;ariaLabelledby;displayWith=null;autoActiveFirstOption;autoSelectActiveOption;requireSelection;panelWidth;disableRipple=!1;optionSelected=new P;opened=new P;closed=new P;optionActivated=new P;set classList(e){this._classList=e,this._elementRef.nativeElement.className=""}_classList;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator;_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}id=d(Be).getId("mat-autocomplete-");inertGroups;constructor(){let e=d(ve);this.inertGroups=e?.SAFARI||!1,this.autoActiveFirstOption=!!this._defaults.autoActiveFirstOption,this.autoSelectActiveOption=!!this._defaults.autoSelectActiveOption,this.requireSelection=!!this._defaults.requireSelection,this._hideSingleSelectionIndicator=this._defaults.hideSingleSelectionIndicator??!1}ngAfterContentInit(){this._keyManager=new ta(this.options).withWrap().skipPredicate(this._skipPredicate),this._activeOptionChanges=this._keyManager.change.subscribe(e=>{this.isOpen&&this.optionActivated.emit({source:this,option:this.options.toArray()[e]||null})}),this._setVisibility()}ngOnDestroy(){this._keyManager?.destroy(),this._activeOptionChanges.unsubscribe()}_setScrollTop(e){this.panel&&(this.panel.nativeElement.scrollTop=e)}_getScrollTop(){return this.panel?this.panel.nativeElement.scrollTop:0}_setVisibility(){this.showPanel=!!this.options?.length,this._changeDetectorRef.markForCheck()}_emitSelectEvent(e){let i=new I_(this,e);this.optionSelected.emit(i)}_getPanelAriaLabelledby(e){if(this.ariaLabel)return null;let i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_skipPredicate(){return!1}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-autocomplete"]],contentQueries:function(i,r,o){if(i&1&&it(o,wn,5)(o,kc,5),i&2){let a;$(a=q())&&(r.options=a),$(a=q())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&Ge(kt,7)(KF,5),i&2){let o;$(o=q())&&(r.template=o.first),$(o=q())&&(r.panel=o.first)}},hostAttrs:[1,"mat-mdc-autocomplete"],inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],displayWith:"displayWith",autoActiveFirstOption:[2,"autoActiveFirstOption","autoActiveFirstOption",J],autoSelectActiveOption:[2,"autoSelectActiveOption","autoSelectActiveOption",J],requireSelection:[2,"requireSelection","requireSelection",J],panelWidth:"panelWidth",disableRipple:[2,"disableRipple","disableRipple",J],classList:[0,"class","classList"],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",J]},outputs:{optionSelected:"optionSelected",opened:"opened",closed:"closed",optionActivated:"optionActivated"},exportAs:["matAutocomplete"],features:[xe([{provide:Ac,useExisting:t}])],ngContentSelectors:XF,decls:1,vars:0,consts:[["panel",""],["role","listbox",1,"mat-mdc-autocomplete-panel","mdc-menu-surface","mdc-menu-surface--open",3,"id"]],template:function(i,r){i&1&&(Ce(),Ru(0,JF,3,17,"ng-template"))},styles:[`div.mat-mdc-autocomplete-panel {
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
`],encapsulation:2,changeDetection:0})}return t})();var eP={provide:hc,useExisting:hn(()=>Pc),multi:!0};var tP=new y("mat-autocomplete-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>pr(t)}}),Pc=(()=>{class t{_environmentInjector=d(Ae);_element=d(N);_injector=d(W);_viewContainerRef=d(nt);_zone=d(F);_changeDetectorRef=d(ge);_dir=d(Et,{optional:!0});_formField=d(sa,{optional:!0,host:!0});_viewportRuler=d(Wn);_scrollStrategy=d(tP);_renderer=d(Se);_animationsDisabled=He();_defaults=d(AD,{optional:!0});_overlayRef=null;_portal;_componentDestroyed=!1;_initialized=new E;_keydownSubscription;_outsideClickSubscription;_cleanupWindowBlur;_previousValue=null;_valueOnAttach=null;_valueOnLastKeydown=null;_positionStrategy;_manuallyFloatingLabel=!1;_closingActionsSubscription;_viewportSubscription=fe.EMPTY;_breakpointObserver=d(xc);_handsetLandscapeSubscription=fe.EMPTY;_canOpenOnNextFocus=!0;_valueBeforeAutoSelection;_pendingAutoselectedOption=null;_closeKeyEventStream=new E;_overlayPanelClass=cr(this._defaults?.overlayPanelClass||[]);_windowBlurHandler=()=>{this._canOpenOnNextFocus=this.panelOpen||!this._hasFocus()};_onChange=()=>{};_onTouched=()=>{};autocomplete;position="auto";connectedTo;autocompleteAttribute="off";autocompleteDisabled=!1;constructor(){}_aboveClass="mat-mdc-autocomplete-panel-above";ngAfterViewInit(){this._initialized.next(),this._initialized.complete(),this._cleanupWindowBlur=this._renderer.listen("window","blur",this._windowBlurHandler)}ngOnChanges(e){e.position&&this._positionStrategy&&(this._setStrategyPositions(this._positionStrategy),this.panelOpen&&this._overlayRef.updatePosition())}ngOnDestroy(){this._cleanupWindowBlur?.(),this._handsetLandscapeSubscription.unsubscribe(),this._viewportSubscription.unsubscribe(),this._componentDestroyed=!0,this._destroyPanel(),this._closeKeyEventStream.complete(),this._clearFromModal()}get panelOpen(){return this._overlayAttached&&this.autocomplete.showPanel}_overlayAttached=!1;openPanel(){this._openPanelInternal()}closePanel(){this._resetLabel(),this._overlayAttached&&(this.panelOpen&&this._zone.run(()=>{this.autocomplete.closed.emit()}),this.autocomplete._latestOpeningTrigger===this&&(this.autocomplete._isOpen=!1,this.autocomplete._latestOpeningTrigger=null),this._overlayAttached=!1,this._pendingAutoselectedOption=null,this._overlayRef&&this._overlayRef.hasAttached()&&(this._overlayRef.detach(),this._closingActionsSubscription.unsubscribe()),this._updatePanelState(),this._componentDestroyed||this._changeDetectorRef.detectChanges(),this._trackedModal&&no(this._trackedModal,"aria-owns",this.autocomplete.id))}updatePosition(){this._overlayAttached&&this._overlayRef.updatePosition()}get panelClosingActions(){return _t(this.optionSelections,this.autocomplete._keyManager.tabOut.pipe(ae(()=>this._overlayAttached)),this._closeKeyEventStream,this._getOutsideClickStream(),this._overlayRef?this._overlayRef.detachments().pipe(ae(()=>this._overlayAttached)):Z()).pipe(V(e=>e instanceof Tc?e:null))}optionSelections=Tn(()=>{let e=this.autocomplete?this.autocomplete.options:null;return e?e.changes.pipe(Me(e),Ze(()=>_t(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(Ze(()=>this.optionSelections))});get activeOption(){return this.autocomplete&&this.autocomplete._keyManager?this.autocomplete._keyManager.activeItem:null}_getOutsideClickStream(){return new ie(e=>{let i=o=>{let a=wt(o),s=this._formField?this._formField.getConnectedOverlayOrigin().nativeElement:null,l=this.connectedTo?this.connectedTo.elementRef.nativeElement:null;this._overlayAttached&&a!==this._element.nativeElement&&!this._hasFocus()&&(!s||!s.contains(a))&&(!l||!l.contains(a))&&this._overlayRef&&!this._overlayRef.overlayElement.contains(a)&&e.next(o)},r=[this._renderer.listen("document","click",i),this._renderer.listen("document","auxclick",i),this._renderer.listen("document","touchend",i)];return()=>{r.forEach(o=>o())}})}writeValue(e){Promise.resolve(null).then(()=>this._assignOptionValue(e))}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this._element.nativeElement.disabled=e}_handleKeydown(e){let i=e,r=i.keyCode,o=We(i);if(r===27&&!o&&i.preventDefault(),this._valueOnLastKeydown=this._element.nativeElement.value,this.activeOption&&r===13&&this.panelOpen&&!o)this.activeOption._selectViaInteraction(),this._resetActiveItem(),i.preventDefault();else if(this.autocomplete){let a=this.autocomplete._keyManager.activeItem,s=r===38||r===40;r===9||s&&!o&&this.panelOpen?this.autocomplete._keyManager.onKeydown(i):s&&this._canOpen()&&this._openPanelInternal(this._valueOnLastKeydown),(s||this.autocomplete._keyManager.activeItem!==a)&&(this._scrollToOption(this.autocomplete._keyManager.activeItemIndex||0),this.autocomplete.autoSelectActiveOption&&this.activeOption&&(this._pendingAutoselectedOption||(this._valueBeforeAutoSelection=this._valueOnLastKeydown),this._pendingAutoselectedOption=this.activeOption,this._assignOptionValue(this.activeOption.value)))}}_handleInput(e){let i=e.target,r=i.value;if(i.type==="number"&&(r=r==""?null:parseFloat(r)),this._previousValue!==r){if(this._previousValue=r,this._pendingAutoselectedOption=null,(!this.autocomplete||!this.autocomplete.requireSelection)&&this._onChange(r),!r)this._clearPreviousSelectedOption(null,!1);else if(this.panelOpen&&!this.autocomplete.requireSelection){let o=this.autocomplete.options?.find(a=>a.selected);if(o){let a=this._getDisplayValue(o.value);r!==a&&o.deselect(!1)}}if(this._canOpen()&&this._hasFocus()){let o=this._valueOnLastKeydown??this._element.nativeElement.value;this._valueOnLastKeydown=null,this._openPanelInternal(o)}}}_handleFocus(){this._canOpenOnNextFocus?this._canOpen()&&(this._previousValue=this._element.nativeElement.value,this._attachOverlay(this._previousValue),this._floatLabel(!0)):this._canOpenOnNextFocus=!0}_handleClick(){this._canOpen()&&!this.panelOpen&&this._openPanelInternal()}_hasFocus(){return eo()===this._element.nativeElement}_floatLabel(e=!1){this._formField&&this._formField.floatLabel==="auto"&&(e?this._formField._animateAndLockLabel():this._formField.floatLabel="always",this._manuallyFloatingLabel=!0)}_resetLabel(){this._manuallyFloatingLabel&&(this._formField&&(this._formField.floatLabel="auto"),this._manuallyFloatingLabel=!1)}_subscribeToClosingActions(){let e=new ie(r=>{st(()=>{r.next()},{injector:this._environmentInjector})}),i=this.autocomplete.options?.changes.pipe(ot(()=>this._positionStrategy.reapplyLastPosition()),sp(0))??Z();return _t(e,i).pipe(Ze(()=>this._zone.run(()=>{let r=this.panelOpen;return this._resetActiveItem(),this._updatePanelState(),this._changeDetectorRef.detectChanges(),this.panelOpen&&this._overlayRef.updatePosition(),r!==this.panelOpen&&(this.panelOpen?this._emitOpened():this.autocomplete.closed.emit()),this.panelClosingActions})),Oe(1)).subscribe(r=>this._setValueAndClose(r))}_emitOpened(){this.autocomplete.opened.emit()}_destroyPanel(){this._overlayRef&&(this.closePanel(),this._overlayRef.dispose(),this._overlayRef=null)}_getDisplayValue(e){let i=this.autocomplete;return i&&i.displayWith?i.displayWith(e):e}_assignOptionValue(e){let i=this._getDisplayValue(e);e==null&&this._clearPreviousSelectedOption(null,!1),this._updateNativeInputValue(i??"")}_updateNativeInputValue(e){this._formField?this._formField._control.value=e:this._element.nativeElement.value=e,this._previousValue=e}_setValueAndClose(e){let i=this.autocomplete,r=e?e.source:this._pendingAutoselectedOption;r?(this._clearPreviousSelectedOption(r),this._assignOptionValue(r.value),this._onChange(r.value),i._emitSelectEvent(r),this._element.nativeElement.focus()):i.requireSelection&&this._element.nativeElement.value!==this._valueOnAttach&&(this._clearPreviousSelectedOption(null),this._assignOptionValue(null),this._onChange(null)),this.closePanel()}_clearPreviousSelectedOption(e,i){this.autocomplete?.options?.forEach(r=>{r!==e&&r.selected&&r.deselect(i)})}_openPanelInternal(e=this._element.nativeElement.value){if(this._attachOverlay(e),this._floatLabel(),this._trackedModal){let i=this.autocomplete.id;vs(this._trackedModal,"aria-owns",i)}}_attachOverlay(e){if(!this.autocomplete)return;let i=this._overlayRef;i?(this._positionStrategy.setOrigin(this._getConnectedElement()),i.updateSize({width:this._getPanelWidth()})):(this._portal=new Ci(this.autocomplete.template,this._viewContainerRef,{id:this._formField?.getLabelId()}),i=hr(this._injector,this._getOverlayConfig()),this._overlayRef=i,this._viewportSubscription=this._viewportRuler.change().subscribe(()=>{this.panelOpen&&i&&i.updateSize({width:this._getPanelWidth()})}),this._handsetLandscapeSubscription=this._breakpointObserver.observe(qE.HandsetLandscape).subscribe(o=>{o.matches?this._positionStrategy.withFlexibleDimensions(!0).withGrowAfterOpen(!0).withViewportMargin(8):this._positionStrategy.withFlexibleDimensions(!1).withGrowAfterOpen(!1).withViewportMargin(0)})),i&&!i.hasAttached()&&(i.attach(this._portal),this._valueOnAttach=e,this._valueOnLastKeydown=null,this._closingActionsSubscription=this._subscribeToClosingActions());let r=this.panelOpen;this.autocomplete._isOpen=this._overlayAttached=!0,this.autocomplete._latestOpeningTrigger=this,this.autocomplete._setColor(this._formField?.color),this._updatePanelState(),this._applyModalPanelOwnership(),this.panelOpen&&r!==this.panelOpen&&this._emitOpened()}_handlePanelKeydown=e=>{(e.keyCode===27&&!We(e)||e.keyCode===38&&We(e,"altKey"))&&(this._pendingAutoselectedOption&&(this._updateNativeInputValue(this._valueBeforeAutoSelection??""),this._pendingAutoselectedOption=null),this._closeKeyEventStream.next(),this._resetActiveItem(),e.stopPropagation(),e.preventDefault())};_updatePanelState(){if(this.autocomplete._setVisibility(),this.panelOpen){let e=this._overlayRef;this._keydownSubscription||(this._keydownSubscription=e.keydownEvents().subscribe(this._handlePanelKeydown)),this._outsideClickSubscription||(this._outsideClickSubscription=e.outsidePointerEvents().subscribe())}else this._keydownSubscription?.unsubscribe(),this._outsideClickSubscription?.unsubscribe(),this._keydownSubscription=this._outsideClickSubscription=void 0}_getOverlayConfig(){return new fr({positionStrategy:this._getOverlayPosition(),scrollStrategy:this._scrollStrategy(),width:this._getPanelWidth(),direction:this._dir??void 0,hasBackdrop:this._defaults?.hasBackdrop,backdropClass:this._defaults?.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:this._overlayPanelClass,disableAnimations:this._animationsDisabled})}_getOverlayPosition(){let e=aa(this._injector,this._getConnectedElement()).withFlexibleDimensions(!1).withPush(!1).withPopoverLocation("inline");return this._setStrategyPositions(e),this._positionStrategy=e,e}_setStrategyPositions(e){let i=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],r=this._aboveClass,o=[{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:r},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:r}],a;this.position==="above"?a=o:this.position==="below"?a=i:a=[...i,...o],e.withPositions(a)}_getConnectedElement(){return this.connectedTo?this.connectedTo.elementRef:this._formField?this._formField.getConnectedOverlayOrigin():this._element}_getPanelWidth(){return this.autocomplete.panelWidth||this._getHostWidth()}_getHostWidth(){return this._getConnectedElement().nativeElement.getBoundingClientRect().width}_resetActiveItem(){let e=this.autocomplete;if(e.autoActiveFirstOption){let i=-1;for(let r=0;r<e.options.length;r++)if(!e.options.get(r).disabled){i=r;break}e._keyManager.setActiveItem(i)}else e._keyManager.setActiveItem(-1)}_canOpen(){let e=this._element.nativeElement;return!e.readOnly&&!e.disabled&&!this.autocompleteDisabled}_scrollToOption(e){let i=this.autocomplete,r=nf(e,i.options,i.optionGroups);if(e===0&&r===1)i._setScrollTop(0);else if(i.panel){let o=i.options.toArray()[e];if(o){let a=o._getHostElement(),s=rf(a.offsetTop,a.offsetHeight,i._getScrollTop(),i.panel.nativeElement.offsetHeight);i._setScrollTop(s)}}}_trackedModal=null;_applyModalPanelOwnership(){let e=this._element.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=this.autocomplete.id;this._trackedModal&&no(this._trackedModal,"aria-owns",i),vs(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(this._trackedModal){let e=this.autocomplete.id;no(this._trackedModal,"aria-owns",e),this._trackedModal=null}}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["input","matAutocomplete",""],["textarea","matAutocomplete",""]],hostAttrs:[1,"mat-mdc-autocomplete-trigger"],hostVars:7,hostBindings:function(i,r){i&1&&I("focusin",function(){return r._handleFocus()})("blur",function(){return r._onTouched()})("input",function(a){return r._handleInput(a)})("keydown",function(a){return r._handleKeydown(a)})("click",function(){return r._handleClick()}),i&2&&ce("autocomplete",r.autocompleteAttribute)("role",r.autocompleteDisabled?null:"combobox")("aria-autocomplete",r.autocompleteDisabled?null:"list")("aria-activedescendant",r.panelOpen&&r.activeOption?r.activeOption.id:null)("aria-expanded",r.autocompleteDisabled?null:r.panelOpen.toString())("aria-controls",r.autocompleteDisabled||!r.panelOpen||r.autocomplete==null?null:r.autocomplete.id)("aria-haspopup",r.autocompleteDisabled?null:"listbox")},inputs:{autocomplete:[0,"matAutocomplete","autocomplete"],position:[0,"matAutocompletePosition","position"],connectedTo:[0,"matAutocompleteConnectedTo","connectedTo"],autocompleteAttribute:[0,"autocomplete","autocompleteAttribute"],autocompleteDisabled:[2,"matAutocompleteDisabled","autocompleteDisabled",J]},exportAs:["matAutocompleteTrigger"],features:[xe([eP]),je]})}return t})(),Pt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[xi,Ms,Gn,Ms,_e]})}return t})();var nP=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
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
`],encapsulation:2,changeDetection:0})}return t})(),iP={passive:!0},kD=(()=>{class t{_platform=d(ve);_ngZone=d(F);_renderer=d(at).createRenderer(null,null);_styleLoader=d(mt);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return Ye;this._styleLoader.load(nP);let i=mn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new E,a="cdk-text-field-autofilled",s=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(a)?(i.classList.add(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(a)&&(i.classList.remove(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",s,iP)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=mn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var OD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({})}return t})();var RD=new y("MAT_INPUT_VALUE_ACCESSOR");var _f=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ss=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var Xe=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[fs,Dt,_e]})}return t})();var oP=["button","checkbox","file","hidden","image","radio","range","reset","submit"],aP=new y("MAT_INPUT_CONFIG"),Jt=(()=>{class t{_elementRef=d(N);_platform=d(ve);ngControl=d(_i,{optional:!0,self:!0});_autofillMonitor=d(kD);_ngZone=d(F);_formField=d(sa,{optional:!0});_renderer=d(Se);_uid=d(Be).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(aP,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new E;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=et(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(le.required)??!1}set required(e){this._required=et(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&g_().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=et(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>g_().has(e));constructor(){let e=d(gc,{optional:!0}),i=d(xt,{optional:!0}),r=d(_f),o=d(RD,{optional:!0,self:!0}),a=this._elementRef.nativeElement,s=a.nodeName.toLowerCase();o?nr(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Ss(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=s==="select",this._isTextarea=s==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Xi(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){oP.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&I("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(bn("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),ce("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),R("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",J]},exportAs:["matInput"],features:[xe([{provide:Fc,useExisting:t}]),je]})}return t})(),Lt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[Xe,Xe,OD,_e]})}return t})();var sP={capture:!0},lP=["focus","mousedown","mouseenter","touchstart"],M_="mat-ripple-loader-uninitialized",S_="mat-ripple-loader-class-name",ND="mat-ripple-loader-centered",bf="mat-ripple-loader-disabled",FD=(()=>{class t{_document=d(G);_animationsDisabled=He();_globalRippleOptions=d(Sc,{optional:!0});_platform=d(ve);_ngZone=d(F);_injector=d(W);_eventCleanups;_hosts=new Map;constructor(){let e=d(at).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>lP.map(i=>e.listen(this._document,i,this._onInteraction,sP)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(M_,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(S_))&&e.setAttribute(S_,i.className||""),i.centered&&e.setAttribute(ND,""),i.disabled&&e.setAttribute(bf,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(bf,""):e.removeAttribute(bf)}_onInteraction=e=>{let i=wt(e);if(i instanceof HTMLElement){let r=i.closest(`[${M_}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(S_)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Mc.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??Mc.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(bf),rippleConfig:{centered:e.hasAttribute(ND),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new ia(s,this._ngZone,i,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(M_)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var cP=["mat-icon-button",""],dP=["*"],uP=new y("MAT_BUTTON_CONFIG");function PD(t){return t==null?void 0:pi(t)}var T_=(()=>{class t{_elementRef=d(N);_ngZone=d(F);_animationsDisabled=He();_config=d(uP,{optional:!0});_focusMonitor=d(bi);_cleanupClick;_renderer=d(Se);_rippleLoader=d(FD);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(mt).load(io);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(ce("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),ln(r.color?"mat-"+r.color:""),R("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",J],disabled:[2,"disabled","disabled",J],ariaDisabled:[2,"aria-disabled","ariaDisabled",J],disabledInteractive:[2,"disabledInteractive","disabledInteractive",J],tabIndex:[2,"tabIndex","tabIndex",PD],_tabindex:[2,"tabindex","_tabindex",PD]}})}return t})(),Fn=(()=>{class t extends T_{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[be],attrs:cP,ngContentSelectors:dP,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ce(),_n(0,"span",0),H(1),_n(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
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
`],encapsulation:2,changeDetection:0})}return t})();var mP=["matButton",""],fP=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],pP=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var LD=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),en=(()=>{class t extends T_{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=hP(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?LD.get(this._appearance):null,o=LD.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[be],attrs:mP,ngContentSelectors:pP,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ce(fP),_n(0,"span",0),H(1),lt(2,"span",1),H(3,1),Rt(),H(4,2),_n(5,"span",2)(6,"span",3)),i&2&&R("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2,changeDetection:0})}return t})();function hP(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var jt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[Is,_e]})}return t})();var gP=["*"];var vP=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],_P=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],bP=new y("MAT_CARD_CONFIG"),tn=(()=>{class t{appearance;constructor(){let e=d(bP,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&R("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:gP,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),H(0))},styles:[`.mat-mdc-card {
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
`],encapsulation:2,changeDetection:0})}return t})(),En=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var nn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})(),Dn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-subtitle"],["","mat-card-subtitle",""],["","matCardSubtitle",""]],hostAttrs:[1,"mat-mdc-card-subtitle"]})}return t})(),Zn=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})(),In=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:_P,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(Ce(vP),H(0),lt(1,"div",0),H(2,1),Rt(),H(3,2))},encapsulation:2,changeDetection:0})}return t})();var Mn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-card-avatar",""],["","matCardAvatar",""]],hostAttrs:[1,"mat-mdc-card-avatar"]})}return t})();var pt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[_e]})}return t})();function jD(t){return Error(`Unable to find icon with the name "${t}"`)}function yP(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function VD(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function BD(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var yr=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},UD=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new yr(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let a=this._sanitizer.sanitize(Te.HTML,r);if(!a)throw BD(r);let s=Jo(a);return this._addSvgIconConfig(e,i,new yr("",s,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new yr(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(Te.HTML,i);if(!o)throw BD(i);let a=Jo(o);return this._addSvgIconSetConfig(e,new yr("",a,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(Te.RESOURCE_URL,e);if(!i)throw VD(e);let r=this._cachedIconsByUrl.get(i);return r?Z(yf(r)):this._loadSvgIconFromConfig(new yr(e,null)).pipe(ot(o=>this._cachedIconsByUrl.set(i,o)),V(o=>yf(o)))}getNamedSvgIcon(e,i=""){let r=HD(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let a=this._iconSetConfigs.get(i);return a?this._getSvgFromIconSetConfigs(e,a):Hs(jD(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?Z(yf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(V(i=>yf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return Z(r);let o=i.filter(a=>!a.svgText).map(a=>this._loadSvgIconSetFromConfig(a).pipe(kr(s=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(Te.RESOURCE_URL,a.url)} failed: ${s.message}`;return this._errorHandler.handleError(new Error(c)),Z(null)})));return wo(o).pipe(V(()=>{let a=this._extractIconWithNameFromAnySet(e,i);if(!a)throw jD(e);return a}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let a=this._svgElementFromConfig(o),s=this._extractSvgIconFromSet(a,e,o.options);if(s)return s}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(ot(i=>e.svgText=i),V(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?Z(null):this._fetchIcon(e).pipe(ot(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let a=o.cloneNode(!0);if(a.removeAttribute("id"),a.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(a,r);if(a.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(a),r);let s=this._svgElementFromString(Jo("<svg></svg>"));return s.appendChild(a),this._setSvgAttributes(s,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(Jo("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:a,value:s}=r[o];a!=="id"&&i.setAttribute(a,s)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw yP();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let a=this._sanitizer.sanitize(Te.RESOURCE_URL,i);if(!a)throw VD(i);let s=this._inProgressUrlFetches.get(a);if(s)return s;let l=this._httpClient.get(a,{responseType:"text",withCredentials:o}).pipe(V(c=>Jo(c)),Do(()=>this._inProgressUrlFetches.delete(a)),$s());return this._inProgressUrlFetches.set(a,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(HD(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return CP(o)?new yr(o.url,null,o.options):new yr(o,null)}}static \u0275fac=function(i){return new(i||t)(Q(Je,8),Q(ql),Q(G,8),Q(on))};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function yf(t){return t.cloneNode(!0)}function HD(t,n){return t+":"+n}function CP(t){return!!(t.url&&t.options)}var xP=["*"],wP=new y("MAT_ICON_DEFAULT_OPTIONS"),EP=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(G),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),zD=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],DP=zD.map(t=>`[${t}]`).join(", "),IP=/^url\(['"]?#(.*?)['"]?\)$/,ht=(()=>{class t{_elementRef=d(N);_iconRegistry=d(UD);_location=d(EP);_errorHandler=d(on);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=fe.EMPTY;constructor(){let e=d(new Ni("aria-hidden"),{optional:!0}),i=d(wP,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(a=>{o.setAttribute(a.name,`url('${e}#${a.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(DP),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)zD.forEach(a=>{let s=i[o],l=s.getAttribute(a),c=l?l.match(IP):null;if(c){let f=r.get(s);f||(f=[],r.set(s,f)),f.push({name:a,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Oe(1)).subscribe(o=>this._setSvgElement(o),o=>{let a=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(a))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(ce("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),ln(r.color?"mat-"+r.color:""),R("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",J],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:xP,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),H(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
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
`],encapsulation:2,changeDetection:0})}return t})(),gt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[_e]})}return t})();var Lc=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new E;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var jc=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var $D=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[_e]})}return t})();var qD=["*"],GD=`.mdc-list {
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
`,MP=["unscopedContent"],SP=["text"],TP=[[["","matListItemAvatar",""],["","matListItemIcon",""]],[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["","matListItemMeta",""]],[["mat-divider"]]],AP=["[matListItemAvatar],[matListItemIcon]","[matListItemTitle]","[matListItemLine]","*","[matListItemMeta]","mat-divider"];var kP=new y("ListOption"),Pn=(()=>{class t{_elementRef=d(N);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return t})(),Bc=(()=>{class t{_elementRef=d(N);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return t})(),wi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matListItemMeta",""]],hostAttrs:[1,"mat-mdc-list-item-meta","mdc-list-item__end"]})}return t})(),WD=(()=>{class t{_listOption=d(kP,{optional:!0});constructor(){}_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,hostVars:4,hostBindings:function(i,r){i&2&&R("mdc-list-item__start",r._isAlignedAtStart())("mdc-list-item__end",!r._isAlignedAtStart())}})}return t})(),OP=(()=>{class t extends WD{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[be]})}return t})(),Ln=(()=>{class t extends WD{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[be]})}return t})(),RP=new y("MAT_LIST_CONFIG"),Vc=(()=>{class t{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=et(e)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(et(e))}_disabled=ne(!1);_defaultOptions=d(RP,{optional:!0});static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,hostVars:1,hostBindings:function(i,r){i&2&&ce("aria-disabled",r.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return t})(),NP=(()=>{class t{_elementRef=d(N);_ngZone=d(F);_listBase=d(Vc,{optional:!0});_platform=d(ve);_hostElement;_isButtonElement;_noopAnimations=He();_avatars;_icons;set lines(e){this._explicitLines=Qt(e,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(e){this._disableRipple=et(e)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(e){this._disabled.set(et(e))}_disabled=ne(!1);_subscriptions=new fe;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){d(mt).load(io);let e=d(Sc,{optional:!0});this.rippleConfig=e||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new ia(this,this._ngZone,this._hostElement,this._platform,d(W)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(_t(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(e){if(!this._lines||!this._titles||!this._unscopedContent)return;e&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),r=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let o=this._titles.length===0&&i===1;r.classList.toggle("mdc-list-item__primary-text",o),r.classList.toggle("mdc-list-item__secondary-text",!o)}else r.classList.remove("mdc-list-item__primary-text"),r.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let e=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(e+=1),e}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(e=>e.nodeType!==e.COMMENT_NODE).some(e=>!!(e.textContent&&e.textContent.trim()))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,contentQueries:function(i,r,o){if(i&1&&it(o,OP,4)(o,Ln,4),i&2){let a;$(a=q())&&(r._avatars=a),$(a=q())&&(r._icons=a)}},hostVars:4,hostBindings:function(i,r){i&2&&(ce("aria-disabled",r.disabled)("disabled",r._isButtonElement&&r.disabled||null),R("mdc-list-item--disabled",r.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return t})();var Vi=(()=>{class t extends Vc{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=A({type:t,selectors:[["mat-list"]],hostAttrs:[1,"mat-mdc-list","mat-mdc-list-base","mdc-list"],exportAs:["matList"],features:[xe([{provide:Vc,useExisting:t}]),be],ngContentSelectors:qD,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),H(0))},styles:[GD],encapsulation:2,changeDetection:0})}return t})(),Qn=(()=>{class t extends NP{_lines;_titles;_meta;_unscopedContent;_itemText;get activated(){return this._activated}set activated(e){this._activated=et(e)}_activated=!1;_getAriaCurrent(){return this._hostElement.nodeName==="A"&&this._activated?"page":null}_hasBothLeadingAndTrailing(){return this._meta.length!==0&&(this._avatars.length!==0||this._icons.length!==0)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=A({type:t,selectors:[["mat-list-item"],["a","mat-list-item",""],["button","mat-list-item",""]],contentQueries:function(i,r,o){if(i&1&&it(o,Bc,5)(o,Pn,5)(o,wi,5),i&2){let a;$(a=q())&&(r._lines=a),$(a=q())&&(r._titles=a),$(a=q())&&(r._meta=a)}},viewQuery:function(i,r){if(i&1&&Ge(MP,5)(SP,5),i&2){let o;$(o=q())&&(r._unscopedContent=o.first),$(o=q())&&(r._itemText=o.first)}},hostAttrs:[1,"mat-mdc-list-item","mdc-list-item"],hostVars:13,hostBindings:function(i,r){i&2&&(ce("aria-current",r._getAriaCurrent()),R("mdc-list-item--activated",r.activated)("mdc-list-item--with-leading-avatar",r._avatars.length!==0)("mdc-list-item--with-leading-icon",r._icons.length!==0)("mdc-list-item--with-trailing-meta",r._meta.length!==0)("mat-mdc-list-item-both-leading-and-trailing",r._hasBothLeadingAndTrailing())("_mat-animation-noopable",r._noopAnimations))},inputs:{activated:"activated"},exportAs:["matListItem"],features:[be],ngContentSelectors:AP,decls:10,vars:0,consts:[["unscopedContent",""],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mat-focus-indicator"]],template:function(i,r){i&1&&(Ce(TP),H(0),m(1,"span",1),H(2,1),H(3,2),m(4,"span",2,0),I("cdkObserveContent",function(){return r._updateItemLines(!0)}),H(6,3),u()(),H(7,4),H(8,5),re(9,"div",3))},dependencies:[SE],encapsulation:2,changeDetection:0})}return t})();var YD=(()=>{class t extends Vc{_isNonInteractive=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=A({type:t,selectors:[["mat-nav-list"]],hostAttrs:["role","navigation",1,"mat-mdc-nav-list","mat-mdc-list-base","mdc-list"],exportAs:["matNavList"],features:[xe([{provide:Vc,useExisting:t}]),be],ngContentSelectors:qD,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),H(0))},styles:[GD],encapsulation:2,changeDetection:0})}return t})();var Vt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[fs,Is,gf,_e,$D]})}return t})();var FP=["trigger"],PP=["panel"],LP=[[["mat-select-trigger"]],"*"],jP=["mat-select-trigger","*"];function VP(t,n){if(t&1&&(m(0,"span",4),p(1),u()),t&2){let e=T();h(),z(e.placeholder)}}function BP(t,n){t&1&&H(0)}function HP(t,n){if(t&1&&(m(0,"span",11),p(1),u()),t&2){let e=T(2);h(),z(e.triggerValue)}}function UP(t,n){if(t&1&&(m(0,"span",5),Fe(1,BP,1,0)(2,HP,2,1,"span",11),u()),t&2){let e=T();h(),Pe(e.customTrigger?1:2)}}function zP(t,n){if(t&1){let e=qe();m(0,"div",12,1),I("keydown",function(r){we(e);let o=T();return Ee(o._handleKeydown(r))}),H(2,1),u()}if(t&2){let e=T();ln(e.panelClass),R("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),ce("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var $P=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>pr(t)}}),qP=new y("MAT_SELECT_CONFIG"),GP=new y("MatSelectTrigger"),A_=class{source;value;constructor(n,e){this.source=n,this.value=e}},ao=(()=>{class t{_viewportRuler=d(Wn);_changeDetectorRef=d(ge);_elementRef=d(N);_dir=d(Et,{optional:!0});_idGenerator=d(Be);_renderer=d(Se);_parentFormField=d(sa,{optional:!0});ngControl=d(_i,{self:!0,optional:!0});_liveAnnouncer=d(u_);_defaultOptions=d(qP,{optional:!0});_animationsDisabled=He();_popoverLocation;_initialized=new E;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=nf(e,this.options,this.optionGroups),a=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=rf(a.offsetTop,a.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new A_(this,e)}_scrollStrategyFactory=d($P);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new E;_errorStateTracker;stateChanges=new E;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=ne(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(le.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=Tn(()=>{let e=this.options;return e?e.changes.pipe(Me(e),Ze(()=>_t(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(Ze(()=>this.optionSelectionChanges))});openedChange=new P;_openedStream=this.openedChange.pipe(ae(e=>e),V(()=>{}));_closedStream=this.openedChange.pipe(ae(e=>!e),V(()=>{}));selectionChange=new P;valueChange=new P;constructor(){let e=d(_f),i=d(gc,{optional:!0}),r=d(xt,{optional:!0}),o=d(new Ni("tabindex"),{optional:!0}),a=d(Nc,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Ss(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=a?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Lc(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(ye(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(ye(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(Me(null),ye(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Oe(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&no(this._trackedModal,"aria-owns",i),vs(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;no(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,a=this._keyManager;if(!a.isTyping()&&o&&!We(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let s=this.selected;a.onKeydown(e);let l=this.selected;l&&s!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,a=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!a&&(r===13||r===32)&&i.activeItem&&!We(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!a&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let s=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(s?l.select():l.deselect())})}else{let s=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==s&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!We(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof ws?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new ta(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=_t(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(ye(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),_t(...this.options.map(i=>i._stateChanges)).pipe(ye(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=wt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&it(o,GP,5)(o,wn,5)(o,kc,5),i&2){let a;$(a=q())&&(r.customTrigger=a.first),$(a=q())&&(r.options=a),$(a=q())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&Ge(FP,5)(PP,5)(ff,5),i&2){let o;$(o=q())&&(r.trigger=o.first),$(o=q())&&(r.panel=o.first),$(o=q())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&I("keydown",function(a){return r._handleKeydown(a)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(ce("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),R("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",J],disableRipple:[2,"disableRipple","disableRipple",J],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:pi(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",J],placeholder:"placeholder",required:[2,"required","required",J],multiple:[2,"multiple","multiple",J],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",J],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",pi],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",J]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[xe([{provide:Fc,useExisting:t},{provide:Ac,useExisting:t}]),je],ngContentSelectors:jP,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Ce(LP),m(0,"div",2,0),I("click",function(){return r.open()}),m(3,"div",3),Fe(4,VP,2,1,"span",4)(5,UP,3,1,"span",5),u(),m(6,"div",6)(7,"div",7),Ai(),m(8,"svg",8),re(9,"path",9),u()()()(),M(10,zP,3,16,"ng-template",10),I("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(a){return r._handleOverlayKeydown(a)})),i&2){let o=yt(1);h(3),ce("id",r._valueId),h(),Pe(r.empty?4:5),h(6),_("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[ws,ff],styles:[`@keyframes _mat-select-enter {
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
`],encapsulation:2,changeDetection:0})}return t})();var Bt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[xi,Ms,_e,Gn,Xe,Ms]})}return t})();var wf=["*"],WP=["content"],YP=[[["mat-drawer"]],[["mat-drawer-content"]],"*"],ZP=["mat-drawer","mat-drawer-content","*"];function QP(t,n){if(t&1){let e=qe();m(0,"div",1),I("click",function(){we(e);let r=T();return Ee(r._onBackdropClicked())}),u()}if(t&2){let e=T();R("mat-drawer-shown",e._isShowingBackdrop())}}function KP(t,n){t&1&&(m(0,"mat-drawer-content"),H(1,2),u())}var XP=[[["mat-sidenav"]],[["mat-sidenav-content"]],"*"],JP=["mat-sidenav","mat-sidenav-content","*"];function eL(t,n){if(t&1){let e=qe();m(0,"div",1),I("click",function(){we(e);let r=T();return Ee(r._onBackdropClicked())}),u()}if(t&2){let e=T();R("mat-drawer-shown",e._isShowingBackdrop())}}function tL(t,n){t&1&&(m(0,"mat-sidenav-content"),H(1,2),u())}var nL=`.mat-drawer-container {
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
`;var iL=new y("MAT_DRAWER_DEFAULT_AUTOSIZE",{providedIn:"root",factory:()=>!1}),R_=new y("MAT_DRAWER_CONTAINER"),Cf=(()=>{class t extends ur{_platform=d(ve);_changeDetectorRef=d(ge);_container=d(O_);constructor(){let e=d(N),i=d(dr),r=d(F);super(e,i,r)}ngAfterContentInit(){this._container._contentMarginChanges.subscribe(()=>{this._changeDetectorRef.markForCheck()})}_shouldBeHidden(){if(this._platform.isBrowser)return!1;let{start:e,end:i}=this._container;return e!=null&&e.mode!=="over"&&e.opened||i!=null&&i.mode!=="over"&&i.opened}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-drawer-content"]],hostAttrs:[1,"mat-drawer-content"],hostVars:6,hostBindings:function(i,r){i&2&&(On("margin-left",r._container._contentMargins.left,"px")("margin-right",r._container._contentMargins.right,"px"),R("mat-drawer-content-hidden",r._shouldBeHidden()))},features:[xe([{provide:ur,useExisting:t}]),be],ngContentSelectors:wf,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),H(0))},encapsulation:2,changeDetection:0})}return t})(),k_=(()=>{class t{_elementRef=d(N);_focusTrapFactory=d(wc);_focusMonitor=d(bi);_platform=d(ve);_ngZone=d(F);_renderer=d(Se);_interactivityChecker=d(ps);_doc=d(G);_container=d(R_,{optional:!0});_focusTrap=null;_elementFocusedBeforeDrawerWasOpened=null;_eventCleanups;_isAttached=!1;_anchor=null;get position(){return this._position}set position(e){e=e==="end"?"end":"start",e!==this._position&&(this._isAttached&&this._updatePositionInParent(e),this._position=e,this.onPositionChanged.emit())}_position="start";get mode(){return this._mode}set mode(e){this._mode=e,this._updateFocusTrapState(),this._modeChanged.next()}_mode="over";get disableClose(){return this._disableClose}set disableClose(e){this._disableClose=et(e)}_disableClose=!1;get autoFocus(){let e=this._autoFocus;return e??(this.mode==="side"?"dialog":"first-tabbable")}set autoFocus(e){(e==="true"||e==="false"||e==null)&&(e=et(e)),this._autoFocus=e}_autoFocus;get opened(){return this._opened()}set opened(e){this.toggle(et(e))}_opened=ne(!1);_openedVia=null;_animationStarted=new E;_animationEnd=new E;openedChange=new P(!0);_openedStream=this.openedChange.pipe(ae(e=>e),V(()=>{}));openedStart=this._animationStarted.pipe(ae(()=>this.opened),ga(void 0));_closedStream=this.openedChange.pipe(ae(e=>!e),V(()=>{}));closedStart=this._animationStarted.pipe(ae(()=>!this.opened),ga(void 0));_destroyed=new E;onPositionChanged=new P;_content;_modeChanged=new E;_injector=d(W);_changeDetectorRef=d(ge);constructor(){this.openedChange.pipe(ye(this._destroyed)).subscribe(e=>{e?(this._elementFocusedBeforeDrawerWasOpened=this._doc.activeElement,this._takeFocus()):this._isFocusWithinDrawer()&&this._restoreFocus(this._openedVia||"program")}),this._eventCleanups=this._ngZone.runOutsideAngular(()=>{let e=this._renderer,i=this._elementRef.nativeElement;return[e.listen(i,"keydown",r=>{r.keyCode===27&&!this.disableClose&&!We(r)&&this._ngZone.run(()=>{this.close(),r.stopPropagation(),r.preventDefault()})}),e.listen(i,"transitionend",this._handleTransitionEvent),e.listen(i,"transitioncancel",this._handleTransitionEvent)]}),this._animationEnd.subscribe(()=>{this.openedChange.emit(this.opened)})}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_takeFocus(){if(!this._focusTrap)return;let e=this._elementRef.nativeElement;switch(this.autoFocus){case!1:case"dialog":return;case!0:case"first-tabbable":st(()=>{!this._focusTrap.focusInitialElement()&&typeof e.focus=="function"&&e.focus()},{injector:this._injector});break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]');break;default:this._focusByCssSelector(this.autoFocus);break}}_restoreFocus(e){this.autoFocus!=="dialog"&&(this._elementFocusedBeforeDrawerWasOpened?this._focusMonitor.focusVia(this._elementFocusedBeforeDrawerWasOpened,e):this._elementRef.nativeElement.blur(),this._elementFocusedBeforeDrawerWasOpened=null)}_isFocusWithinDrawer(){let e=this._doc.activeElement;return!!e&&this._elementRef.nativeElement.contains(e)}ngAfterViewInit(){this._isAttached=!0,this._position==="end"&&this._updatePositionInParent("end"),this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._updateFocusTrapState())}ngOnDestroy(){this._eventCleanups.forEach(e=>e()),this._focusTrap?.destroy(),this._anchor?.remove(),this._anchor=null,this._animationStarted.complete(),this._animationEnd.complete(),this._modeChanged.complete(),this._destroyed.next(),this._destroyed.complete()}open(e){return this.toggle(!0,e)}close(){return this.toggle(!1)}_closeViaBackdropClick(){return this._setOpen(!1,!0,"mouse")}toggle(e=!this.opened,i){e&&i&&(this._openedVia=i);let r=this._setOpen(e,!e&&this._isFocusWithinDrawer(),this._openedVia||"program");return e||(this._openedVia=null),r}_setOpen(e,i,r){return e===this.opened?Promise.resolve(e?"open":"close"):(this._opened.set(e),this._container?._transitionsEnabled?(this._setIsAnimating(!0),setTimeout(()=>this._animationStarted.next())):setTimeout(()=>{this._animationStarted.next(),this._animationEnd.next()}),this._elementRef.nativeElement.classList.toggle("mat-drawer-opened",e),!e&&i&&this._restoreFocus(r),this._changeDetectorRef.markForCheck(),this._updateFocusTrapState(),new Promise(o=>{this.openedChange.pipe(Oe(1)).subscribe(a=>o(a?"open":"close"))}))}_setIsAnimating(e){this._elementRef.nativeElement.classList.toggle("mat-drawer-animating",e)}_getWidth(){return this._elementRef.nativeElement.offsetWidth||0}_updateFocusTrapState(){this._focusTrap&&(this._focusTrap.enabled=this.opened&&!!this._container?._isShowingBackdrop())}_updatePositionInParent(e){if(!this._platform.isBrowser)return;let i=this._elementRef.nativeElement,r=i.parentNode;e==="end"?(this._anchor||(this._anchor=this._doc.createComment("mat-drawer-anchor"),r.insertBefore(this._anchor,i)),r.appendChild(i)):this._anchor&&this._anchor.parentNode.insertBefore(i,this._anchor)}_handleTransitionEvent=e=>{let i=this._elementRef.nativeElement;e.target===i&&this._ngZone.run(()=>{e.type==="transitionend"&&this._setIsAnimating(!1),this._animationEnd.next(e)})};static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-drawer"]],viewQuery:function(i,r){if(i&1&&Ge(WP,5),i&2){let o;$(o=q())&&(r._content=o.first)}},hostAttrs:[1,"mat-drawer"],hostVars:12,hostBindings:function(i,r){i&2&&(ce("align",null)("tabIndex",r.mode!=="side"?"-1":null),On("visibility",!r._container&&!r.opened?"hidden":null),R("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side"))},inputs:{position:"position",mode:"mode",disableClose:"disableClose",autoFocus:"autoFocus",opened:"opened"},outputs:{openedChange:"openedChange",_openedStream:"opened",openedStart:"openedStart",_closedStream:"closed",closedStart:"closedStart",onPositionChanged:"positionChanged"},exportAs:["matDrawer"],ngContentSelectors:wf,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(Ce(),m(0,"div",1,0),H(2),u())},dependencies:[ur],encapsulation:2,changeDetection:0})}return t})(),O_=(()=>{class t{_dir=d(Et,{optional:!0});_element=d(N);_ngZone=d(F);_changeDetectorRef=d(ge);_animationDisabled=He();_transitionsEnabled=!1;_allDrawers;_drawers=new li;_content;_userContent;get start(){return this._start}get end(){return this._end}get autosize(){return this._autosize}set autosize(e){this._autosize=et(e)}_autosize=d(iL);get hasBackdrop(){return this._drawerHasBackdrop(this._start)||this._drawerHasBackdrop(this._end)}set hasBackdrop(e){this._backdropOverride=e==null?null:et(e)}_backdropOverride=null;backdropClick=new P;_start=null;_end=null;_left=null;_right=null;_destroyed=new E;_doCheckSubject=new E;_contentMargins={left:null,right:null};_contentMarginChanges=new E;get scrollable(){return this._userContent||this._content}_injector=d(W);constructor(){let e=d(ve),i=d(Wn);this._dir?.change.pipe(ye(this._destroyed)).subscribe(()=>{this._validateDrawers(),this.updateContentMargins()}),i.change().pipe(ye(this._destroyed)).subscribe(()=>this.updateContentMargins()),!this._animationDisabled&&e.isBrowser&&this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._element.nativeElement.classList.add("mat-drawer-transition"),this._transitionsEnabled=!0},200)})}ngAfterContentInit(){this._allDrawers.changes.pipe(Me(this._allDrawers),ye(this._destroyed)).subscribe(e=>{this._drawers.reset(e.filter(i=>!i._container||i._container===this)),this._drawers.notifyOnChanges()}),this._drawers.changes.pipe(Me(null)).subscribe(()=>{this._validateDrawers(),this._drawers.forEach(e=>{this._watchDrawerToggle(e),this._watchDrawerPosition(e),this._watchDrawerMode(e)}),(!this._drawers.length||this._isDrawerOpen(this._start)||this._isDrawerOpen(this._end))&&this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),this._ngZone.runOutsideAngular(()=>{this._doCheckSubject.pipe(Xn(10),ye(this._destroyed)).subscribe(()=>this.updateContentMargins())})}ngOnDestroy(){this._contentMarginChanges.complete(),this._doCheckSubject.complete(),this._drawers.destroy(),this._destroyed.next(),this._destroyed.complete()}open(){this._drawers.forEach(e=>e.open())}close(){this._drawers.forEach(e=>e.close())}updateContentMargins(){let e=0,i=0;if(this._left&&this._left.opened){if(this._left.mode=="side")e+=this._left._getWidth();else if(this._left.mode=="push"){let r=this._left._getWidth();e+=r,i-=r}}if(this._right&&this._right.opened){if(this._right.mode=="side")i+=this._right._getWidth();else if(this._right.mode=="push"){let r=this._right._getWidth();i+=r,e-=r}}e=e||null,i=i||null,(e!==this._contentMargins.left||i!==this._contentMargins.right)&&(this._contentMargins={left:e,right:i},this._ngZone.run(()=>this._contentMarginChanges.next(this._contentMargins)))}ngDoCheck(){this._autosize&&this._isPushed()&&this._ngZone.runOutsideAngular(()=>this._doCheckSubject.next())}_watchDrawerToggle(e){e._animationStarted.pipe(ye(this._drawers.changes)).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),e.mode!=="side"&&e.openedChange.pipe(ye(this._drawers.changes)).subscribe(()=>this._setContainerClass(e.opened))}_watchDrawerPosition(e){e.onPositionChanged.pipe(ye(this._drawers.changes)).subscribe(()=>{st({read:()=>this._validateDrawers()},{injector:this._injector})})}_watchDrawerMode(e){e._modeChanged.pipe(ye(_t(this._drawers.changes,this._destroyed))).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()})}_setContainerClass(e){let i=this._element.nativeElement.classList,r="mat-drawer-container-has-open";e?i.add(r):i.remove(r)}_validateDrawers(){this._start=this._end=null,this._drawers.forEach(e=>{e.position=="end"?(this._end!=null,this._end=e):(this._start!=null,this._start=e)}),this._right=this._left=null,this._dir&&this._dir.value==="rtl"?(this._left=this._end,this._right=this._start):(this._left=this._start,this._right=this._end)}_isPushed(){return this._isDrawerOpen(this._start)&&this._start.mode!="over"||this._isDrawerOpen(this._end)&&this._end.mode!="over"}_onBackdropClicked(){this.backdropClick.emit(),this._closeModalDrawersViaBackdrop()}_closeModalDrawersViaBackdrop(){[this._start,this._end].filter(e=>e&&!e.disableClose&&this._drawerHasBackdrop(e)).forEach(e=>e._closeViaBackdropClick())}_isShowingBackdrop(){return this._isDrawerOpen(this._start)&&this._drawerHasBackdrop(this._start)||this._isDrawerOpen(this._end)&&this._drawerHasBackdrop(this._end)}_isDrawerOpen(e){return e!=null&&e.opened}_drawerHasBackdrop(e){return this._backdropOverride==null?!!e&&e.mode!=="side":this._backdropOverride}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-drawer-container"]],contentQueries:function(i,r,o){if(i&1&&it(o,Cf,5)(o,k_,5),i&2){let a;$(a=q())&&(r._content=a.first),$(a=q())&&(r._allDrawers=a)}},viewQuery:function(i,r){if(i&1&&Ge(Cf,5),i&2){let o;$(o=q())&&(r._userContent=o.first)}},hostAttrs:[1,"mat-drawer-container"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-drawer-container-explicit-backdrop",r._backdropOverride)},inputs:{autosize:"autosize",hasBackdrop:"hasBackdrop"},outputs:{backdropClick:"backdropClick"},exportAs:["matDrawerContainer"],features:[xe([{provide:R_,useExisting:t}])],ngContentSelectors:ZP,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(Ce(YP),Fe(0,QP,1,2,"div",0),H(1),H(2,1),Fe(3,KP,2,0,"mat-drawer-content")),i&2&&(Pe(r.hasBackdrop?0:-1),h(3),Pe(r._content?-1:3))},dependencies:[Cf],styles:[`.mat-drawer-container {
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
`],encapsulation:2,changeDetection:0})}return t})(),xf=(()=>{class t extends Cf{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=A({type:t,selectors:[["mat-sidenav-content"]],hostAttrs:[1,"mat-drawer-content","mat-sidenav-content"],features:[xe([{provide:ur,useExisting:t}]),be],ngContentSelectors:wf,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),H(0))},encapsulation:2,changeDetection:0})}return t})(),N_=(()=>{class t extends k_{get fixedInViewport(){return this._fixedInViewport}set fixedInViewport(e){this._fixedInViewport=et(e)}_fixedInViewport=!1;get fixedTopGap(){return this._fixedTopGap}set fixedTopGap(e){this._fixedTopGap=Qt(e)}_fixedTopGap=0;get fixedBottomGap(){return this._fixedBottomGap}set fixedBottomGap(e){this._fixedBottomGap=Qt(e)}_fixedBottomGap=0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=A({type:t,selectors:[["mat-sidenav"]],hostAttrs:[1,"mat-drawer","mat-sidenav"],hostVars:16,hostBindings:function(i,r){i&2&&(ce("tabIndex",r.mode!=="side"?"-1":null)("align",null),On("top",r.fixedInViewport?r.fixedTopGap:null,"px")("bottom",r.fixedInViewport?r.fixedBottomGap:null,"px"),R("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side")("mat-sidenav-fixed",r.fixedInViewport))},inputs:{fixedInViewport:"fixedInViewport",fixedTopGap:"fixedTopGap",fixedBottomGap:"fixedBottomGap"},exportAs:["matSidenav"],features:[xe([{provide:k_,useExisting:t}]),be],ngContentSelectors:wf,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(Ce(),m(0,"div",1,0),H(2),u())},dependencies:[ur],encapsulation:2,changeDetection:0})}return t})(),ZD=(()=>{class t extends O_{_allDrawers=void 0;_content=void 0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=A({type:t,selectors:[["mat-sidenav-container"]],contentQueries:function(i,r,o){if(i&1&&it(o,xf,5)(o,N_,5),i&2){let a;$(a=q())&&(r._content=a.first),$(a=q())&&(r._allDrawers=a)}},hostAttrs:[1,"mat-drawer-container","mat-sidenav-container"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-drawer-container-explicit-backdrop",r._backdropOverride)},exportAs:["matSidenavContainer"],features:[xe([{provide:R_,useExisting:t},{provide:O_,useExisting:t}]),be],ngContentSelectors:JP,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(Ce(XP),Fe(0,eL,1,2,"div",0),H(1),H(2,1),Fe(3,tL,2,0,"mat-sidenav-content")),i&2&&(Pe(r.hasBackdrop?0:-1),h(3),Pe(r._content?-1:3))},dependencies:[xf],styles:[nL],encapsulation:2,changeDetection:0})}return t})(),Ht=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[Gn,_e,Gn]})}return t})();var rL=["*",[["mat-toolbar-row"]]],oL=["*","mat-toolbar-row"],aL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),QD=(()=>{class t{_elementRef=d(N);_platform=d(ve);_document=d(G);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&it(o,aL,5),i&2){let a;$(a=q())&&(r._toolbarRows=a)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(ln(r.color?"mat-"+r.color:""),R("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:oL,decls:2,vars:0,template:function(i,r){i&1&&(Ce(rL),H(0),H(1,1))},styles:[`.mat-toolbar {
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
`],encapsulation:2,changeDetection:0})}return t})();var Ut=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[_e]})}return t})();function sL(t,n){t&1&&(m(0,"mat-error"),p(1," El n\xFAmero es obligatorio "),u())}function lL(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;_("value",e),h(),K(" ",e.nombreCompleto||e.nombre," ")}}function cL(t,n){t&1&&(m(0,"mat-error"),p(1," El jugador es obligatorio "),u())}function dL(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;_("value",e),h(),K(" ",e.nombre," ")}}function uL(t,n){t&1&&(m(0,"mat-error"),p(1," El equipo es obligatorio "),u())}function mL(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;_("value",e),h(),K(" ",e.nombre," ")}}function fL(t,n){t&1&&(m(0,"mat-error"),p(1," El tipo de cromo es obligatorio "),u())}function pL(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;_("value",e.id),h(),K(" ",e.nombre," ")}}function hL(t,n){t&1&&(m(0,"mat-error"),p(1," La edici\xF3n es obligatoria "),u())}function gL(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;_("value",e.id),h(),ct(" ",e.nombre," (",e.temporada,") ")}}function vL(t,n){t&1&&(m(0,"mat-error"),p(1," El \xE1lbum es obligatorio "),u())}var Ef=class t{constructor(n,e,i){this.fb=n;this.http=e;this.cdr=i}fb;http;cdr;cromoForm;ediciones=[];albumes=[];equipos=[];jugadores=[];tiposCromo=[];filteredEquipos$;filteredJugadores$;filteredTiposCromo$;cargando=!1;mensaje="";error="";apiUrl="/api";ngOnInit(){this.crearFormulario(),this.cargarDatos()}crearFormulario(){this.cromoForm=this.fb.group({numero:["",le.required],edicionId:[null,le.required],albumId:[null,le.required],equipo:[null,le.required],jugador:[{value:null,disabled:!0},le.required],tipoCromo:[null,le.required]})}cargarDatos(){this.cargarEdiciones(),this.cargarAlbumes(),this.cargarEquipos(),this.cargarTiposCromo()}cargarEdiciones(){this.http.get(`${this.apiUrl}/Ediciones`).subscribe({next:n=>{this.ediciones=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar las ediciones."}})}cargarAlbumes(){this.http.get(`${this.apiUrl}/Albumes`).subscribe({next:n=>{this.albumes=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los \xE1lbumes."}})}cargarEquipos(){this.http.get(`${this.apiUrl}/Equipos`).subscribe({next:n=>{this.equipos=n,this.filteredEquipos$=this.cromoForm.get("equipo").valueChanges.pipe(Me(""),V(e=>this.filtrarEquipos(e))),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los equipos."}})}filtrarEquipos(n){let e=(typeof n=="string"?n:"").toLowerCase();return this.equipos.filter(i=>i.nombre.toLowerCase().includes(e))}onEquipoInput(n){this.cromoForm.get("equipo")?.setValue(n)}onEquipoSelected(n){this.cromoForm.get("equipo")?.setValue(n),this.onEquipoChange()}displayEquipo(n){return n?.nombre??""}onEquipoChange(){let e=this.cromoForm.get("equipo")?.value?.id;if(this.jugadores=[],this.cromoForm.patchValue({jugador:null}),!e){this.cromoForm.get("jugador")?.disable();return}this.http.get(`${this.apiUrl}/Equipos/${e}/Jugadores`).subscribe({next:i=>{this.jugadores=i,this.filteredJugadores$=this.cromoForm.get("jugador").valueChanges.pipe(Me(""),V(r=>this.filtrarJugadores(r))),this.cromoForm.get("jugador")?.enable(),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los jugadores."}})}filtrarJugadores(n){let e=(typeof n=="string"?n:"").toLowerCase();return this.jugadores.filter(i=>(i.nombreCompleto||i.nombre).toLowerCase().includes(e))}onJugadorInput(n){this.cromoForm.get("jugador")?.setValue(n)}onJugadorSelected(n){this.cromoForm.get("jugador")?.setValue(n)}displayJugador(n){return n?typeof n=="string"?n:n.nombreCompleto||n.nombre:""}cargarTiposCromo(){this.http.get(`${this.apiUrl}/TiposCromo`).subscribe({next:n=>{this.tiposCromo=n,this.filteredTiposCromo$=this.cromoForm.get("tipoCromo").valueChanges.pipe(Me(""),V(e=>this.filtrarTiposCromo(e))),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los tipos de cromo."}})}filtrarTiposCromo(n){let e=(typeof n=="string"?n:n?.nombre??"").toLowerCase();return this.tiposCromo.filter(i=>i.nombre.toLowerCase().includes(e))}onTipoCromoInput(n){this.cromoForm.get("tipoCromo")?.setValue(n)}onTipoCromoSelected(n){this.cromoForm.get("tipoCromo")?.setValue(n)}displayTipoCromo(n){return n?.nombre??""}guardar(){if(this.cromoForm.invalid){this.cromoForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n={numero:this.cromoForm.get("numero")?.value,edicionId:this.cromoForm.get("edicionId")?.value,albumId:this.cromoForm.get("albumId")?.value,equipoId:this.cromoForm.get("equipo")?.value?.id,jugadorId:this.cromoForm.get("jugador")?.value?.id,tipoCromoId:this.cromoForm.get("tipoCromo")?.value?.id};this.http.post(`${this.apiUrl}/Cromo`,n).subscribe({next:()=>{this.cargando=!1,this.mensaje="Cromo a\xF1adido correctamente.",this.cromoForm.patchValue({jugador:null}),this.cromoForm.get("jugador")?.disable(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el cromo."}})}cancelar(){this.cromoForm.reset(),this.mensaje="",this.error="",this.cromoForm.get("jugador")?.disable(),this.cdr.detectChanges()}static \u0275fac=function(e){return new(e||t)(D(xn),D(Je),D(ge))};static \u0275cmp=A({type:t,selectors:[["app-cromos"]],decls:80,vars:28,consts:[["autoJugador","matAutocomplete"],["autoEquipo","matAutocomplete"],["autoTipoCromo","matAutocomplete"],[1,"page-container"],[1,"cromo-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],[1,"form-grid"],["appearance","outline"],["matInput","","type","text","formControlName","numero","placeholder","Ej. 108"],["matSuffix",""],[4,"ngIf"],["matInput","","type","text","formControlName","jugador","placeholder","Busca un jugador...",3,"input","matAutocomplete"],[3,"optionSelected","displayWith"],[3,"value",4,"ngFor","ngForOf"],["matInput","","type","text","formControlName","equipo","placeholder","Busca un equipo...",3,"matAutocomplete"],["matInput","","type","text","formControlName","tipoCromo","placeholder","Busca un tipo...",3,"input","matAutocomplete"],["formControlName","edicionId"],[3,"value"],["formControlName","albumId"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"]],template:function(e,i){if(e&1&&(m(0,"div",3)(1,"mat-card",4)(2,"mat-card-header")(3,"div",5)(4,"mat-icon"),p(5,"style"),u()(),m(6,"mat-card-title"),p(7,"A\xF1adir nuevo cromo"),u(),m(8,"mat-card-subtitle"),p(9," Completa los datos del cromo que quieres a\xF1adir "),u()(),m(10,"mat-card-content")(11,"form",6),I("ngSubmit",function(){return i.guardar()}),m(12,"div",7)(13,"mat-form-field",8)(14,"mat-label"),p(15,"N\xFAmero del cromo"),u(),re(16,"input",9),m(17,"mat-icon",10),p(18,"numbers"),u(),M(19,sL,2,0,"mat-error",11),u(),m(20,"mat-form-field",8)(21,"mat-label"),p(22,"Jugador"),u(),m(23,"input",12),I("input",function(o){return i.onJugadorInput(o.target.value)}),u(),m(24,"mat-autocomplete",13,0),I("optionSelected",function(o){return i.onJugadorSelected(o.option.value)}),M(26,lL,2,2,"mat-option",14),rr(27,"async"),u(),m(28,"mat-icon",10),p(29,"person"),u(),M(30,cL,2,0,"mat-error",11),u(),m(31,"mat-form-field",8)(32,"mat-label"),p(33,"Equipo"),u(),re(34,"input",15),m(35,"mat-autocomplete",13,1),I("optionSelected",function(o){return i.onEquipoSelected(o.option.value)}),M(37,dL,2,2,"mat-option",14),rr(38,"async"),u(),m(39,"mat-icon",10),p(40,"groups"),u(),M(41,uL,2,0,"mat-error",11),u(),m(42,"mat-form-field",8)(43,"mat-label"),p(44,"Tipo de cromo"),u(),m(45,"input",16),I("input",function(o){return i.onTipoCromoInput(o.target.value)}),u(),m(46,"mat-autocomplete",13,2),I("optionSelected",function(o){return i.onTipoCromoSelected(o.option.value)}),M(48,mL,2,2,"mat-option",14),rr(49,"async"),u(),m(50,"mat-icon",10),p(51,"category"),u(),M(52,fL,2,0,"mat-error",11),u(),m(53,"mat-form-field",8)(54,"mat-label"),p(55,"Edici\xF3n"),u(),m(56,"mat-select",17)(57,"mat-option",18),p(58," Selecciona una edici\xF3n "),u(),M(59,pL,2,2,"mat-option",14),u(),m(60,"mat-icon",10),p(61,"collections_bookmark"),u(),M(62,hL,2,0,"mat-error",11),u(),m(63,"mat-form-field",8)(64,"mat-label"),p(65,"\xC1lbum"),u(),m(66,"mat-select",19)(67,"mat-option",18),p(68," Selecciona un \xE1lbum "),u(),M(69,gL,2,3,"mat-option",14),u(),m(70,"mat-icon",10),p(71,"album"),u(),M(72,vL,2,0,"mat-error",11),u()(),m(73,"mat-card-actions",20)(74,"button",21),I("click",function(){return i.cancelar()}),p(75," Cancelar "),u(),m(76,"button",22)(77,"mat-icon"),p(78,"save"),u(),p(79),u()()()()()()),e&2){let r,o,a,s,l,c,f=yt(25),g=yt(36),v=yt(47);h(11),_("formGroup",i.cromoForm),h(8),_("ngIf",((r=i.cromoForm.get("numero"))==null?null:r.touched)&&((r=i.cromoForm.get("numero"))==null?null:r.hasError("required"))),h(4),_("matAutocomplete",f),h(),_("displayWith",i.displayJugador),h(2),_("ngForOf",or(27,22,i.filteredJugadores$)),h(4),_("ngIf",((o=i.cromoForm.get("jugador"))==null?null:o.touched)&&((o=i.cromoForm.get("jugador"))==null?null:o.hasError("required"))),h(4),_("matAutocomplete",g),h(),_("displayWith",i.displayEquipo),h(2),_("ngForOf",or(38,24,i.filteredEquipos$)),h(4),_("ngIf",((a=i.cromoForm.get("equipo"))==null?null:a.touched)&&((a=i.cromoForm.get("equipo"))==null?null:a.hasError("required"))),h(4),_("matAutocomplete",v),h(),_("displayWith",i.displayTipoCromo),h(2),_("ngForOf",or(49,26,i.filteredTiposCromo$)),h(4),_("ngIf",((s=i.cromoForm.get("tipoCromo"))==null?null:s.touched)&&((s=i.cromoForm.get("tipoCromo"))==null?null:s.hasError("required"))),h(5),_("value",null),h(2),_("ngForOf",i.ediciones),h(3),_("ngIf",((l=i.cromoForm.get("edicionId"))==null?null:l.touched)&&((l=i.cromoForm.get("edicionId"))==null?null:l.hasError("required"))),h(5),_("value",null),h(2),_("ngForOf",i.albumes),h(3),_("ngIf",((c=i.cromoForm.get("albumId"))==null?null:c.touched)&&((c=i.cromoForm.get("albumId"))==null?null:c.hasError("required"))),h(4),_("disabled",i.cargando),h(3),K(" ",i.cargando?"Guardando...":"Guardar cromo"," ")}},dependencies:[ut,dt,Ct,Zt,Cn,Ft,Yt,yn,xt,un,Pt,vf,wn,Pc,Lt,Jt,Dt,ft,Kt,Xt,jt,en,pt,tn,Zn,Mn,nn,In,Dn,En,Xe,gt,ht,Vt,Bt,ao,Ht,Ut,jl],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa;display:flex;justify-content:center;align-items:flex-start}.cromo-card[_ngcontent-%COMP%]{width:100%;max-width:750px;border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:24px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}.form-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 20px}.full-width[_ngcontent-%COMP%]{grid-column:1/-1}mat-form-field[_ngcontent-%COMP%]{width:100%}mat-card-actions[_ngcontent-%COMP%]{padding:16px 24px 24px;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}.form-grid[_ngcontent-%COMP%]{grid-template-columns:1fr}.full-width[_ngcontent-%COMP%]{grid-column:auto}mat-card-title[_ngcontent-%COMP%]{font-size:21px!important}}"]})};var _L=()=>({exact:!0}),Df=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=A({type:t,selectors:[["app-layout"]],decls:55,vars:2,consts:[["sidenav",""],[1,"sidenav-container"],["mode","side","opened",""],[1,"logo"],["mat-list-item","","routerLink","/","routerLinkActive","active",3,"routerLinkActiveOptions"],["matListItemIcon",""],["matListItemTitle",""],[1,"nav-section"],["mat-list-item","","routerLink","/cromos","routerLinkActive","active"],["mat-list-item","","routerLink","/jugadores","routerLinkActive","active"],["mat-list-item","","routerLink","/equipos","routerLinkActive","active"],["mat-list-item","","routerLink","/tipos-cromo","routerLinkActive","active"],["mat-list-item","","routerLink","/albumes","routerLinkActive","active"],["mat-list-item","","routerLink","/editoriales","routerLinkActive","active"],["mat-icon-button","",3,"click"],[1,"toolbar-title"],[1,"content"]],template:function(e,i){if(e&1){let r=qe();m(0,"mat-sidenav-container",1)(1,"mat-sidenav",2,0)(3,"div",3)(4,"mat-icon"),p(5,"style"),u(),m(6,"span"),p(7,"CromosList"),u()(),m(8,"mat-nav-list")(9,"a",4)(10,"mat-icon",5),p(11,"dashboard"),u(),m(12,"span",6),p(13,"Panel"),u()(),m(14,"h3",7),p(15,"Cat\xE1logo"),u(),m(16,"a",8)(17,"mat-icon",5),p(18,"style"),u(),m(19,"span",6),p(20,"Cromos"),u()(),m(21,"a",9)(22,"mat-icon",5),p(23,"person"),u(),m(24,"span",6),p(25,"Jugadores"),u()(),m(26,"a",10)(27,"mat-icon",5),p(28,"groups"),u(),m(29,"span",6),p(30,"Equipos"),u()(),m(31,"a",11)(32,"mat-icon",5),p(33,"category"),u(),m(34,"span",6),p(35,"Tipos de cromo"),u()(),m(36,"a",12)(37,"mat-icon",5),p(38,"album"),u(),m(39,"span",6),p(40,"\xC1lbumes"),u()(),m(41,"a",13)(42,"mat-icon",5),p(43,"storefront"),u(),m(44,"span",6),p(45,"Editoriales"),u()()()(),m(46,"mat-sidenav-content")(47,"mat-toolbar")(48,"button",14),I("click",function(){we(r);let a=yt(2);return Ee(a.toggle())}),m(49,"mat-icon"),p(50,"menu"),u()(),m(51,"span",15),p(52," CromosList "),u()(),m(53,"main",16),re(54,"router-outlet"),u()()()}e&2&&(h(9),_("routerLinkActiveOptions",qg(1,_L)))},dependencies:[Ko,$n,Gv,jt,Fn,Pt,pt,Xe,gt,ht,Lt,Vt,YD,Qn,Ln,Pn,Bt,Ht,N_,ZD,xf,Ut,QD],styles:[".sidenav-container[_ngcontent-%COMP%]{height:100vh}mat-sidenav[_ngcontent-%COMP%]{width:250px;background:var(--app-surface);border-right:1px solid var(--app-border)}.logo[_ngcontent-%COMP%]{height:64px;display:flex;align-items:center;gap:12px;padding:0 20px;font-size:20px;font-weight:700;color:var(--app-primary-dark);letter-spacing:.2px}.logo[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:28px;width:28px;height:28px;color:var(--app-green)}mat-nav-list[_ngcontent-%COMP%]{padding-top:6px}.nav-section[_ngcontent-%COMP%]{margin:20px 22px 6px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--app-text-muted)}mat-nav-list[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{margin:3px 10px;border-radius:10px;height:46px}mat-nav-list[_ngcontent-%COMP%]   a.active[_ngcontent-%COMP%]{background:var(--app-primary-soft);color:var(--app-primary-dark)}mat-nav-list[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover{background:var(--app-green-soft)}mat-nav-list[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--app-text-muted)}mat-nav-list[_ngcontent-%COMP%]   a.active[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--app-primary)}mat-toolbar[_ngcontent-%COMP%]{background:var(--app-surface);color:var(--app-primary-dark);border-bottom:1px solid var(--app-border)}mat-toolbar[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--app-primary)}.toolbar-title[_ngcontent-%COMP%]{margin-left:10px;font-weight:700}.content[_ngcontent-%COMP%]{padding:24px;min-height:calc(100vh - 64px);background:var(--app-bg)}"]})};var Bi=class t{http=d(Je);apiUrl="/api/Equipos";getEquipos(){return this.http.get(this.apiUrl)}crearEquipo(n){return this.http.post(this.apiUrl,n)}editarEquipo(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};function bL(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function yL(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function CL(t,n){if(t&1){let e=qe();m(0,"mat-list-item")(1,"mat-icon",15),p(2,"groups"),u(),m(3,"span",16),p(4),u(),m(5,"button",17),I("click",function(){let r=we(e).$implicit,o=T();return Ee(o.editar(r))}),m(6,"mat-icon"),p(7,"edit"),u()()()}if(t&2){let e=n.$implicit,i=T();h(4),z(e.nombre),h(),_("disabled",i.esModoEdicion)}}function xL(t,n){t&1&&(m(0,"mat-list-item")(1,"span",16),p(2,"No hay equipos registrados."),u()())}var Mf=class t{constructor(n,e){this.fb=n;this.equipoService=e}fb;equipoService;equipoForm;equipos=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarEquipos()}crearFormulario(){this.equipoForm=this.fb.group({nombre:["",[le.required,le.maxLength(100)]]})}cargarEquipos(){this.equipoService.getEquipos().subscribe({next:n=>{this.equipos=n},error:()=>{this.error="No se han podido cargar los equipos."}})}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.equipoForm.patchValue({nombre:n.nombre}),this.mensaje="",this.error=""}guardar(){if(this.equipoForm.invalid){this.equipoForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n=this.equipoForm.get("nombre")?.value;this.esModoEdicion?this.equipoService.editarEquipo(this.editandoId,{nombre:n}).subscribe({next:e=>{this.cargando=!1;let i=this.equipos.findIndex(r=>r.id===this.editandoId);i!==-1&&(this.equipos[i]=e),this.mensaje="Equipo actualizado correctamente.",this.cancelar()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el equipo."}}):this.equipoService.crearEquipo({nombre:n}).subscribe({next:e=>{this.cargando=!1,this.equipos.push(e),this.mensaje="Equipo a\xF1adido correctamente.",this.equipoForm.reset()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el equipo."}})}cancelar(){this.editandoId=null,this.equipoForm.reset(),this.mensaje="",this.error=""}static \u0275fac=function(e){return new(e||t)(D(xn),D(Bi))};static \u0275cmp=A({type:t,selectors:[["app-equipos"]],decls:43,vars:16,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. Real Madrid"],["matSuffix",""],[4,"ngIf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],["matListItemIcon",""],["matListItemTitle",""],["mat-icon-button","","matListItemMeta","",3,"click","disabled"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),I("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Nombre"),u(),re(16,"input",6),m(17,"mat-icon",7),p(18,"badge"),u(),M(19,bL,2,0,"mat-error",8)(20,yL,2,0,"mat-error",8),u(),m(21,"div",9),p(22),u(),m(23,"mat-card-actions",10)(24,"button",11),I("click",function(){return i.cancelar()}),p(25),u(),m(26,"button",12)(27,"mat-icon"),p(28),u(),p(29),u()()()()(),m(30,"mat-card",13)(31,"mat-card-header")(32,"div",3)(33,"mat-icon"),p(34,"list"),u()(),m(35,"mat-card-title"),p(36,"Equipos registrados"),u(),m(37,"mat-card-subtitle"),p(38," Equipos existentes en la base de datos "),u()(),m(39,"mat-card-content")(40,"mat-list"),M(41,CL,8,2,"mat-list-item",14)(42,xL,3,0,"mat-list-item",8),u()()()()()),e&2){let r,o;h(6),z(i.esModoEdicion?"edit":"groups"),h(2),z(i.esModoEdicion?"Editar equipo":"A\xF1adir nuevo equipo"),h(2),K(" ",i.esModoEdicion?"Modifica el nombre del equipo":"Completa los datos del equipo que quieres a\xF1adir"," "),h(2),_("formGroup",i.equipoForm),h(7),_("ngIf",(r=i.equipoForm.get("nombre"))==null?null:r.hasError("required")),h(),_("ngIf",(o=i.equipoForm.get("nombre"))==null?null:o.hasError("maxlength")),h(),R("error",!!i.error),h(),ct(" ",i.mensaje,"",i.error," "),h(3),K(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),_("disabled",i.cargando),h(2),z(i.esModoEdicion?"save":"add"),h(),K(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar equipo"," "),h(12),_("ngForOf",i.equipos),h(),_("ngIf",i.equipos.length===0)}},dependencies:[ut,dt,Ct,Zt,Cn,Ft,Yt,yn,xt,un,jt,en,Fn,Pt,pt,tn,Zn,Mn,nn,In,Dn,En,Xe,Dt,ft,Kt,Xt,gt,ht,Lt,Jt,Vt,Vi,Qn,Ln,Pn,wi,Bt,Ht,Ut],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:#2e7d32;font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var Sf=class t{http=d(Je);apiUrl="/api/Jugadores";getJugadores(){return this.http.get(this.apiUrl)}crearJugador(n){return this.http.post(this.apiUrl,n)}editarJugador(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}eliminarJugador(n){return this.http.delete(`${this.apiUrl}/${n}`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};var EL=(t,n)=>e=>e.nombre;function DL(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function IL(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function ML(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 150 caracteres "),u())}function SL(t,n){if(t&1&&(m(0,"mat-option",18),p(1),u()),t&2){let e=n.$implicit;_("value",e.id),h(),K(" ",e.nombre," ")}}function TL(t,n){t&1&&(m(0,"mat-error"),p(1," Debes seleccionar al menos un equipo "),u())}function AL(t,n){if(t&1){let e=qe();m(0,"mat-list-item")(1,"mat-icon",19),p(2,"person"),u(),m(3,"span",20),p(4),u(),m(5,"span",21),p(6),u(),m(7,"button",22),I("click",function(){let r=we(e).$implicit,o=T();return Ee(o.editar(r))}),m(8,"mat-icon"),p(9,"edit"),u()(),m(10,"button",22),I("click",function(){let r=we(e).$implicit,o=T();return Ee(o.eliminar(r))}),m(11,"mat-icon"),p(12,"delete"),u()()()}if(t&2){let e=n.$implicit,i=T();h(4),z(e.nombre),h(2),z(e.equipos.map(Ba(4,EL,n)).join(", ")),h(),_("disabled",i.esModoEdicion),h(3),_("disabled",i.esModoEdicion)}}function kL(t,n){t&1&&(m(0,"mat-list-item")(1,"span",20),p(2,"No hay jugadores registrados."),u()())}var Tf=class t{constructor(n,e,i,r){this.fb=n;this.equipoService=e;this.jugadorService=i;this.cdr=r}fb;equipoService;jugadorService;cdr;jugadorForm;equipos=[];jugadores=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarEquipos(),this.cargarJugadores()}crearFormulario(){this.jugadorForm=this.fb.group({nombre:["",[le.required,le.maxLength(100)]],nombreCompleto:["",[le.maxLength(150)]],equipos:[[],[le.required]]})}cargarEquipos(){this.equipoService.getEquipos().subscribe({next:n=>{this.equipos=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los equipos.",this.cdr.detectChanges()}})}cargarJugadores(){this.jugadorService.getJugadores().subscribe({next:n=>{this.jugadores=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los jugadores.",this.cdr.detectChanges()}})}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.jugadorForm.patchValue({nombre:n.nombre,nombreCompleto:n.nombreCompleto,equipos:n.equipos.map(e=>e.id)}),this.mensaje="",this.error=""}guardar(){if(this.jugadorForm.invalid){this.jugadorForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n={nombre:this.jugadorForm.get("nombre")?.value,nombreCompleto:this.jugadorForm.get("nombreCompleto")?.value,equipos:this.jugadorForm.get("equipos")?.value};this.esModoEdicion?this.jugadorService.editarJugador(this.editandoId,n).subscribe({next:e=>{this.cargando=!1;let i=this.jugadores.findIndex(r=>r.id===this.editandoId);i!==-1&&(this.jugadores[i]=e),this.mensaje="Jugador actualizado correctamente.",this.cancelar(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el jugador.",this.cdr.detectChanges()}}):this.jugadorService.crearJugador(n).subscribe({next:e=>{this.cargando=!1,this.jugadores.push(e),this.mensaje="Jugador a\xF1adido correctamente.",this.jugadorForm.reset(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el jugador.",this.cdr.detectChanges()}})}eliminar(n){confirm(`\xBFSeguro que quieres eliminar el jugador "${n.nombre}"?`)&&this.jugadorService.eliminarJugador(n.id).subscribe({next:()=>{this.jugadores=this.jugadores.filter(e=>e.id!==n.id),this.mensaje="Jugador eliminado correctamente.",this.error="",this.cdr.detectChanges()},error:()=>{this.error="No se ha podido eliminar el jugador.",this.cdr.detectChanges()}})}cancelar(){this.editandoId=null,this.jugadorForm.reset({nombre:"",nombreCompleto:"",equipos:[]}),this.mensaje="",this.error=""}static \u0275fac=function(e){return new(e||t)(D(xn),D(Bi),D(Sf),D(ge))};static \u0275cmp=A({type:t,selectors:[["app-jugadores"]],decls:58,vars:19,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. Lamine Yamal"],["matSuffix",""],[4,"ngIf"],["matInput","","type","text","formControlName","nombreCompleto","placeholder","Ej. Lamine Yamal Nasraoui Ebana"],["formControlName","equipos","multiple",""],[3,"value",4,"ngFor","ngForOf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],[3,"value"],["matListItemIcon",""],["matListItemTitle",""],["matListItemLine",""],["mat-icon-button","","matListItemMeta","",3,"click","disabled"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),I("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Nombre"),u(),re(16,"input",6),m(17,"mat-icon",7),p(18,"badge"),u(),M(19,DL,2,0,"mat-error",8)(20,IL,2,0,"mat-error",8),u(),m(21,"mat-form-field",5)(22,"mat-label"),p(23,"Nombre completo"),u(),re(24,"input",9),m(25,"mat-icon",7),p(26,"badge"),u(),M(27,ML,2,0,"mat-error",8),u(),m(28,"mat-form-field",5)(29,"mat-label"),p(30,"Equipos"),u(),m(31,"mat-select",10),M(32,SL,2,2,"mat-option",11),u(),m(33,"mat-icon",7),p(34,"groups"),u(),M(35,TL,2,0,"mat-error",8),u(),m(36,"div",12),p(37),u(),m(38,"mat-card-actions",13)(39,"button",14),I("click",function(){return i.cancelar()}),p(40),u(),m(41,"button",15)(42,"mat-icon"),p(43),u(),p(44),u()()()()(),m(45,"mat-card",16)(46,"mat-card-header")(47,"div",3)(48,"mat-icon"),p(49,"list"),u()(),m(50,"mat-card-title"),p(51,"Jugadores registrados"),u(),m(52,"mat-card-subtitle"),p(53," Jugadores existentes en la base de datos "),u()(),m(54,"mat-card-content")(55,"mat-list"),M(56,AL,13,5,"mat-list-item",17)(57,kL,3,0,"mat-list-item",8),u()()()()()),e&2){let r,o,a,s;h(6),z(i.esModoEdicion?"edit":"person"),h(2),z(i.esModoEdicion?"Editar jugador":"A\xF1adir nuevo jugador"),h(2),K(" ",i.esModoEdicion?"Modifica los datos del jugador":"Completa los datos del jugador que quieres a\xF1adir"," "),h(2),_("formGroup",i.jugadorForm),h(7),_("ngIf",(r=i.jugadorForm.get("nombre"))==null?null:r.hasError("required")),h(),_("ngIf",(o=i.jugadorForm.get("nombre"))==null?null:o.hasError("maxlength")),h(7),_("ngIf",(a=i.jugadorForm.get("nombreCompleto"))==null?null:a.hasError("maxlength")),h(5),_("ngForOf",i.equipos),h(3),_("ngIf",(s=i.jugadorForm.get("equipos"))==null?null:s.hasError("required")),h(),R("error",!!i.error),h(),ct(" ",i.mensaje,"",i.error," "),h(3),K(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),_("disabled",i.cargando),h(2),z(i.esModoEdicion?"save":"add"),h(),K(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar jugador"," "),h(12),_("ngForOf",i.jugadores),h(),_("ngIf",i.jugadores.length===0)}},dependencies:[ut,dt,Ct,Zt,Cn,Ft,Yt,yn,xt,un,jt,en,Fn,Pt,wn,pt,tn,Zn,Mn,nn,In,Dn,En,Xe,Dt,ft,Kt,Xt,gt,ht,Lt,Jt,Vt,Vi,Qn,Ln,Bc,Pn,wi,Bt,ao,Ht,Ut],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:#2e7d32;font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var so=class t{http=d(Je);apiUrl="/api/TiposCromo";getTiposCromo(){return this.http.get(this.apiUrl)}crearTipoCromo(n){return this.http.post(this.apiUrl,n)}editarTipoCromo(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}eliminarTipoCromo(n){return this.http.delete(`${this.apiUrl}/${n}`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};function OL(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function RL(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function NL(t,n){if(t&1){let e=qe();m(0,"mat-list-item")(1,"mat-icon",15),p(2,"category"),u(),m(3,"span",16),p(4),u(),m(5,"button",17),I("click",function(){let r=we(e).$implicit,o=T();return Ee(o.editar(r))}),m(6,"mat-icon"),p(7,"edit"),u()(),m(8,"button",18),I("click",function(){let r=we(e).$implicit,o=T();return Ee(o.eliminar(r))}),m(9,"mat-icon",19),p(10,"delete"),u()()()}if(t&2){let e=n.$implicit,i=T();h(4),z(e.nombre),h(),_("disabled",i.esModoEdicion)}}function FL(t,n){t&1&&(m(0,"mat-list-item")(1,"span",16),p(2,"No hay tipos de cromo registrados."),u()())}var Af=class t{constructor(n,e){this.fb=n;this.tipoCromoService=e}fb;tipoCromoService;tipoCromoForm;tiposCromo=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarTiposCromo()}crearFormulario(){this.tipoCromoForm=this.fb.group({nombre:["",[le.required,le.maxLength(100)]]})}cargarTiposCromo(){this.tipoCromoService.getTiposCromo().subscribe({next:n=>{this.tiposCromo=n},error:()=>{this.error="No se han podido cargar los tipos de cromo."}})}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.tipoCromoForm.patchValue({nombre:n.nombre}),this.mensaje="",this.error=""}guardar(){if(this.tipoCromoForm.invalid){this.tipoCromoForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n=this.tipoCromoForm.get("nombre")?.value;this.esModoEdicion?this.tipoCromoService.editarTipoCromo(this.editandoId,{nombre:n}).subscribe({next:e=>{this.cargando=!1;let i=this.tiposCromo.findIndex(r=>r.id===this.editandoId);i!==-1&&(this.tiposCromo[i]=e),this.mensaje="Tipo de cromo actualizado correctamente.",this.cancelar()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el tipo de cromo."}}):this.tipoCromoService.crearTipoCromo({nombre:n}).subscribe({next:e=>{this.cargando=!1,this.tiposCromo.push(e),this.mensaje="Tipo de cromo a\xF1adido correctamente.",this.tipoCromoForm.reset()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el tipo de cromo."}})}eliminar(n){this.tipoCromoService.eliminarTipoCromo(n.id).subscribe({next:()=>{this.tiposCromo=this.tiposCromo.filter(e=>e.id!==n.id),this.mensaje="Tipo de cromo eliminado correctamente.",this.error=""},error:()=>{this.mensaje="",this.error="No se ha podido eliminar el tipo de cromo."}})}cancelar(){this.editandoId=null,this.tipoCromoForm.reset(),this.mensaje="",this.error=""}static \u0275fac=function(e){return new(e||t)(D(xn),D(so))};static \u0275cmp=A({type:t,selectors:[["app-tipos-cromo"]],decls:43,vars:16,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. Normal, Caja, Flor, Hologr\xE1fico..."],["matSuffix",""],[4,"ngIf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],["matListItemIcon",""],["matListItemTitle",""],["mat-icon-button","","matListItemMeta","",3,"click","disabled"],["mat-icon-button","","matListItemMeta","",3,"click"],["color","warn"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),I("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Nombre"),u(),re(16,"input",6),m(17,"mat-icon",7),p(18,"sell"),u(),M(19,OL,2,0,"mat-error",8)(20,RL,2,0,"mat-error",8),u(),m(21,"div",9),p(22),u(),m(23,"mat-card-actions",10)(24,"button",11),I("click",function(){return i.cancelar()}),p(25),u(),m(26,"button",12)(27,"mat-icon"),p(28),u(),p(29),u()()()()(),m(30,"mat-card",13)(31,"mat-card-header")(32,"div",3)(33,"mat-icon"),p(34,"list"),u()(),m(35,"mat-card-title"),p(36,"Tipos de cromo registrados"),u(),m(37,"mat-card-subtitle"),p(38," Tipos de cromo existentes en la base de datos "),u()(),m(39,"mat-card-content")(40,"mat-list"),M(41,NL,11,2,"mat-list-item",14)(42,FL,3,0,"mat-list-item",8),u()()()()()),e&2){let r,o;h(6),z(i.esModoEdicion?"edit":"category"),h(2),z(i.esModoEdicion?"Editar tipo de cromo":"A\xF1adir nuevo tipo de cromo"),h(2),K(" ",i.esModoEdicion?"Modifica el nombre del tipo de cromo":"Completa los datos del tipo de cromo que quieres a\xF1adir"," "),h(2),_("formGroup",i.tipoCromoForm),h(7),_("ngIf",(r=i.tipoCromoForm.get("nombre"))==null?null:r.hasError("required")),h(),_("ngIf",(o=i.tipoCromoForm.get("nombre"))==null?null:o.hasError("maxlength")),h(),R("error",!!i.error),h(),ct(" ",i.mensaje,"",i.error," "),h(3),K(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),_("disabled",i.cargando),h(2),z(i.esModoEdicion?"save":"add"),h(),K(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar tipo"," "),h(12),_("ngForOf",i.tiposCromo),h(),_("ngIf",i.tiposCromo.length===0)}},dependencies:[ut,dt,Ct,Zt,Cn,Ft,Yt,yn,xt,un,jt,en,Fn,Pt,pt,tn,Zn,Mn,nn,In,Dn,En,Xe,Dt,ft,Kt,Xt,gt,ht,Lt,Jt,Vt,Vi,Qn,Ln,Pn,wi,Bt,Ht,Ut],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:#2e7d32;font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var PL=["tooltip"],LL=20;var jL=new y("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>pr(t,{scrollThrottle:LL})}}),VL=new y("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var KD="tooltip-panel",BL={passive:!0},HL=8,UL=8,zL=24,$L=200,P_=(()=>{class t{_elementRef=d(N);_ngZone=d(F);_platform=d(ve);_ariaDescriber=d(zE);_focusMonitor=d(bi);_dir=d(Et);_injector=d(W);_viewContainerRef=d(nt);_mediaMatcher=d(ms);_document=d(G);_renderer=d(Se);_animationsDisabled=He();_defaultOptions=d(VL,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=XD;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=et(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=et(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=Qt(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=Qt(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new E;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=HL}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(ye(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new oo(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(ye(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let a=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&a._origin instanceof N)return this._overlayRef;this._detach()}let i=this._injector.get(dr).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${KD}`,o=aa(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(ye(this._destroyed)).subscribe(a=>{this._updateCurrentPositionClass(a.connectionPair),this._tooltipInstance&&a.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=hr(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(jL)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(ye(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(ye(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(ye(this._destroyed)).subscribe(a=>{a.preventDefault(),a.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(ye(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(C(C({},r.main),o.main)),this._addOffset(C(C({},r.fallback),o.fallback))])}_addOffset(e){let i=UL,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:a}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:a}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:a}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:a}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),st(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,a;if(i==="center"?this._dir&&this._dir.value==="rtl"?a=r==="end"?"left":"right":a=r==="start"?"left":"right":a=i==="bottom"&&o==="top"?"above":"below",a!==this._currentPosition){let s=this._overlayRef;if(s){let l=`${this._cssClassPrefix}-${KD}-`;s.removePanelClass(l+this._currentPosition),s.addPanelClass(l+a)}this._currentPosition=a}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,BL))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||st({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!We(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),XD=(()=>{class t{_changeDetectorRef=d(ge);_elementRef=d(N);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=He();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new E;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";constructor(){}show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>zL&&e.width>=$L}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let a=getComputedStyle(i);(a.getPropertyValue("animation-duration")==="0s"||a.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&Ge(PL,7),i&2){let o;$(o=q())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&I("mouseleave",function(a){return r._handleMouseLeave(a)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(lt(0,"div",1,0),Lu("animationend",function(a){return r._handleAnimationEnd(a)}),lt(2,"div",2),p(3),Rt()()),i&2&&(ln(r.tooltipClass),R("mdc-tooltip--multiline",r._isMultiline),h(3),z(r.message))},styles:[`.mat-mdc-tooltip {
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
`],encapsulation:2,changeDetection:0})}return t})();var JD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[Ec,xi,_e,Gn]})}return t})();var lo=class t{http=d(Je);apiUrl="/api/Albumes";getAlbumes(){return this.http.get(this.apiUrl)}crearAlbum(n){return this.http.post(this.apiUrl,n)}editarAlbum(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}eliminarAlbum(n){return this.http.delete(`${this.apiUrl}/${n}`)}getAlbumConCromos(n){return this.http.get(`${this.apiUrl}/${n}/Cromos`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};var Ts=class t{http=d(Je);apiUrl="/api/Editoriales";getEditoriales(){return this.http.get(this.apiUrl)}crearEditorial(n){return this.http.post(this.apiUrl,n)}editarEditorial(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}eliminarEditorial(n){return this.http.delete(`${this.apiUrl}/${n}`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};var GL=t=>["/album",t],WL=(t,n)=>e=>e.nombre,YL=(t,n)=>e=>e.nombre;function ZL(t,n){if(t&1&&(m(0,"mat-option",23),p(1),u()),t&2){let e=n.$implicit;_("value",e.id),h(),K(" ",e.nombre," ")}}function QL(t,n){t&1&&(m(0,"mat-error"),p(1," La editorial es obligatoria "),u())}function KL(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function XL(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function JL(t,n){t&1&&(m(0,"mat-error"),p(1," La temporada es obligatoria "),u())}function e2(t,n){t&1&&(m(0,"mat-error"),p(1," La temporada no puede superar los 50 caracteres "),u())}function t2(t,n){if(t&1&&(m(0,"mat-option",23),p(1),u()),t&2){let e=n.$implicit;_("value",e.id),h(),K(" ",e.nombre," ")}}function n2(t,n){t&1&&(m(0,"mat-error"),p(1," Selecciona un equipo "),u())}function i2(t,n){if(t&1){let e=qe();m(0,"div",24)(1,"span",25),p(2),u(),m(3,"mat-form-field",26)(4,"mat-select",27),M(5,t2,2,2,"mat-option",7),u(),M(6,n2,2,0,"mat-error",9),u(),m(7,"div",28)(8,"button",29),I("click",function(){let r=we(e).index,o=T();return Ee(o.moverEquipo(r,-1))}),m(9,"mat-icon"),p(10,"arrow_upward"),u()(),m(11,"button",29),I("click",function(){let r=we(e).index,o=T();return Ee(o.moverEquipo(r,1))}),m(12,"mat-icon"),p(13,"arrow_downward"),u()()(),m(14,"button",30),I("click",function(){let r=we(e).index,o=T();return Ee(o.quitarEquipo(r))}),m(15,"mat-icon",31),p(16,"delete"),u()()()}if(t&2){let e=n.index,i=T();h(2),z(e+1),h(2),_("formControl",i.equipoControl(e)),h(),_("ngForOf",i.equipos),h(),_("ngIf",i.equipoControl(e).hasError("required")&&i.equipoControl(e).touched),h(2),_("disabled",e===0),h(3),_("disabled",e===i.equiposForm.length-1)}}function r2(t,n){t&1&&(m(0,"div",32),p(1," A\xFAn no hay equipos a\xF1adidos. "),u())}function o2(t,n){if(t&1&&(m(0,"mat-option",23),p(1),u()),t&2){let e=n.$implicit;_("value",e.id),h(),K(" ",e.nombre," ")}}function a2(t,n){t&1&&(m(0,"mat-error"),p(1," Selecciona un tipo de cromo "),u())}function s2(t,n){if(t&1){let e=qe();m(0,"div",24)(1,"span",25),p(2),u(),m(3,"mat-form-field",26)(4,"mat-select",27),M(5,o2,2,2,"mat-option",7),u(),M(6,a2,2,0,"mat-error",9),u(),m(7,"div",28)(8,"button",29),I("click",function(){let r=we(e).index,o=T();return Ee(o.moverTipoCromo(r,-1))}),m(9,"mat-icon"),p(10,"arrow_upward"),u()(),m(11,"button",29),I("click",function(){let r=we(e).index,o=T();return Ee(o.moverTipoCromo(r,1))}),m(12,"mat-icon"),p(13,"arrow_downward"),u()()(),m(14,"button",30),I("click",function(){let r=we(e).index,o=T();return Ee(o.quitarTipoCromo(r))}),m(15,"mat-icon",31),p(16,"delete"),u()()()}if(t&2){let e=n.index,i=T();h(2),z(e+1),h(2),_("formControl",i.tipoCromoControl(e)),h(),_("ngForOf",i.tiposCromo),h(),_("ngIf",i.tipoCromoControl(e).hasError("required")&&i.tipoCromoControl(e).touched),h(2),_("disabled",e===0),h(3),_("disabled",e===i.tiposCromoForm.length-1)}}function l2(t,n){t&1&&(m(0,"div",32),p(1," A\xFAn no hay tipos de cromo a\xF1adidos. "),u())}function c2(t,n){if(t&1){let e=qe();m(0,"mat-list-item")(1,"mat-icon",33),p(2,"album"),u(),m(3,"span",34),p(4),u(),m(5,"span",35),p(6),u(),m(7,"span",35),p(8),u(),m(9,"span",35),p(10),u(),m(11,"span",36)(12,"mat-icon",37),p(13,"style"),u(),p(14),u(),m(15,"a",38)(16,"mat-icon"),p(17,"visibility"),u()(),m(18,"button",39),I("click",function(){let r=we(e).$implicit,o=T();return Ee(o.editar(r))}),m(19,"mat-icon"),p(20,"edit"),u()(),m(21,"button",40),I("click",function(){let r=we(e).$implicit,o=T();return Ee(o.eliminar(r))}),m(22,"mat-icon",31),p(23,"delete"),u()()()}if(t&2){let e=n.$implicit,i=T();h(4),z(e.nombre),h(2),ct(" ",e.editorial.nombre," \xB7 ",e.temporada," "),h(2),K(" Equipos: ",e.equipos.map(Ba(8,WL,n)).join(", ")," "),h(2),K(" Cromos especiales: ",e.tiposCromo.map(Ba(9,YL,n)).join(", ")," "),h(4),K(" ",e.contadorCromos," cromos "),h(),_("routerLink",Gg(10,GL,e.id)),h(3),_("disabled",i.esModoEdicion)}}function d2(t,n){t&1&&(m(0,"mat-list-item")(1,"span",34),p(2,"No hay \xE1lbumes registrados."),u()())}var kf=class t{constructor(n,e,i,r,o,a){this.fb=n;this.albumService=e;this.editorialService=i;this.equipoService=r;this.tipoCromoService=o;this.cdr=a}fb;albumService;editorialService;equipoService;tipoCromoService;cdr;albumForm;albumes=[];editoriales=[];equipos=[];tiposCromo=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarAlbumes(),this.cargarEditoriales(),this.cargarEquipos(),this.cargarTiposCromo()}get equiposForm(){return this.albumForm.get("equipos")}get tiposCromoForm(){return this.albumForm.get("tiposCromo")}equipoControl(n){return this.equiposForm.at(n)}tipoCromoControl(n){return this.tiposCromoForm.at(n)}crearFormulario(){this.albumForm=this.fb.group({nombre:["",[le.required,le.maxLength(100)]],temporada:["",[le.required,le.maxLength(50)]],editorialId:[null,[le.required]],equipos:this.fb.array([],[le.required]),tiposCromo:this.fb.array([],[le.required])})}cargarAlbumes(){this.albumService.getAlbumes().subscribe({next:n=>{this.albumes=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los \xE1lbumes.",this.cdr.detectChanges()}})}cargarEditoriales(){this.editorialService.getEditoriales().subscribe({next:n=>{this.editoriales=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar las editoriales.",this.cdr.detectChanges()}})}cargarEquipos(){this.equipoService.getEquipos().subscribe({next:n=>{this.equipos=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los equipos.",this.cdr.detectChanges()}})}cargarTiposCromo(){this.tipoCromoService.getTiposCromo().subscribe({next:n=>{this.tiposCromo=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los tipos de cromo.",this.cdr.detectChanges()}})}agregarEquipo(){this.equiposForm.push(this.fb.control(null,le.required))}agregarTipoCromo(){this.tiposCromoForm.push(this.fb.control(null,le.required))}quitarEquipo(n){this.equiposForm.removeAt(n)}quitarTipoCromo(n){this.tiposCromoForm.removeAt(n)}moverEquipo(n,e){let i=n+e;i<0||i>=this.equiposForm.length||this._mover(this.equiposForm,n,i)}moverTipoCromo(n,e){let i=n+e;i<0||i>=this.tiposCromoForm.length||this._mover(this.tiposCromoForm,n,i)}_mover(n,e,i){let r=n.at(e).value;n.removeAt(e),n.insert(i,this.fb.control(r,le.required))}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.albumForm.patchValue({nombre:n.nombre,temporada:n.temporada,editorialId:n.editorial.id}),this.equiposForm.clear(),n.equipos.slice().sort((e,i)=>e.orden-i.orden).forEach(e=>this.equiposForm.push(this.fb.control(e.id,le.required))),this.tiposCromoForm.clear(),n.tiposCromo.slice().sort((e,i)=>e.orden-i.orden).forEach(e=>this.tiposCromoForm.push(this.fb.control(e.id,le.required))),this.mensaje="",this.error="",this.cdr.detectChanges()}guardar(){if(this.albumForm.invalid){this.albumForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n={nombre:this.albumForm.get("nombre")?.value,temporada:this.albumForm.get("temporada")?.value,editorialId:this.albumForm.get("editorialId")?.value,equipos:this.equiposForm.value.map(e=>({equipoId:e})),tiposCromo:this.tiposCromoForm.value.map(e=>({tipoCromoId:e}))};this.esModoEdicion?this.albumService.editarAlbum(this.editandoId,n).subscribe({next:()=>{this.cargando=!1,this.mensaje="\xC1lbum actualizado correctamente.",this.cargarAlbumes(),this.cancelar(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el \xE1lbum.",this.cdr.detectChanges()}}):this.albumService.crearAlbum(n).subscribe({next:()=>{this.cargando=!1,this.mensaje="\xC1lbum a\xF1adido correctamente.",this.cargarAlbumes(),this.cancelar(),this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir el \xE1lbum.",this.cdr.detectChanges()}})}eliminar(n){this.albumService.eliminarAlbum(n.id).subscribe({next:()=>{this.albumes=this.albumes.filter(e=>e.id!==n.id),this.mensaje="\xC1lbum eliminado correctamente.",this.error="",this.cdr.detectChanges()},error:()=>{this.mensaje="",this.error="No se ha podido eliminar el \xE1lbum.",this.cdr.detectChanges()}})}cancelar(){this.editandoId=null,this.albumForm.reset(),this.equiposForm.clear(),this.tiposCromoForm.clear(),this.mensaje="",this.error="",this.cdr.detectChanges()}static \u0275fac=function(e){return new(e||t)(D(xn),D(lo),D(Ts),D(Bi),D(so),D(ge))};static \u0275cmp=A({type:t,selectors:[["app-albumes"]],decls:81,vars:24,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["formControlName","editorialId"],[3,"value",4,"ngFor","ngForOf"],["matSuffix",""],[4,"ngIf"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. La Liga 2025"],["matInput","","type","text","formControlName","temporada","placeholder","Ej. 2024/2025"],[1,"list-block"],[1,"list-block-title"],["mat-icon-button","","type","button",1,"add-item-btn",3,"click"],["class","item-row",4,"ngFor","ngForOf"],["class","empty-note",4,"ngIf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],[3,"value"],[1,"item-row"],[1,"item-index"],["appearance","outline",1,"item-field"],[3,"formControl"],[1,"move-buttons"],["mat-icon-button","","type","button",3,"click","disabled"],["mat-icon-button","","type","button",3,"click"],["color","warn"],[1,"empty-note"],["matListItemIcon",""],["matListItemTitle",""],["matListItemLine","",1,"album-detail"],["matListItemLine","",1,"album-counter"],[1,"counter-icon"],["mat-icon-button","","matListItemMeta","","matTooltip","Ver todo","matTooltipPosition","left",3,"routerLink"],["mat-icon-button","","matListItemMeta","",3,"click","disabled"],["mat-icon-button","","matListItemMeta","",3,"click"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),I("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Editorial"),u(),m(16,"mat-select",6),M(17,ZL,2,2,"mat-option",7),u(),m(18,"mat-icon",8),p(19,"business"),u(),M(20,QL,2,0,"mat-error",9),u(),m(21,"mat-form-field",5)(22,"mat-label"),p(23,"Nombre"),u(),re(24,"input",10),m(25,"mat-icon",8),p(26,"badge"),u(),M(27,KL,2,0,"mat-error",9)(28,XL,2,0,"mat-error",9),u(),m(29,"mat-form-field",5)(30,"mat-label"),p(31,"Temporada"),u(),re(32,"input",11),m(33,"mat-icon",8),p(34,"calendar_today"),u(),M(35,JL,2,0,"mat-error",9)(36,e2,2,0,"mat-error",9),u(),m(37,"div",12)(38,"div",13)(39,"mat-icon"),p(40,"groups"),u(),m(41,"span"),p(42,"Equipos"),u(),m(43,"button",14),I("click",function(){return i.agregarEquipo()}),m(44,"mat-icon"),p(45,"add"),u()()(),M(46,i2,17,6,"div",15)(47,r2,2,0,"div",16),u(),m(48,"div",12)(49,"div",13)(50,"mat-icon"),p(51,"category"),u(),m(52,"span"),p(53,"Cromos especiales"),u(),m(54,"button",14),I("click",function(){return i.agregarTipoCromo()}),m(55,"mat-icon"),p(56,"add"),u()()(),M(57,s2,17,6,"div",15)(58,l2,2,0,"div",16),u(),m(59,"div",17),p(60),u(),m(61,"mat-card-actions",18)(62,"button",19),I("click",function(){return i.cancelar()}),p(63),u(),m(64,"button",20)(65,"mat-icon"),p(66),u(),p(67),u()()()()(),m(68,"mat-card",21)(69,"mat-card-header")(70,"div",3)(71,"mat-icon"),p(72,"list"),u()(),m(73,"mat-card-title"),p(74,"\xC1lbumes registrados"),u(),m(75,"mat-card-subtitle"),p(76," \xC1lbumes existentes en la base de datos "),u()(),m(77,"mat-card-content")(78,"mat-list"),M(79,c2,24,12,"mat-list-item",22)(80,d2,3,0,"mat-list-item",9),u()()()()()),e&2){let r,o,a,s,l;h(6),z(i.esModoEdicion?"edit":"album"),h(2),z(i.esModoEdicion?"Editar \xE1lbum":"A\xF1adir nuevo \xE1lbum"),h(2),K(" ",i.esModoEdicion?"Modifica los datos del \xE1lbum":"Completa los datos del \xE1lbum que quieres a\xF1adir"," "),h(2),_("formGroup",i.albumForm),h(5),_("ngForOf",i.editoriales),h(3),_("ngIf",(r=i.albumForm.get("editorialId"))==null?null:r.hasError("required")),h(7),_("ngIf",(o=i.albumForm.get("nombre"))==null?null:o.hasError("required")),h(),_("ngIf",(a=i.albumForm.get("nombre"))==null?null:a.hasError("maxlength")),h(7),_("ngIf",(s=i.albumForm.get("temporada"))==null?null:s.hasError("required")),h(),_("ngIf",(l=i.albumForm.get("temporada"))==null?null:l.hasError("maxlength")),h(10),_("ngForOf",i.equiposForm.controls),h(),_("ngIf",i.equiposForm.length===0),h(10),_("ngForOf",i.tiposCromoForm.controls),h(),_("ngIf",i.tiposCromoForm.length===0),h(),R("error",!!i.error),h(),ct(" ",i.mensaje,"",i.error," "),h(3),K(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),_("disabled",i.cargando),h(2),z(i.esModoEdicion?"save":"add"),h(),K(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar \xE1lbum"," "),h(12),_("ngForOf",i.albumes),h(),_("ngIf",i.albumes.length===0)}},dependencies:[ut,dt,Ct,$n,JD,P_,Zt,Cn,Ft,Yt,yn,vc,xt,un,jt,en,Fn,Pt,wn,pt,tn,Zn,Mn,nn,In,Dn,En,Xe,Dt,ft,Kt,Xt,gt,ht,Lt,Jt,Vt,Vi,Qn,Ln,Bc,Pn,wi,Bt,ao,Ht,Ut],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:#f5f7fa}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:#2e7d32;font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}.list-block[_ngcontent-%COMP%]{border:1px solid #e0e0e0;border-radius:8px;padding:12px;display:flex;flex-direction:column;gap:8px;background:#fafafa}.list-block-title[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;font-weight:500;color:#333}.list-block-title[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:#1976d2}.add-item-btn[_ngcontent-%COMP%]{margin-left:auto}.item-row[_ngcontent-%COMP%]{display:flex;align-items:center;gap:4px}.item-index[_ngcontent-%COMP%]{display:inline-flex;align-items:center;justify-content:center;min-width:28px;height:28px;padding:0 8px;border-radius:14px;background:#1976d2;color:#fff;font-size:13px;font-weight:500}.item-field[_ngcontent-%COMP%]{flex:1 1 auto}.move-buttons[_ngcontent-%COMP%]{display:flex;flex-direction:column}.move-buttons[_ngcontent-%COMP%]   mat-icon-button[_ngcontent-%COMP%]{width:32px;height:32px;line-height:32px}.empty-note[_ngcontent-%COMP%]{color:#888;font-size:13px;padding:4px 0}.album-detail[_ngcontent-%COMP%]{font-size:13px;color:#666}.album-counter[_ngcontent-%COMP%]{display:flex;align-items:center;gap:4px;font-size:13px;color:#1976d2;font-weight:500}.counter-icon[_ngcontent-%COMP%]{font-size:16px;width:16px;height:16px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var u2=["determinateSpinner"];function m2(t,n){if(t&1&&(Ai(),m(0,"svg",11),re(1,"circle",12),u()),t&2){let e=T();ce("viewBox",e._viewBox()),h(),On("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),ce("r",e._circleRadius())}}var f2=new y("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:tI})}),tI=100,p2=10,As=(()=>{class t{_elementRef=d(N);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=d(f2),i=v_(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=tI;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-p2)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&Ge(u2,5),i&2){let o;$(o=q())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(ce("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),ln("mat-"+r.color),On("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),R("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",pi],diameter:[2,"diameter","diameter",pi],strokeWidth:[2,"strokeWidth","strokeWidth",pi]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(M(0,m2,2,8,"ng-template",null,0,Ha),m(2,"div",2,1),Ai(),m(4,"svg",3),re(5,"circle",4),u()(),ul(),m(6,"div",5)(7,"div",6)(8,"div",7),Va(9,8),u(),m(10,"div",9),Va(11,8),u(),m(12,"div",10),Va(13,8),u()()()),i&2){let o=yt(1);h(4),ce("viewBox",r._viewBox()),h(),On("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),ce("r",r._circleRadius()),h(4),_("ngTemplateOutlet",o),h(2),_("ngTemplateOutlet",o),h(2),_("ngTemplateOutlet",o)}},dependencies:[Ll],styles:[`.mat-mdc-progress-spinner {
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
`],encapsulation:2,changeDetection:0})}return t})();var ks=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[_e]})}return t})();var Of=class t{http=d(Je);apiUrl="/api";cargarResumen(){return wo({cromos:this.http.get(`${this.apiUrl}/Cromo`),jugadores:this.http.get(`${this.apiUrl}/Jugadores`),equipos:this.http.get(`${this.apiUrl}/Equipos`),ediciones:this.http.get(`${this.apiUrl}/Ediciones`),editoriales:this.http.get(`${this.apiUrl}/Editoriales`),publicaciones:this.http.get(`${this.apiUrl}/Publicaciones`),tiposCromo:this.http.get(`${this.apiUrl}/TiposCromo`),albumes:this.http.get(`${this.apiUrl}/Albumes`),colecciones:this.http.get(`${this.apiUrl}/Colecciones`),usuarios:this.http.get(`${this.apiUrl}/Usuarios`),usuariosCromo:this.http.get(`${this.apiUrl}/UsuariosCromos`)}).pipe(V(({cromos:n,jugadores:e,equipos:i,ediciones:r,editoriales:o,publicaciones:a,tiposCromo:s,albumes:l,colecciones:c,usuarios:f,usuariosCromo:g})=>({cromos:n.length,jugadores:e.length,equipos:i.length,ediciones:r.length,editoriales:o.length,publicaciones:a.length,tiposCromo:s.length,albumes:l.length,colecciones:c.length,usuarios:f.length,cromosPorUsuario:f.length>0?Math.round(g.length/f.length):0})))}buscarCromos(n){return this.http.get(`${this.apiUrl}/Cromo`,{params:n?{q:n}:{}})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};function g2(t,n){if(t&1&&(m(0,"a",19)(1,"mat-card-content")(2,"div",20)(3,"mat-icon"),p(4),u()(),m(5,"div",21)(6,"p",22),p(7),u(),m(8,"p",23),p(9),u(),m(10,"p",24),p(11),u()()()()),t&2){let e=T().$implicit,i=T(2);_("routerLink",e.ruta),h(4),z(e.icono),h(3),z(i.valor(e.clave)),h(2),z(e.titulo),h(2),z(e.descripcion)}}function v2(t,n){if(t&1&&(m(0,"mat-card",25)(1,"mat-card-content")(2,"div",20)(3,"mat-icon"),p(4),u()(),m(5,"div",21)(6,"p",22),p(7),u(),m(8,"p",23),p(9),u(),m(10,"p",24),p(11),u()()()()),t&2){let e=T().$implicit,i=T(2);h(4),z(e.icono),h(3),z(i.valor(e.clave)),h(2),z(e.titulo),h(2),z(e.descripcion)}}function _2(t,n){if(t&1&&(m(0,"div",16),M(1,g2,12,5,"a",17)(2,v2,12,4,"mat-card",18),u()),t&2){let e=n.$implicit;h(),_("ngIf",e.ruta),h(),_("ngIf",!e.ruta)}}function b2(t,n){if(t&1&&(m(0,"div",8)(1,"div",9)(2,"mat-card",10)(3,"div",11)(4,"mat-icon",12),p(5,"style"),u(),m(6,"div")(7,"p",13),p(8),u(),m(9,"p",14),p(10,"Cromos en total"),u()()(),m(11,"mat-card-content")(12,"p")(13,"mat-icon"),p(14,"style"),u(),p(15),u(),m(16,"p")(17,"mat-icon"),p(18,"album"),u(),p(19),u()()()(),M(20,_2,3,2,"div",15),u()),t&2){let e=T();h(8),z(e.resumen.cromos),h(7),K(" ",e.resumen.cromosPorUsuario," cromos por usuario "),h(4),K(" ",e.resumen.albumes," \xE1lbumes disponibles "),h(),_("ngForOf",e.tarjetas)}}function y2(t,n){if(t&1){let e=qe();m(0,"div",36)(1,"button",37),I("click",function(){we(e);let r=T(2);return Ee(r.limpiar())}),m(2,"mat-icon"),p(3,"close"),u(),p(4," Limpiar "),u()()}}function C2(t,n){t&1&&(m(0,"div",38),re(1,"mat-spinner",39),m(2,"p"),p(3,"Buscando..."),u()())}function x2(t,n){if(t&1&&(m(0,"div",40)(1,"mat-icon"),p(2,"search_off"),u(),m(3,"p"),p(4),u()()),t&2){let e=T(2);h(4),K("No hay cromos para \xAB",e.buscado,"\xBB.")}}function w2(t,n){if(t&1&&(m(0,"div",38)(1,"p",41),p(2),u()()),t&2){let e=T(2);h(2),ct("",e.resultados.length," cromo(s) para \xAB",e.buscado,"\xBB")}}function E2(t,n){if(t&1&&(m(0,"mat-card",42)(1,"mat-card-content")(2,"div",43)(3,"mat-icon"),p(4,"style"),u()(),m(5,"div",44)(6,"p",45),p(7),u(),m(8,"p",46),p(9),u(),m(10,"p",47)(11,"mat-icon"),p(12,"groups"),u(),p(13),m(14,"mat-icon",48),p(15,"album"),u(),p(16),u()()()()),t&2){let e=n.$implicit;h(7),K("N.\xBA ",e.numero),h(2),z((e.jugador==null?null:e.jugador.nombreCompleto)||(e.jugador==null?null:e.jugador.nombre)||"\u2014"),h(4),K(" ",(e.equipo==null?null:e.equipo.nombre)||"\u2014"," "),h(3),K(" ",(e.album==null?null:e.album.nombre)||"\u2014"," ")}}function D2(t,n){if(t&1&&(m(0,"section",26)(1,"mat-card",27)(2,"mat-card-header")(3,"div",28)(4,"mat-icon"),p(5,"search"),u()(),m(6,"mat-card-title"),p(7,"Buscar cromos"),u(),m(8,"mat-card-subtitle"),p(9,"Busca por nombre de jugador o de equipo"),u()(),m(10,"mat-card-content")(11,"mat-form-field",29)(12,"mat-label"),p(13,"Jugador o equipo"),u(),re(14,"input",30),m(15,"mat-icon",31),p(16,"search"),u()(),M(17,y2,5,0,"div",32),u()(),M(18,C2,4,0,"div",33)(19,x2,5,1,"div",34)(20,w2,3,2,"div",33)(21,E2,17,4,"mat-card",35),u()),t&2){let e=T();h(14),_("formControl",e.busqueda),h(3),_("ngIf",e.busqueda.value),h(),_("ngIf",e.buscando),h(),_("ngIf",!e.buscando&&e.sinResultados),h(),_("ngIf",!e.buscando&&e.buscado&&!e.sinResultados),h(),_("ngForOf",e.resultados)}}function I2(t,n){t&1&&(m(0,"div",49),re(1,"mat-spinner",50),m(2,"p"),p(3,"Cargando datos del panel..."),u()())}function M2(t,n){if(t&1&&(m(0,"div",51)(1,"mat-icon"),p(2,"error_outline"),u(),m(3,"p"),p(4),u()()),t&2){let e=T();h(4),z(e.error)}}var Rf=class t{constructor(n,e){this.adminService=n;this.cdr=e}adminService;cdr;cargando=!0;error="";resumen=null;busqueda=new ss("");buscando=!1;resultados=[];buscado="";sinResultados=!1;subBusqueda;tarjetas=[{clave:"cromos",titulo:"Cromos",descripcion:"Personajes y tarjetas",icono:"style",ruta:"/cromos"},{clave:"jugadores",titulo:"Jugadores",descripcion:"Deportistas registrados",icono:"person",ruta:"/jugadores"},{clave:"equipos",titulo:"Equipos",descripcion:"Clubes y selecciones",icono:"groups",ruta:"/equipos"},{clave:"ediciones",titulo:"Ediciones",descripcion:"Lanzamientos y series",icono:"auto_awesome",ruta:"/ediciones"},{clave:"editoriales",titulo:"Editoriales",descripcion:"Empresas editoras",icono:"storefront",ruta:"/editoriales"},{clave:"publicaciones",titulo:"Publicaciones",descripcion:"Entregas publicadas",icono:"newspaper"},{clave:"tiposCromo",titulo:"Tipos de cromo",descripcion:"Categor\xEDas especiales",icono:"category",ruta:"/tipos-cromo"},{clave:"albumes",titulo:"\xC1lbumes",descripcion:"Colecciones armadas",icono:"album",ruta:"/albumes"},{clave:"colecciones",titulo:"Colecciones",descripcion:"Conjuntos definidos",icono:"collections_bookmark",ruta:"/albumes"},{clave:"usuarios",titulo:"Usuarios",descripcion:"Cuentas activas",icono:"group"}];ngOnInit(){this.cargarResumen(),this.subBusqueda=this.busqueda.valueChanges.pipe(Xn(350),va(),Ze(n=>{let e=(n??"").trim();return e?(this.buscando=!0,this.sinResultados=!1,this.adminService.buscarCromos(e)):(this.resultados=[],this.buscado="",this.sinResultados=!1,this.buscando=!1,this.cdr.detectChanges(),Z([]))})).subscribe({next:n=>{this.buscando=!1,this.resultados=n,this.buscado=(this.busqueda.value??"").trim(),this.sinResultados=this.buscado!==""&&n.length===0,this.cdr.detectChanges()},error:()=>{this.buscando=!1,this.error="No se ha podido realizar la b\xFAsqueda.",this.cdr.detectChanges()}})}ngOnDestroy(){this.subBusqueda?.unsubscribe()}limpiar(){this.busqueda.setValue(""),this.resultados=[],this.buscado="",this.sinResultados=!1}cargarResumen(){this.cargando=!0,this.error="",this.adminService.cargarResumen().subscribe({next:n=>{this.resumen=n,this.cargando=!1,this.cdr.detectChanges()},error:()=>{this.cargando=!1,this.error="No se ha podido cargar el panel de administraci\xF3n.",this.cdr.detectChanges()}})}valor(n){return this.resumen?this.resumen[n]??0:0}static \u0275fac=function(e){return new(e||t)(D(Of),D(ge))};static \u0275cmp=A({type:t,selectors:[["app-admin-panel"]],decls:15,vars:5,consts:[[1,"panel"],[1,"panel-header"],[1,"subtitle"],["mat-stroked-button","","color","primary",3,"click","disabled"],["class","card-grid",4,"ngIf"],["class","busqueda-section",4,"ngIf"],["class","cargando",4,"ngIf"],["class","error",4,"ngIf"],[1,"card-grid"],[1,"highlight"],[1,"hero"],[1,"hero-body"],[1,"hero-icon"],[1,"hero-title"],[1,"hero-label"],["class","grid-item",4,"ngFor","ngForOf"],[1,"grid-item"],["mat-card","","class","stat-card",3,"routerLink",4,"ngIf"],["class","stat-card",4,"ngIf"],["mat-card","",1,"stat-card",3,"routerLink"],[1,"stat-icon"],[1,"stat-body"],[1,"stat-value"],[1,"stat-title"],[1,"stat-desc"],[1,"stat-card"],[1,"busqueda-section"],[1,"busqueda-card"],["mat-card-avatar","",1,"busqueda-icon"],["appearance","outline",1,"full-width"],["matInput","","type","text","placeholder","Ej. Lamine Yamal o FC Barcelona",3,"formControl"],["matSuffix",""],["class","busqueda-actions",4,"ngIf"],["class","resultados",4,"ngIf"],["class","resultados vacio",4,"ngIf"],["class","result-card",4,"ngFor","ngForOf"],[1,"busqueda-actions"],["mat-button","","type","button",3,"click"],[1,"resultados"],["diameter","32"],[1,"resultados","vacio"],[1,"resultado-titulo"],[1,"result-card"],[1,"result-icon"],[1,"result-body"],[1,"result-numero"],[1,"result-jugador"],[1,"result-meta"],[1,"sep"],[1,"cargando"],["diameter","40"],[1,"error"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"header",1)(2,"div")(3,"h1"),p(4,"Panel de administraci\xF3n"),u(),m(5,"p",2),p(6,"Resumen general de la colecci\xF3n CromosList"),u()(),m(7,"button",3),I("click",function(){return i.cargarResumen()}),m(8,"mat-icon"),p(9,"refresh"),u(),p(10," Actualizar "),u()(),M(11,b2,21,4,"div",4)(12,D2,22,6,"section",5)(13,I2,4,0,"div",6)(14,M2,5,1,"div",7),u()),e&2&&(h(7),_("disabled",i.cargando),h(4),_("ngIf",i.resumen),h(),_("ngIf",i.resumen),h(),_("ngIf",i.cargando&&!i.resumen),h(),_("ngIf",i.error&&!i.cargando))},dependencies:[ut,dt,Ct,$n,Zt,Ft,Yt,vc,ks,As,jt,en,Pt,pt,tn,Mn,nn,In,Dn,En,Xe,Dt,ft,Xt,gt,ht,Lt,Jt,Vt,Bt,Ht,Ut],styles:['@charset "UTF-8";.panel[_ngcontent-%COMP%]{max-width:1200px;margin:0 auto;display:flex;flex-direction:column;gap:24px}.panel-header[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}.panel-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0;font-size:26px;font-weight:700;color:var(--app-primary-dark)}.panel-header[_ngcontent-%COMP%]   .subtitle[_ngcontent-%COMP%]{margin:6px 0 0;color:var(--app-text-muted);font-size:14px}.card-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(2,1fr);gap:20px}@media(min-width:768px){.card-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(4,1fr)}}.highlight[_ngcontent-%COMP%]{grid-column:1/-1}.hero[_ngcontent-%COMP%]{background:linear-gradient(135deg,var(--app-primary) 0%,var(--app-green) 100%);border-radius:20px;color:#fff}.hero-body[_ngcontent-%COMP%]{display:flex;align-items:center;gap:20px;padding:28px 24px 8px}.hero-icon[_ngcontent-%COMP%]{font-size:48px;width:48px;height:48px;opacity:.9}.hero-title[_ngcontent-%COMP%]{margin:0;font-size:44px;font-weight:700;line-height:1}.hero-label[_ngcontent-%COMP%]{margin:4px 0 0;font-size:15px;opacity:.9}.hero[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{display:flex;gap:32px;flex-wrap:wrap;padding:8px 24px 24px 92px!important}.hero[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;margin:0;font-size:14px;opacity:.95}.stat-card[_ngcontent-%COMP%]{border-radius:18px;background:var(--app-surface);border:1px solid var(--app-border);box-shadow:0 6px 18px #1f6f8b0f;transition:transform .18s ease,box-shadow .18s ease;height:100%}a.stat-card[_ngcontent-%COMP%]{text-decoration:none;color:inherit;cursor:pointer;display:block}.stat-card[_ngcontent-%COMP%]:hover{transform:translateY(-4px);box-shadow:0 12px 28px #1f6f8b1f}.stat-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{display:flex;align-items:center;gap:16px;padding:20px!important}.stat-icon[_ngcontent-%COMP%]{width:52px;height:52px;flex-shrink:0;border-radius:14px;display:flex;align-items:center;justify-content:center;background:var(--app-primary-soft);color:var(--app-primary)}.grid-item[_ngcontent-%COMP%]:nth-child(3n)   .stat-icon[_ngcontent-%COMP%], .grid-item[_ngcontent-%COMP%]:nth-child(4n)   .stat-icon[_ngcontent-%COMP%]{background:var(--app-green-soft);color:var(--app-green)}.stat-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:28px;width:28px;height:28px}.stat-body[_ngcontent-%COMP%]{min-width:0}.stat-value[_ngcontent-%COMP%]{margin:0;font-size:28px;font-weight:700;color:var(--app-text);line-height:1.1}.stat-title[_ngcontent-%COMP%]{margin:2px 0 0;font-size:14px;font-weight:600;color:var(--app-primary-dark)}.stat-desc[_ngcontent-%COMP%]{margin:2px 0 0;font-size:12px;color:var(--app-text-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.cargando[_ngcontent-%COMP%], .error[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:center;gap:12px;padding:48px 0}.cargando[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{color:var(--app-text-muted);font-size:14px}.error[_ngcontent-%COMP%]{color:#c62828}.error[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;font-size:14px}@media(max-width:560px){.hero[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{padding-left:24px!important}}.busqueda-section[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.busqueda-card[_ngcontent-%COMP%], .result-card[_ngcontent-%COMP%]{border-radius:18px;background:var(--app-surface);border:1px solid var(--app-border);box-shadow:0 6px 18px #1f6f8b0f}.busqueda-card[_ngcontent-%COMP%]   mat-card-header[_ngcontent-%COMP%]{padding:20px 20px 0}.busqueda-card[_ngcontent-%COMP%]   mat-card-title[_ngcontent-%COMP%]{font-weight:600}.busqueda-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{padding:16px 20px 20px!important}.busqueda-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px;background:var(--app-primary-soft);color:var(--app-primary)}.busqueda-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}.full-width[_ngcontent-%COMP%]{width:100%}.busqueda-actions[_ngcontent-%COMP%]{display:flex;justify-content:flex-end;margin-top:8px}.resultados[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:center;gap:12px;color:var(--app-text-muted);font-size:14px;padding:8px 0}.resultados.vacio[_ngcontent-%COMP%]{flex-direction:column;padding:24px 0}.result-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{display:flex;align-items:center;gap:16px;padding:16px 20px!important}.result-icon[_ngcontent-%COMP%]{width:44px;height:44px;flex-shrink:0;border-radius:12px;display:flex;align-items:center;justify-content:center;background:var(--app-primary-soft);color:var(--app-primary)}.result-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}.result-body[_ngcontent-%COMP%]{min-width:0}.result-numero[_ngcontent-%COMP%]{margin:0;font-size:12px;font-weight:600;color:var(--app-primary);text-transform:uppercase;letter-spacing:.4px}.result-jugador[_ngcontent-%COMP%]{margin:2px 0 0;font-size:16px;font-weight:600;color:var(--app-text)}.result-meta[_ngcontent-%COMP%]{margin:4px 0 0;display:flex;align-items:center;gap:4px;font-size:13px;color:var(--app-text-muted)}.result-meta[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:16px;width:16px;height:16px}.result-meta[_ngcontent-%COMP%]   .sep[_ngcontent-%COMP%]{margin-left:8px}']})};function S2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre es obligatorio "),u())}function T2(t,n){t&1&&(m(0,"mat-error"),p(1," El nombre no puede superar los 100 caracteres "),u())}function A2(t,n){if(t&1){let e=qe();m(0,"mat-list-item")(1,"mat-icon",15),p(2,"storefront"),u(),m(3,"span",16),p(4),u(),m(5,"button",17),I("click",function(){let r=we(e).$implicit,o=T();return Ee(o.editar(r))}),m(6,"mat-icon"),p(7,"edit"),u()(),m(8,"button",18),I("click",function(){let r=we(e).$implicit,o=T();return Ee(o.eliminar(r))}),m(9,"mat-icon",19),p(10,"delete"),u()()()}if(t&2){let e=n.$implicit,i=T();h(4),z(e.nombre),h(),_("disabled",i.esModoEdicion)}}function k2(t,n){t&1&&(m(0,"mat-list-item")(1,"span",16),p(2,"No hay editoriales registradas."),u()())}var Nf=class t{constructor(n,e){this.fb=n;this.editorialService=e}fb;editorialService;editorialForm;editoriales=[];editandoId=null;cargando=!1;mensaje="";error="";ngOnInit(){this.crearFormulario(),this.cargarEditoriales()}crearFormulario(){this.editorialForm=this.fb.group({nombre:["",[le.required,le.maxLength(100)]]})}cargarEditoriales(){this.editorialService.getEditoriales().subscribe({next:n=>{this.editoriales=n},error:()=>{this.error="No se han podido cargar las editoriales."}})}get esModoEdicion(){return this.editandoId!==null}editar(n){this.editandoId=n.id,this.editorialForm.patchValue({nombre:n.nombre}),this.mensaje="",this.error=""}guardar(){if(this.editorialForm.invalid){this.editorialForm.markAllAsTouched();return}this.cargando=!0,this.mensaje="",this.error="";let n=this.editorialForm.get("nombre")?.value;this.esModoEdicion?this.editorialService.editarEditorial(this.editandoId,{nombre:n}).subscribe({next:e=>{this.cargando=!1;let i=this.editoriales.findIndex(r=>r.id===this.editandoId);i!==-1&&(this.editoriales[i]=e),this.mensaje="Editorial actualizada correctamente.",this.cancelar()},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar la editorial."}}):this.editorialService.crearEditorial({nombre:n}).subscribe({next:e=>{this.cargando=!1,this.editoriales.push(e),this.mensaje="Editorial a\xF1adida correctamente.",this.editorialForm.reset()},error:()=>{this.cargando=!1,this.error="No se ha podido a\xF1adir la editorial."}})}eliminar(n){this.editorialService.eliminarEditorial(n.id).subscribe({next:()=>{this.editoriales=this.editoriales.filter(e=>e.id!==n.id),this.mensaje="Editorial eliminada correctamente.",this.error=""},error:()=>{this.mensaje="",this.error="No se ha podido eliminar la editorial."}})}cancelar(){this.editandoId=null,this.editorialForm.reset(),this.mensaje="",this.error=""}static \u0275fac=function(e){return new(e||t)(D(xn),D(Ts))};static \u0275cmp=A({type:t,selectors:[["app-editoriales"]],decls:43,vars:16,consts:[[1,"page-container"],[1,"content-wrapper"],[1,"form-card"],["mat-card-avatar","",1,"header-icon"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"full-width"],["matInput","","type","text","formControlName","nombre","placeholder","Ej. Panini, Topps..."],["matSuffix",""],[4,"ngIf"],[1,"mensaje"],["align","end"],["mat-button","","type","button",3,"click"],["mat-flat-button","","color","primary","type","submit",3,"disabled"],[1,"list-card"],[4,"ngFor","ngForOf"],["matListItemIcon",""],["matListItemTitle",""],["mat-icon-button","","matListItemMeta","",3,"click","disabled"],["mat-icon-button","","matListItemMeta","",3,"click"],["color","warn"]],template:function(e,i){if(e&1&&(m(0,"div",0)(1,"div",1)(2,"mat-card",2)(3,"mat-card-header")(4,"div",3)(5,"mat-icon"),p(6),u()(),m(7,"mat-card-title"),p(8),u(),m(9,"mat-card-subtitle"),p(10),u()(),m(11,"mat-card-content")(12,"form",4),I("ngSubmit",function(){return i.guardar()}),m(13,"mat-form-field",5)(14,"mat-label"),p(15,"Nombre"),u(),re(16,"input",6),m(17,"mat-icon",7),p(18,"storefront"),u(),M(19,S2,2,0,"mat-error",8)(20,T2,2,0,"mat-error",8),u(),m(21,"div",9),p(22),u(),m(23,"mat-card-actions",10)(24,"button",11),I("click",function(){return i.cancelar()}),p(25),u(),m(26,"button",12)(27,"mat-icon"),p(28),u(),p(29),u()()()()(),m(30,"mat-card",13)(31,"mat-card-header")(32,"div",3)(33,"mat-icon"),p(34,"list"),u()(),m(35,"mat-card-title"),p(36,"Editoriales registradas"),u(),m(37,"mat-card-subtitle"),p(38," Editoriales existentes en la base de datos "),u()(),m(39,"mat-card-content")(40,"mat-list"),M(41,A2,11,2,"mat-list-item",14)(42,k2,3,0,"mat-list-item",8),u()()()()()),e&2){let r,o;h(6),z(i.esModoEdicion?"edit":"storefront"),h(2),z(i.esModoEdicion?"Editar editorial":"A\xF1adir nueva editorial"),h(2),K(" ",i.esModoEdicion?"Modifica el nombre de la editorial":"Completa los datos de la editorial que quieres a\xF1adir"," "),h(2),_("formGroup",i.editorialForm),h(7),_("ngIf",(r=i.editorialForm.get("nombre"))==null?null:r.hasError("required")),h(),_("ngIf",(o=i.editorialForm.get("nombre"))==null?null:o.hasError("maxlength")),h(),R("error",!!i.error),h(),ct(" ",i.mensaje,"",i.error," "),h(3),K(" ",i.esModoEdicion?"Cancelar":"Limpiar"," "),h(),_("disabled",i.cargando),h(2),z(i.esModoEdicion?"save":"add"),h(),K(" ",i.cargando?"Guardando...":i.esModoEdicion?"Actualizar":"Guardar editorial"," "),h(12),_("ngForOf",i.editoriales),h(),_("ngIf",i.editoriales.length===0)}},dependencies:[ut,dt,Ct,Zt,Cn,Ft,Yt,yn,xt,un,jt,en,Fn,Pt,pt,tn,Zn,Mn,nn,In,Dn,En,Xe,Dt,ft,Kt,Xt,gt,ht,Lt,Jt,Vt,Vi,Qn,Ln,Pn,wi,Bt,Ht,Ut],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:var(--app-bg)}.content-wrapper[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;display:flex;flex-direction:column;gap:24px}mat-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden;border:1px solid var(--app-border)}mat-card-header[_ngcontent-%COMP%]{padding:24px 24px 16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px!important;font-weight:600}mat-card-subtitle[_ngcontent-%COMP%]{margin-top:6px}.header-icon[_ngcontent-%COMP%]{width:42px;height:42px;display:flex;align-items:center;justify-content:center;border-radius:12px;background:var(--app-primary-soft);color:var(--app-primary)}.header-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px;width:24px;height:24px}mat-card-content[_ngcontent-%COMP%]{padding:24px!important}form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.full-width[_ngcontent-%COMP%]{width:100%}mat-form-field[_ngcontent-%COMP%]{width:100%}.mensaje[_ngcontent-%COMP%]{min-height:20px;color:var(--app-green);font-weight:500;text-align:center}.mensaje.error[_ngcontent-%COMP%]{color:#c62828}mat-card-actions[_ngcontent-%COMP%]{padding:0;gap:8px}mat-card-actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-height:42px}mat-card-actions[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{margin-right:6px}mat-list[_ngcontent-%COMP%]   mat-list-item[_ngcontent-%COMP%]{margin-bottom:4px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}mat-card-title[_ngcontent-%COMP%]{font-size:18px!important}}"]})};var V_=new y("CdkAccordion"),nI=(()=>{class t{_stateChanges=new E;_openCloseAllActions=new E;id=d(Be).getId("cdk-accordion-");multi=!1;openAll(){this.multi&&this._openCloseAllActions.next(!0)}closeAll(){this._openCloseAllActions.next(!1)}ngOnChanges(e){this._stateChanges.next(e)}ngOnDestroy(){this._stateChanges.complete(),this._openCloseAllActions.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["cdk-accordion"],["","cdkAccordion",""]],inputs:{multi:[2,"multi","multi",J]},exportAs:["cdkAccordion"],features:[xe([{provide:V_,useExisting:t}]),je]})}return t})(),iI=(()=>{class t{accordion=d(V_,{optional:!0,skipSelf:!0});_changeDetectorRef=d(ge);_expansionDispatcher=d(jc);_openCloseAllSubscription=fe.EMPTY;closed=new P;opened=new P;destroyed=new P;expandedChange=new P;id=d(Be).getId("cdk-accordion-child-");get expanded(){return this._expanded}set expanded(e){if(this._expanded!==e){if(this._expanded=e,this.expandedChange.emit(e),e){this.opened.emit();let i=this.accordion?this.accordion.id:this.id;this._expansionDispatcher.notify(this.id,i)}else this.closed.emit();this._changeDetectorRef.markForCheck()}}_expanded=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=ne(!1);_removeUniqueSelectionListener=()=>{};constructor(){}ngOnInit(){this._removeUniqueSelectionListener=this._expansionDispatcher.listen((e,i)=>{this.accordion&&!this.accordion.multi&&this.accordion.id===i&&this.id!==e&&(this.expanded=!1)}),this.accordion&&(this._openCloseAllSubscription=this._subscribeToOpenCloseAllActions())}ngOnDestroy(){this.opened.complete(),this.closed.complete(),this.destroyed.emit(),this.destroyed.complete(),this._removeUniqueSelectionListener(),this._openCloseAllSubscription.unsubscribe()}toggle(){this.disabled||(this.expanded=!this.expanded)}close(){this.disabled||(this.expanded=!1)}open(){this.disabled||(this.expanded=!0)}_subscribeToOpenCloseAllActions(){return this.accordion._openCloseAllActions.subscribe(e=>{this.disabled||(this.expanded=e)})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["cdk-accordion-item"],["","cdkAccordionItem",""]],inputs:{expanded:[2,"expanded","expanded",J],disabled:[2,"disabled","disabled",J]},outputs:{closed:"closed",opened:"opened",destroyed:"destroyed",expandedChange:"expandedChange"},exportAs:["cdkAccordionItem"],features:[xe([{provide:V_,useValue:void 0}])]})}return t})(),rI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({})}return t})();var O2=["body"],R2=["bodyWrapper"],N2=[[["mat-expansion-panel-header"]],"*",[["mat-action-row"]]],F2=["mat-expansion-panel-header","*","mat-action-row"];function P2(t,n){}var L2=[[["mat-panel-title"]],[["mat-panel-description"]],"*"],j2=["mat-panel-title","mat-panel-description","*"];function V2(t,n){t&1&&(lt(0,"span",1),Ai(),lt(1,"svg",2),_n(2,"path",3),Rt()())}var B_=new y("MAT_ACCORDION"),oI=new y("MAT_EXPANSION_PANEL"),B2=(()=>{class t{_template=d(kt);_expansionPanel=d(oI,{optional:!0});constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["ng-template","matExpansionPanelContent",""]]})}return t})(),aI=new y("MAT_EXPANSION_PANEL_DEFAULT_OPTIONS"),H_=(()=>{class t extends iI{_viewContainerRef=d(nt);_animationsDisabled=He();_document=d(G);_ngZone=d(F);_elementRef=d(N);_renderer=d(Se);_cleanupTransitionEnd;get hideToggle(){return this._hideToggle||this.accordion&&this.accordion.hideToggle}set hideToggle(e){this._hideToggle=e}_hideToggle=!1;get togglePosition(){return this._togglePosition||this.accordion&&this.accordion.togglePosition}set togglePosition(e){this._togglePosition=e}_togglePosition;afterExpand=new P;afterCollapse=new P;_inputChanges=new E;accordion=d(B_,{optional:!0,skipSelf:!0});_lazyContent;_body;_bodyWrapper;_portal;_headerId=d(Be).getId("mat-expansion-panel-header-");constructor(){super();let e=d(aI,{optional:!0});this._expansionDispatcher=d(jc),e&&(this.hideToggle=e.hideToggle)}_hasSpacing(){return this.accordion?this.expanded&&this.accordion.displayMode==="default":!1}_getExpandedState(){return this.expanded?"expanded":"collapsed"}toggle(){this.expanded=!this.expanded}close(){this.expanded=!1}open(){this.expanded=!0}ngAfterContentInit(){this._lazyContent&&this._lazyContent._expansionPanel===this&&this.opened.pipe(Me(null),ae(()=>this.expanded&&!this._portal),Oe(1)).subscribe(()=>{this._portal=new Ci(this._lazyContent._template,this._viewContainerRef)}),this._setupAnimationEvents()}ngOnChanges(e){this._inputChanges.next(e)}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransitionEnd?.(),this._inputChanges.complete()}_containsFocus(){if(this._body){let e=this._document.activeElement,i=this._body.nativeElement;return e===i||i.contains(e)}return!1}_transitionEndListener=({target:e,propertyName:i})=>{e===this._bodyWrapper?.nativeElement&&i==="grid-template-rows"&&this._ngZone.run(()=>{this.expanded?this.afterExpand.emit():this.afterCollapse.emit()})};_setupAnimationEvents(){this._ngZone.runOutsideAngular(()=>{this._animationsDisabled?(this.opened.subscribe(()=>this._ngZone.run(()=>this.afterExpand.emit())),this.closed.subscribe(()=>this._ngZone.run(()=>this.afterCollapse.emit()))):setTimeout(()=>{let e=this._elementRef.nativeElement;this._cleanupTransitionEnd=this._renderer.listen(e,"transitionend",this._transitionEndListener),e.classList.add("mat-expansion-panel-animations-enabled")},200)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-expansion-panel"]],contentQueries:function(i,r,o){if(i&1&&it(o,B2,5),i&2){let a;$(a=q())&&(r._lazyContent=a.first)}},viewQuery:function(i,r){if(i&1&&Ge(O2,5)(R2,5),i&2){let o;$(o=q())&&(r._body=o.first),$(o=q())&&(r._bodyWrapper=o.first)}},hostAttrs:[1,"mat-expansion-panel"],hostVars:4,hostBindings:function(i,r){i&2&&R("mat-expanded",r.expanded)("mat-expansion-panel-spacing",r._hasSpacing())},inputs:{hideToggle:[2,"hideToggle","hideToggle",J],togglePosition:"togglePosition"},outputs:{afterExpand:"afterExpand",afterCollapse:"afterCollapse"},exportAs:["matExpansionPanel"],features:[xe([{provide:B_,useValue:void 0},{provide:oI,useExisting:t}]),be,je],ngContentSelectors:F2,decls:9,vars:4,consts:[["bodyWrapper",""],["body",""],[1,"mat-expansion-panel-content-wrapper"],["role","region",1,"mat-expansion-panel-content",3,"id"],[1,"mat-expansion-panel-body"],[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(Ce(N2),H(0),m(1,"div",2,0)(3,"div",3,1)(5,"div",4),H(6,1),M(7,P2,0,0,"ng-template",5),u(),H(8,2),u()()),i&2&&(h(),ce("inert",r.expanded?null:""),h(2),_("id",r.id),ce("aria-labelledby",r._headerId),h(4),_("cdkPortalOutlet",r._portal))},dependencies:[ra],styles:[`.mat-expansion-panel {
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
`],encapsulation:2,changeDetection:0})}return t})();var U_=(()=>{class t{panel=d(H_,{host:!0});_element=d(N);_focusMonitor=d(bi);_changeDetectorRef=d(ge);_parentChangeSubscription=fe.EMPTY;constructor(){d(mt).load(io);let e=this.panel,i=d(aI,{optional:!0}),r=d(new Ni("tabindex"),{optional:!0}),o=e.accordion?e.accordion._stateChanges.pipe(ae(a=>!!(a.hideToggle||a.togglePosition))):Ye;this.tabIndex=parseInt(r||"")||0,this._parentChangeSubscription=_t(e.opened,e.closed,o,e._inputChanges.pipe(ae(a=>!!(a.hideToggle||a.disabled||a.togglePosition)))).subscribe(()=>this._changeDetectorRef.markForCheck()),e.closed.pipe(ae(()=>e._containsFocus())).subscribe(()=>this._focusMonitor.focusVia(this._element,"program")),i&&(this.expandedHeight=i.expandedHeight,this.collapsedHeight=i.collapsedHeight)}expandedHeight;collapsedHeight;tabIndex=0;get disabled(){return this.panel.disabled}_toggle(){this.disabled||this.panel.toggle()}_isExpanded(){return this.panel.expanded}_getExpandedState(){return this.panel._getExpandedState()}_getPanelId(){return this.panel.id}_getTogglePosition(){return this.panel.togglePosition}_showToggle(){return!this.panel.hideToggle&&!this.panel.disabled}_getHeaderHeight(){let e=this._isExpanded();return e&&this.expandedHeight?this.expandedHeight:!e&&this.collapsedHeight?this.collapsedHeight:null}_keydown(e){switch(e.keyCode){case 32:case 13:We(e)||(e.preventDefault(),this._toggle());break;default:this.panel.accordion&&this.panel.accordion._handleHeaderKeydown(e);return}}focus(e,i){e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}ngAfterViewInit(){this._focusMonitor.monitor(this._element).subscribe(e=>{e&&this.panel.accordion&&this.panel.accordion._handleHeaderFocus(this)})}ngOnDestroy(){this._parentChangeSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._element)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["mat-expansion-panel-header"]],hostAttrs:["role","button",1,"mat-expansion-panel-header","mat-focus-indicator"],hostVars:13,hostBindings:function(i,r){i&1&&I("click",function(){return r._toggle()})("keydown",function(a){return r._keydown(a)}),i&2&&(ce("id",r.panel._headerId)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r._getPanelId())("aria-expanded",r._isExpanded())("aria-disabled",r.panel.disabled),On("height",r._getHeaderHeight()),R("mat-expanded",r._isExpanded())("mat-expansion-toggle-indicator-after",r._getTogglePosition()==="after")("mat-expansion-toggle-indicator-before",r._getTogglePosition()==="before"))},inputs:{expandedHeight:"expandedHeight",collapsedHeight:"collapsedHeight",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:pi(e)]},ngContentSelectors:j2,decls:5,vars:3,consts:[[1,"mat-content"],[1,"mat-expansion-indicator"],["xmlns","http://www.w3.org/2000/svg","viewBox","0 -960 960 960","aria-hidden","true","focusable","false"],["d","M480-345 240-585l56-56 184 184 184-184 56 56-240 240Z"]],template:function(i,r){i&1&&(Ce(L2),lt(0,"span",0),H(1),H(2,1),H(3,2),Rt(),Fe(4,V2,3,0,"span",1)),i&2&&(R("mat-content-hide-toggle",!r._showToggle()),h(4),Pe(r._showToggle()?4:-1))},styles:[`.mat-expansion-panel-header {
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
`],encapsulation:2,changeDetection:0})}return t})(),sI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-panel-description"]],hostAttrs:[1,"mat-expansion-panel-header-description"]})}return t})(),lI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-panel-title"]],hostAttrs:[1,"mat-expansion-panel-header-title"]})}return t})(),cI=(()=>{class t extends nI{_keyManager;_ownHeaders=new li;_headers;hideToggle=!1;displayMode="default";togglePosition="after";ngAfterContentInit(){this._headers.changes.pipe(Me(this._headers)).subscribe(e=>{this._ownHeaders.reset(e.filter(i=>i.panel.accordion===this)),this._ownHeaders.notifyOnChanges()}),this._keyManager=new Ic(this._ownHeaders).withWrap().withHomeAndEnd()}_handleHeaderKeydown(e){this._keyManager.onKeydown(e)}_handleHeaderFocus(e){this._keyManager.updateActiveItem(e)}ngOnDestroy(){super.ngOnDestroy(),this._keyManager?.destroy(),this._ownHeaders.destroy()}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["mat-accordion"]],contentQueries:function(i,r,o){if(i&1&&it(o,U_,5),i&2){let a;$(a=q())&&(r._headers=a)}},hostAttrs:[1,"mat-accordion"],hostVars:2,hostBindings:function(i,r){i&2&&R("mat-accordion-multi",r.multi)},inputs:{hideToggle:[2,"hideToggle","hideToggle",J],displayMode:"displayMode",togglePosition:"togglePosition"},exportAs:["matAccordion"],features:[xe([{provide:B_,useExisting:t}]),be]})}return t})(),dI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({imports:[rI,mr,_e]})}return t})();function U2(t,n){}var co=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var $_=(()=>{class t extends Cs{_elementRef=d(N);_focusTrapFactory=d(wc);_config;_interactivityChecker=d(ps);_ngZone=d(F);_focusMonitor=d(bi);_renderer=d(Se);_changeDetectorRef=d(ge);_injector=d(W);_platform=d(ve);_document=d(G);_portalOutlet;_focusTrapped=new E;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(co,{optional:!0})||new co,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||st(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=eo(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=eo();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=eo()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=A({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&Ge(ra,7),i&2){let o;$(o=q())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&ce("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[be],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&M(0,U2,0,0,"ng-template",0)},dependencies:[ra],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),Hc=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new E;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!We(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},z2=new y("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>Es(t)}}),$2=new y("DialogData"),q2=new y("DefaultDialogConfig");function G2(t){let n=ne(t),e=new P;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var q_=(()=>{class t{_injector=d(W);_defaultOptions=d(q2,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(mf);_idGenerator=d(Be);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new E;_afterOpenedAtThisLevel=new E;_ariaHiddenElements=new Map;_scrollStrategy=d(z2);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=Tn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Me(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new co;i=C(C({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),a=hr(this._injector,o),s=new Hc(a,i),l=this._attachContainer(a,s,i);if(s.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(Oe(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,s,l,i),this.openDialogs.push(s),s.closed.subscribe(()=>this._removeOpenDialog(s,!0)),this.afterOpened.next(s),s}closeAll(){z_(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){z_(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),z_(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new fr({positionStrategy:e.positionStrategy||Ds().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,a=[{provide:co,useValue:r},{provide:Hc,useValue:i},{provide:xs,useValue:e}],s;r.container?typeof r.container=="function"?s=r.container:(s=r.container.type,a.push(...r.container.providers(r))):s=$_;let l=new oo(s,r.viewContainerRef,W.create({parent:o||this._injector,providers:a}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof kt){let a=this._createInjector(o,i,r,void 0),s={$implicit:o.data,dialogRef:i};o.templateContext&&(s=C(C({},s),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Ci(e,null,s,a))}else{let a=this._createInjector(o,i,r,this._injector),s=r.attachComponentPortal(new oo(e,o.viewContainerRef,a));i.componentRef=s,i.componentInstance=s.instance}}_createInjector(e,i,r,o){let a=e.injector||e.viewContainerRef?.injector,s=[{provide:$2,useValue:e.data},{provide:Hc,useValue:i}];return e.providers&&(typeof e.providers=="function"?s.push(...e.providers(i,e,r)):s.push(...e.providers)),e.direction&&(!a||!a.get(Et,null,{optional:!0}))&&s.push({provide:Et,useValue:G2(e.direction)}),W.create({parent:a||o,providers:s})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,a)=>{o?a.setAttribute("aria-hidden",o):a.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function z_(t,n){let e=t.length;for(;e--;)n(t[e])}var uI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({providers:[q_],imports:[xi,mr,Ec,mr]})}return t})();function W2(t,n){}var Pf=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},G_="mdc-dialog--open",mI="mdc-dialog--opening",fI="mdc-dialog--closing",Y2=150,Z2=75,Q2=(()=>{class t extends $_{_animationStateChanged=new P;_animationsEnabled=!He();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?hI(this._config.enterAnimationDuration)??Y2:0;_exitAnimationDuration=this._animationsEnabled?hI(this._config.exitAnimationDuration)??Z2:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(pI,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(mI,G_)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(G_),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(G_),this._animationsEnabled?(this._hostElement.style.setProperty(pI,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(fI)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(mI,fI)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275cmp=A({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(bn("id",r._config.id),ce("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),R("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[be],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(m(0,"div",0)(1,"div",1),M(2,W2,0,0,"ng-template",2),u()())},dependencies:[ra],styles:[`.mat-mdc-dialog-container {
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
`],encapsulation:2})}return t})(),pI="--mat-dialog-transition-duration";function hI(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?Qt(t.substring(0,t.length-2)):t.endsWith("s")?Qt(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var Ff=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(Ff||{}),Os=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new Tr(1);_beforeClosed=new Tr(1);_result;_closeFallbackTimeout;_state=Ff.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(ae(r=>r.state==="opened"),Oe(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(ae(r=>r.state==="closed"),Oe(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),_t(this.backdropClick(),this.keydownEvents().pipe(ae(r=>r.keyCode===27&&!this.disableClose&&!We(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),K2(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(ae(i=>i.state==="closing"),Oe(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=Ff.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=Ff.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function K2(t,n,e){return t._closeInteractionType=n,t.close(e)}var W_=new y("MatMdcDialogData"),X2=new y("mat-mdc-dialog-default-options"),J2=new y("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>Es(t)}}),Lf=(()=>{class t{_defaultOptions=d(X2,{optional:!0});_scrollStrategy=d(J2);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Be);_injector=d(W);_dialog=d(q_);_animationsDisabled=He();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new E;_afterOpenedAtThisLevel=new E;dialogConfigClass=Pf;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=Tn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Me(void 0)));constructor(){this._dialogRefConstructor=Os,this._dialogContainerType=Q2,this._dialogDataToken=W_}open(e,i){let r;i=C(C({},this._defaultOptions||new Pf),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,ee(C({},i),{positionStrategy:Ds(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:co,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(a,s,l)=>(r=new this._dialogRefConstructor(a,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:s.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let a=this.openDialogs.indexOf(r);a>-1&&(this.openDialogs.splice(a,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var gI=(()=>{class t{_dialogRef=d(Os,{optional:!0});_elementRef=d(N);_dialog=d(Lf);constructor(){}ngOnInit(){this._dialogRef||(this._dialogRef=ej(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t})}return t})(),vI=(()=>{class t extends gI{id=d(Be).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&bn("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[be]})}return t})(),_I=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[Bg([ur])]})}return t})(),bI=(()=>{class t extends gI{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Ve(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&R("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[be]})}return t})();function ej(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var jf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=L({providers:[Lf],imports:[uI,xi,mr,_e]})}return t})();var Vf=class t{http=d(Je);apiUrl="/api/Ediciones";getEdiciones(){return this.http.get(this.apiUrl)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=x({token:t,factory:t.\u0275fac,providedIn:"root"})};function nj(t,n){if(t&1&&(m(0,"div",20)(1,"mat-icon"),p(2,"error_outline"),u(),m(3,"p"),p(4),u()()),t&2){let e=T();h(4),z(e.error)}}function ij(t,n){t&1&&(m(0,"mat-error"),p(1," El n\xFAmero es obligatorio. "),u())}function rj(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;_("value",e),h(),K(" ",e.nombre," ")}}function oj(t,n){t&1&&(m(0,"mat-error"),p(1," El equipo es obligatorio. "),u())}function aj(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;_("value",e),h(),K(" ",e.nombreCompleto||e.nombre," ")}}function sj(t,n){t&1&&(m(0,"mat-error"),p(1," El jugador es obligatorio. "),u())}function lj(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;_("value",e.id),h(),K(" ",e.nombre," ")}}function cj(t,n){t&1&&(m(0,"mat-error"),p(1," La edici\xF3n es obligatoria. "),u())}function dj(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;_("value",e),h(),K(" ",e.nombre," ")}}function uj(t,n){t&1&&(m(0,"mat-error"),p(1," El tipo de cromo es obligatorio. "),u())}function mj(t,n){if(t&1&&(m(0,"mat-option",21),p(1),u()),t&2){let e=n.$implicit;_("value",e.id),h(),K(" ",e.nombre," ")}}function fj(t,n){t&1&&(m(0,"mat-error"),p(1," El \xE1lbum es obligatorio. "),u())}function pj(t,n){t&1&&re(0,"mat-spinner",22)}var Bf=class t{constructor(n,e,i,r,o,a,s,l,c){this.data=n;this.dialogRef=e;this.fb=i;this.http=r;this.equipoService=o;this.edicionService=a;this.tipoCromoService=s;this.albumService=l;this.cdr=c}data;dialogRef;fb;http;equipoService;edicionService;tipoCromoService;albumService;cdr;cromoForm;equipos=[];ediciones=[];tiposCromo=[];albumes=[];jugadores=[];filteredEquipos$;filteredJugadores$;filteredTiposCromo$;cargando=!1;error="";apiUrl="/api";ngOnInit(){this.crearFormulario(),this.cargarDatosMaestros()}crearFormulario(){let n=this.data.cromo;this.cromoForm=this.fb.group({numero:[n.numero,le.required],equipo:[C({},n.equipo),le.required],jugador:[{value:C({},n.jugador),disabled:!1},le.required],edicionId:[n.edicion.id,le.required],tipoCromo:[C({},n.tipoCromo),le.required],albumId:[this.data.albumId,le.required]})}cargarDatosMaestros(){this.equipoService.getEquipos().subscribe({next:n=>{this.equipos=n,this.filteredEquipos$=this.cromoForm.get("equipo").valueChanges.pipe(Me(""),V(e=>this.filtrarEquipos(e))),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los equipos."}}),this.edicionService.getEdiciones().subscribe({next:n=>{this.ediciones=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar las ediciones."}}),this.tipoCromoService.getTiposCromo().subscribe({next:n=>{this.tiposCromo=n,this.filteredTiposCromo$=this.cromoForm.get("tipoCromo").valueChanges.pipe(Me(""),V(e=>this.filtrarTiposCromo(e))),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los tipos de cromo."}}),this.albumService.getAlbumes().subscribe({next:n=>{this.albumes=n,this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los \xE1lbumes."}}),this.cargarJugadoresDelEquipo(this.data.cromo.equipo.id)}filtrarEquipos(n){let e=(typeof n=="string"?n:n?.nombre??"").toLowerCase();return this.equipos.filter(i=>i.nombre.toLowerCase().includes(e))}onEquipoInput(n){this.cromoForm.get("equipo")?.setValue(n)}onEquipoSelected(n){this.cromoForm.get("equipo")?.setValue(n),this.onEquipoChange()}displayEquipo(n){return n?.nombre??""}onEquipoChange(){let e=this.cromoForm.get("equipo")?.value?.id;e&&this.cargarJugadoresDelEquipo(e)}cargarJugadoresDelEquipo(n){this.jugadores=[],this.cromoForm.get("jugador")?.setValue(null),this.cromoForm.get("jugador")?.disable(),this.http.get(`${this.apiUrl}/Equipos/${n}/Jugadores`).subscribe({next:e=>{this.jugadores=e,this.filteredJugadores$=this.cromoForm.get("jugador").valueChanges.pipe(Me(""),V(i=>this.filtrarJugadores(i))),this.cromoForm.get("jugador")?.enable(),this.cdr.detectChanges()},error:()=>{this.error="No se han podido cargar los jugadores."}})}filtrarJugadores(n){let e=(typeof n=="string"?n:"").toLowerCase();return this.jugadores.filter(i=>(i.nombreCompleto||i.nombre).toLowerCase().includes(e))}onJugadorInput(n){this.cromoForm.get("jugador")?.setValue(n)}onJugadorSelected(n){this.cromoForm.get("jugador")?.setValue(n)}displayJugador(n){return n?typeof n=="string"?n:n.nombreCompleto||n.nombre:""}filtrarTiposCromo(n){let e=(typeof n=="string"?n:n?.nombre??"").toLowerCase();return this.tiposCromo.filter(i=>i.nombre.toLowerCase().includes(e))}onTipoCromoInput(n){this.cromoForm.get("tipoCromo")?.setValue(n)}onTipoCromoSelected(n){this.cromoForm.get("tipoCromo")?.setValue(n)}displayTipoCromo(n){return n?.nombre??""}guardar(){if(this.cromoForm.invalid){this.cromoForm.markAllAsTouched();return}this.cargando=!0,this.error="";let n={numero:this.cromoForm.get("numero")?.value,edicionId:this.cromoForm.get("edicionId")?.value,albumId:this.cromoForm.get("albumId")?.value,equipoId:this.cromoForm.get("equipo")?.value?.id,jugadorId:this.cromoForm.get("jugador")?.value?.id,tipoCromoId:this.cromoForm.get("tipoCromo")?.value?.id,publicacionId:null,coleccionId:null};this.http.put(`${this.apiUrl}/Cromo/${this.data.cromo.id}`,n).subscribe({next:()=>{this.cargando=!1,this.dialogRef.close(!0)},error:()=>{this.cargando=!1,this.error="No se ha podido actualizar el cromo.",this.cdr.detectChanges()}})}cancelar(){this.dialogRef.close(!1)}static \u0275fac=function(e){return new(e||t)(D(W_),D(Os),D(xn),D(Je),D(Bi),D(Vf),D(so),D(lo),D(ge))};static \u0275cmp=A({type:t,selectors:[["app-cromo-editar"]],decls:55,vars:24,consts:[["autoEquipo","matAutocomplete"],["autoJugador","matAutocomplete"],["autoTipo","matAutocomplete"],["mat-dialog-title",""],["class","error",4,"ngIf"],[1,"editar-form",3,"formGroup"],["appearance","outline"],["matInput","","formControlName","numero"],[4,"ngIf"],["type","text","matInput","","formControlName","equipo","placeholder","Buscar equipo...",3,"input","matAutocomplete"],[3,"optionSelected"],[3,"value",4,"ngFor","ngForOf"],["type","text","matInput","","formControlName","jugador","placeholder","Buscar jugador...",3,"input","matAutocomplete"],["formControlName","edicionId"],["type","text","matInput","","formControlName","tipoCromo","placeholder","Buscar tipo...",3,"input","matAutocomplete"],["formControlName","albumId"],["align","end"],["mat-button","",3,"click"],["mat-raised-button","","color","primary",3,"click","disabled"],["diameter","18","style","display:inline-block; margin-right:8px",4,"ngIf"],[1,"error"],[3,"value"],["diameter","18",2,"display","inline-block","margin-right","8px"]],template:function(e,i){if(e&1&&(m(0,"h2",3),p(1,"Editar cromo"),u(),m(2,"mat-dialog-content"),M(3,nj,5,1,"div",4),m(4,"form",5)(5,"mat-form-field",6)(6,"mat-label"),p(7,"N\xFAmero"),u(),re(8,"input",7),M(9,ij,2,0,"mat-error",8),u(),m(10,"mat-form-field",6)(11,"mat-label"),p(12,"Equipo"),u(),m(13,"input",9),I("input",function(o){return i.onEquipoInput(o.target.value)}),u(),m(14,"mat-autocomplete",10,0),I("optionSelected",function(o){return i.onEquipoSelected(o.option.value)}),M(16,rj,2,2,"mat-option",11),rr(17,"async"),u(),M(18,oj,2,0,"mat-error",8),u(),m(19,"mat-form-field",6)(20,"mat-label"),p(21,"Jugador"),u(),m(22,"input",12),I("input",function(o){return i.onJugadorInput(o.target.value)}),u(),m(23,"mat-autocomplete",10,1),I("optionSelected",function(o){return i.onJugadorSelected(o.option.value)}),M(25,aj,2,2,"mat-option",11),rr(26,"async"),u(),M(27,sj,2,0,"mat-error",8),u(),m(28,"mat-form-field",6)(29,"mat-label"),p(30,"Edici\xF3n"),u(),m(31,"mat-select",13),M(32,lj,2,2,"mat-option",11),u(),M(33,cj,2,0,"mat-error",8),u(),m(34,"mat-form-field",6)(35,"mat-label"),p(36,"Tipo de cromo"),u(),m(37,"input",14),I("input",function(o){return i.onTipoCromoInput(o.target.value)}),u(),m(38,"mat-autocomplete",10,2),I("optionSelected",function(o){return i.onTipoCromoSelected(o.option.value)}),M(40,dj,2,2,"mat-option",11),rr(41,"async"),u(),M(42,uj,2,0,"mat-error",8),u(),m(43,"mat-form-field",6)(44,"mat-label"),p(45,"\xC1lbum"),u(),m(46,"mat-select",15),M(47,mj,2,2,"mat-option",11),u(),M(48,fj,2,0,"mat-error",8),u()()(),m(49,"mat-dialog-actions",16)(50,"button",17),I("click",function(){return i.cancelar()}),p(51,"Cancelar"),u(),m(52,"button",18),I("click",function(){return i.guardar()}),M(53,pj,1,0,"mat-spinner",19),p(54," Guardar "),u()()),e&2){let r,o,a,s,l,c,f=yt(15),g=yt(24),v=yt(39);h(3),_("ngIf",i.error),h(),_("formGroup",i.cromoForm),h(5),_("ngIf",(r=i.cromoForm.get("numero"))==null?null:r.hasError("required")),h(4),_("matAutocomplete",f),h(3),_("ngForOf",or(17,18,i.filteredEquipos$)),h(2),_("ngIf",(o=i.cromoForm.get("equipo"))==null?null:o.hasError("required")),h(4),_("matAutocomplete",g),h(3),_("ngForOf",or(26,20,i.filteredJugadores$)),h(2),_("ngIf",(a=i.cromoForm.get("jugador"))==null?null:a.hasError("required")),h(5),_("ngForOf",i.ediciones),h(),_("ngIf",(s=i.cromoForm.get("edicionId"))==null?null:s.hasError("required")),h(4),_("matAutocomplete",v),h(3),_("ngForOf",or(41,22,i.filteredTiposCromo$)),h(2),_("ngIf",(l=i.cromoForm.get("tipoCromo"))==null?null:l.hasError("required")),h(5),_("ngForOf",i.albumes),h(),_("ngIf",(c=i.cromoForm.get("albumId"))==null?null:c.hasError("required")),h(4),_("disabled",i.cargando),h(),_("ngIf",i.cargando)}},dependencies:[ut,dt,Ct,Zt,Cn,Ft,Yt,yn,xt,un,jf,vI,bI,_I,Pt,vf,wn,Pc,Lt,Jt,Dt,ft,Kt,ks,As,jt,en,pt,Xe,gt,ht,Vt,Bt,ao,Ht,Ut,jl],styles:[".error[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;color:#f44336;margin-bottom:12px}.error[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0}"]})};function hj(t,n){t&1&&(m(0,"div",4),re(1,"mat-spinner",5),m(2,"p"),p(3,"Cargando colecci\xF3n..."),u()())}function gj(t,n){if(t&1&&(m(0,"div",6)(1,"mat-icon"),p(2,"error_outline"),u(),m(3,"p"),p(4),u(),m(5,"a",7)(6,"mat-icon"),p(7,"arrow_back"),u(),p(8," Volver a \xE1lbumes "),u()()),t&2){let e=T();h(4),z(e.error)}}function vj(t,n){if(t&1&&(m(0,"p",25),p(1),u()),t&2){let e=T().$implicit;h(),z(e.edicion.nombre)}}function _j(t,n){if(t&1){let e=qe();m(0,"mat-card",19)(1,"mat-card-content")(2,"span",20),p(3),u(),m(4,"p",21),p(5),u(),m(6,"p",22),p(7),u(),M(8,vj,2,1,"p",23),u(),m(9,"button",24),I("click",function(){let r=we(e).$implicit,o=T(4);return Ee(o.editarCromo(r))}),m(10,"mat-icon"),p(11,"edit"),u()()()}if(t&2){let e=n.$implicit,i=T(4);h(3),z(e.numero),h(2),z(e.jugador.nombre),h(2),z(e.tipoCromo.nombre),h(),_("ngIf",i.mostrarEdicion(e))}}function bj(t,n){if(t&1&&(m(0,"div",17),M(1,_j,12,4,"mat-card",18),u()),t&2){let e=T().$implicit,i=T(2);h(),_("ngForOf",i.cromosDeEquipo(e))}}function yj(t,n){t&1&&(m(0,"mat-card",27)(1,"mat-card-content")(2,"span",20),p(3,"1"),u(),m(4,"p",21),p(5,"Sin cromo"),u(),m(6,"p",22),p(7,"\u2014"),u()()())}function Cj(t,n){if(t&1&&(m(0,"div",17),M(1,yj,8,0,"mat-card",26),u()),t&2){let e=T(3);h(),_("ngForOf",e.placeholders)}}function xj(t,n){if(t&1&&(m(0,"mat-expansion-panel")(1,"mat-expansion-panel-header")(2,"mat-panel-title")(3,"mat-icon"),p(4,"groups"),u(),p(5),u(),m(6,"mat-panel-description"),p(7),u()(),M(8,bj,2,1,"div",16)(9,Cj,2,1,"div",16),u()),t&2){let e=n.$implicit,i=T(2);h(5),K(" ",e.nombre," "),h(2),ct(" ",i.cromosDeEquipo(e).length," cromo",i.cromosDeEquipo(e).length!==1?"s":""," "),h(),_("ngIf",i.cromosDeEquipo(e).length>0),h(),_("ngIf",i.cromosDeEquipo(e).length===0)}}function wj(t,n){t&1&&(m(0,"div",28)(1,"mat-icon"),p(2,"info"),u(),m(3,"p"),p(4,"Este \xE1lbum no tiene equipos definidos todav\xEDa."),u()())}function Ej(t,n){if(t&1&&(m(0,"p",25),p(1),u()),t&2){let e=T().$implicit;h(),z(e.edicion.nombre)}}function Dj(t,n){if(t&1){let e=qe();m(0,"mat-card",19)(1,"mat-card-content")(2,"span",20),p(3),u(),m(4,"p",21),p(5),u(),m(6,"p",32),p(7),u(),M(8,Ej,2,1,"p",23),u(),m(9,"button",24),I("click",function(){let r=we(e).$implicit,o=T(4);return Ee(o.editarCromo(r))}),m(10,"mat-icon"),p(11,"edit"),u()()()}if(t&2){let e=n.$implicit,i=T(4);h(3),z(e.numero),h(2),z(e.jugador.nombre),h(2),z(e.equipo.nombre),h(),_("ngIf",i.mostrarEdicion(e))}}function Ij(t,n){if(t&1&&(m(0,"mat-expansion-panel",31)(1,"mat-expansion-panel-header")(2,"mat-panel-title")(3,"mat-icon"),p(4,"star"),u(),p(5),u(),m(6,"mat-panel-description"),p(7),u()(),m(8,"div",17),M(9,Dj,12,4,"mat-card",18),u()()),t&2){let e=n.$implicit;h(5),K(" ",e.tipo," "),h(2),ct(" ",e.cromos.length," cromo",e.cromos.length!==1?"s":""," "),h(2),_("ngForOf",e.cromos)}}function Mj(t,n){if(t&1&&(kl(0),m(1,"mat-accordion",29),M(2,Ij,10,4,"mat-expansion-panel",30),u(),Ol()),t&2){let e=T(2);h(2),_("ngForOf",e.especiales)("ngForTrackBy",e.trackTipo)}}function Sj(t,n){if(t&1&&(m(0,"div",8)(1,"header",9)(2,"a",10)(3,"mat-icon"),p(4,"arrow_back"),u()(),m(5,"div")(6,"h1"),p(7),u(),m(8,"p",11),p(9),u()()(),m(10,"mat-accordion",12),M(11,xj,10,5,"mat-expansion-panel",13),u(),M(12,wj,5,0,"div",14)(13,Mj,3,2,"ng-container",15),u()),t&2){let e=T();h(7),z(e.album.nombre),h(2),Hu("",e.album.temporada," \xB7 ",e.album.equipos.length," equipo",e.album.equipos.length!==1?"s":""),h(2),_("ngForOf",e.album.equipos)("ngForTrackBy",e.trackEquipo),h(),_("ngIf",e.album.equipos.length===0),h(),_("ngIf",e.especiales.length>0)}}var Hf=class t{constructor(n,e,i,r){this.route=n;this.albumService=e;this.dialog=i;this.cdr=r}route;albumService;dialog;cdr;album=null;cargando=!0;error="";placeholders=[1,2,3,4];especiales=[];editarCromo(n){if(!this.album)return;this.dialog.open(Bf,{width:"520px",data:{cromo:n,albumId:this.album.id}}).afterClosed().subscribe(i=>{i&&this.album&&this.cargarDetalle(this.album.id)})}ngOnInit(){let n=Number(this.route.snapshot.paramMap.get("id"));if(!n){this.error="ID de \xE1lbum no v\xE1lido.",this.cargando=!1;return}this.cargarDetalle(n)}trackEquipo(n,e){return e.id}trackTipo(n,e){return e.tipo}esBasico(n){let e=(n||"").trim().toLowerCase();return e==="b\xE1sica"||e==="basica"||e==="coloca"||e==="baja"}cromosDeEquipo(n){return n.cromos.filter(e=>this.esBasico(e.tipoCromo.nombre)).sort((e,i)=>this.compararNumero(e,i))}mostrarEdicion(n){return n.edicion!=null&&n.edicion.id!==1}compararNumero(n,e){return this.numeroValor(n.numero)-this.numeroValor(e.numero)}numeroValor(n){let e=(n||"").match(/\d+/);return e?parseInt(e[0],10):Number.MAX_SAFE_INTEGER}construirEspeciales(n){let e=new Map;for(let i of n.equipos)for(let r of i.cromos){if(this.esBasico(r.tipoCromo.nombre))continue;let o=r.tipoCromo.nombre;e.has(o)||e.set(o,[]),e.get(o).push(r)}this.especiales=Array.from(e.entries()).map(([i,r])=>({tipo:i,cromos:r.sort((o,a)=>this.compararNumero(o,a))})).sort((i,r)=>i.tipo<r.tipo?-1:1)}cargarDetalle(n){this.cargando=!0,this.albumService.getAlbumConCromos(n).subscribe({next:e=>{this.album=e,this.construirEspeciales(e),this.cargando=!1,this.cdr.detectChanges()},error:e=>{this.error="No se ha podido cargar el \xE1lbum.",this.cargando=!1,this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(D(vi),D(lo),D(Lf),D(ge))};static \u0275cmp=A({type:t,selectors:[["app-coleccion-detalle"]],decls:4,vars:3,consts:[[1,"page-container"],["class","cargando",4,"ngIf"],["class","error",4,"ngIf"],["class","detalle",4,"ngIf"],[1,"cargando"],["diameter","40"],[1,"error"],["mat-stroked-button","","color","primary","routerLink","/albumes"],[1,"detalle"],[1,"detalle-header"],["mat-icon-button","","routerLink","/albumes",1,"back-btn"],[1,"subtitle"],["multi",""],[4,"ngFor","ngForOf","ngForTrackBy"],["class","sin-equipos",4,"ngIf"],[4,"ngIf"],["class","cromos-grid",4,"ngIf"],[1,"cromos-grid"],["class","cromo-card",4,"ngFor","ngForOf"],[1,"cromo-card"],[1,"cromo-numero"],[1,"cromo-jugador"],[1,"cromo-tipo"],["class","cromo-edicion",4,"ngIf"],["mat-icon-button","","aria-label","Editar cromo",1,"cromo-editar-btn",3,"click"],[1,"cromo-edicion"],["class","cromo-card placeholder",4,"ngFor","ngForOf"],[1,"cromo-card","placeholder"],[1,"sin-equipos"],["multi","",1,"especiales-accordion"],["class","especiales-panel",4,"ngFor","ngForOf","ngForTrackBy"],[1,"especiales-panel"],[1,"cromo-equipo"]],template:function(e,i){e&1&&(m(0,"div",0),M(1,hj,4,0,"div",1)(2,gj,9,1,"div",2)(3,Sj,14,8,"div",3),u()),e&2&&(h(),_("ngIf",i.cargando),h(),_("ngIf",i.error&&!i.cargando),h(),_("ngIf",i.album&&!i.cargando))},dependencies:[ut,dt,Ct,$n,pt,tn,nn,gt,ht,dI,cI,H_,U_,lI,sI,ks,As,jf],styles:[".page-container[_ngcontent-%COMP%]{min-height:calc(100vh - 64px);padding:40px 24px;background:var(--app-bg)}.cargando[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:16px;padding:80px 24px;color:var(--app-muted)}.error[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:12px;padding:80px 24px;color:#c62828}.error[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:48px;width:48px;height:48px}.detalle[_ngcontent-%COMP%]{max-width:960px;margin:0 auto;display:flex;flex-direction:column;gap:24px}.detalle-header[_ngcontent-%COMP%]{display:flex;align-items:center;gap:16px}.detalle-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:28px;font-weight:700;margin:0;color:var(--app-text)}.detalle-header[_ngcontent-%COMP%]   .subtitle[_ngcontent-%COMP%]{margin:4px 0 0;color:var(--app-muted);font-size:14px}.back-btn[_ngcontent-%COMP%]{color:var(--app-primary)}mat-accordion[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:12px}mat-expansion-panel[_ngcontent-%COMP%]{border-radius:12px!important;border:1px solid var(--app-border);box-shadow:none!important}mat-expansion-panel[_ngcontent-%COMP%]:hover{border-color:var(--app-primary)}mat-panel-title[_ngcontent-%COMP%]{display:flex;align-items:center;gap:10px;font-weight:600;font-size:16px}mat-panel-title[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--app-primary)}mat-panel-description[_ngcontent-%COMP%]{font-size:13px}.cromos-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:12px;padding-top:8px}.cromo-card[_ngcontent-%COMP%]{position:relative;border-radius:12px;border:1px solid var(--app-border);text-align:center;transition:border-color .2s}.cromo-card[_ngcontent-%COMP%]:hover{border-color:var(--app-primary)}.cromo-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]{padding:20px 12px!important;display:flex;flex-direction:column;align-items:center;gap:6px}.cromo-editar-btn[_ngcontent-%COMP%]{position:absolute;top:4px;right:4px;width:28px;height:28px;line-height:28px;color:var(--app-muted)}.cromo-editar-btn[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:16px;width:16px;height:16px;line-height:16px}.cromo-editar-btn[_ngcontent-%COMP%]:hover{color:var(--app-primary);background:var(--app-primary-soft)}.cromo-numero[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:center;width:48px;height:48px;border-radius:50%;background:var(--app-primary-soft);color:var(--app-primary);font-size:20px;font-weight:700}.cromo-jugador[_ngcontent-%COMP%]{margin:0;font-weight:600;font-size:13px;color:var(--app-text)}.cromo-tipo[_ngcontent-%COMP%]{margin:0;font-size:12px;color:var(--app-muted)}.cromo-equipo[_ngcontent-%COMP%]{margin:0;font-size:12px;color:var(--app-muted);font-style:italic}.cromo-edicion[_ngcontent-%COMP%]{margin:0;font-size:11px;color:var(--app-primary);font-weight:600}.especiales-accordion[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:12px}.especiales-panel[_ngcontent-%COMP%]{border-radius:12px!important;border:1px solid var(--app-border);box-shadow:none!important;background:var(--app-primary-soft)}.especiales-panel[_ngcontent-%COMP%]:hover{border-color:var(--app-primary)}.especiales-panel[_ngcontent-%COMP%]   .mat-expansion-panel-header[_ngcontent-%COMP%]{border-radius:12px}.cromo-card.placeholder[_ngcontent-%COMP%]{border-style:dashed;opacity:.5}.cromo-card.placeholder[_ngcontent-%COMP%]   .cromo-numero[_ngcontent-%COMP%]{background:var(--app-border);color:var(--app-muted)}.cromo-card.placeholder[_ngcontent-%COMP%]   .cromo-jugador[_ngcontent-%COMP%]{color:var(--app-muted);font-style:italic}.sin-equipos[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:12px;padding:60px 24px;color:var(--app-muted)}.sin-equipos[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:48px;width:48px;height:48px}@media(max-width:650px){.page-container[_ngcontent-%COMP%]{padding:20px 12px}.detalle-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:22px}.cromos-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:8px}}"]})};var CI=[{path:"",component:Df,children:[{path:"",redirectTo:"panel",pathMatch:"full"},{path:"panel",component:Rf},{path:"cromos",component:Ef},{path:"equipos",component:Mf},{path:"jugadores",component:Tf},{path:"tipos-cromo",component:Af},{path:"albumes",component:kf},{path:"editoriales",component:Nf},{path:"album/:id",component:Hf}]}];var xI={providers:[mh(),Wv(CI),vv()]};var Uf=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=A({type:t,selectors:[["app-root"]],decls:1,vars:0,template:function(e,i){e&1&&re(0,"router-outlet")},dependencies:[Ko],encapsulation:2})};fv(Uf,xI).catch(t=>console.error(t));
