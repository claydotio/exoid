import * as _ from 'lodash-es'
import * as Rx from 'rxjs'
import * as rx from 'rxjs/operators'
// get consistent hash from stringified results
import stringify from 'json-stable-stringify'
import uuid from 'uuid'

export default class Exoid {
  constructor ({ api, cache, ioEmit, io, isServerSide, allowInvalidation }) {
    this.disableInvalidation = this.disableInvalidation.bind(this)
    this.enableInvalidation = this.enableInvalidation.bind(this)
    this.setSynchronousCache = this.setSynchronousCache.bind(this)
    this.getSynchronousCache = this.getSynchronousCache.bind(this)
    this._updateDataCacheStream = this._updateDataCacheStream.bind(this)
    this.getCacheStream = this.getCacheStream.bind(this)
    this._cacheSet = this._cacheSet.bind(this)
    this._batchRequest = this._batchRequest.bind(this)
    this._consumeBatchQueue = this._consumeBatchQueue.bind(this)
    this._combinedRequestStream = this._combinedRequestStream.bind(this)
    this._replaySubjectFromIo = this._replaySubjectFromIo.bind(this)
    this._streamFromIo = this._streamFromIo.bind(this)
    this._initialDataRequest = this._initialDataRequest.bind(this)
    this.getCached = this.getCached.bind(this)
    this.stream = this.stream.bind(this)
    this.call = this.call.bind(this)
    this.disposeAll = this.disposeAll.bind(this)
    this._invalidateAll = this._invalidateAll.bind(this)
    this.invalidate = this.invalidate.bind(this)
    this.api = api
    this.ioEmit = ioEmit
    this.io = io
    this.isServerSide = isServerSide
    this.allowInvalidation = allowInvalidation
    if (cache == null) { cache = {} }
    this.synchronousCache = cache
    if (this.allowInvalidation == null) { this.allowInvalidation = true }

    this._cache = {}
    this._batchQueue = []
    this._listeners = {}
    this._consumeTimeout = null

    this.dataCacheStreams = new Rx.ReplaySubject(1)
    this.dataCacheStreams.next(Rx.of(cache))
    this.dataCacheStream = this.dataCacheStreams.pipe(rx.switchAll())
    // simulataneous invalidateAlls seem to break streams
    this.invalidateAll = _.debounce(this._invalidateAll, 0, { trailing: true })

    this.io.on('reconnect', () => this.invalidateAll(true))

    _.map(cache, (result, key) => {
      return this._cacheSet(key, { dataStream: Rx.of(result) })
    })
  }

  disableInvalidation () {
    return this.allowInvalidation = false
  }

  enableInvalidation () {
    return this.allowInvalidation = true
  }

  // for ssr since it's synchronous 1 render atm (can't use getCacheStream)
  setSynchronousCache (synchronousCache) { this.synchronousCache = synchronousCache; return null }
  getSynchronousCache () { return this.synchronousCache }

  _updateDataCacheStream () {
    const dataStreamsArray = _.map(this._cache, ({ dataStream }, key) => dataStream.pipe(rx.map(value => [key, value])))
    const stream = Rx.combineLatest.apply(this, dataStreamsArray.concat([
      (...vals) => vals
    ]))
      .pipe(rx.map(pairs => _.transform(pairs, function (cache, ...rest) {
      // ignore if the request hasn't finished yet (esp for server-side render)
      // don't use null since some reqs return null
        const [key, val] = Array.from(rest[0])
        if (val !== undefined) {
          return cache[key] = val
        }
      }
      , {})))

    return this.dataCacheStreams.next(stream)
  }

  getCacheStream () { return this.dataCacheStream }

  _cacheSet (key, { combinedStream, dataStream, options }) {
    let combinedStreams, dataStreams
    const valueToCache = options?.ignoreCache ? {} : this._cache[key] || {}
    if (dataStream && !valueToCache?.dataStream) {
      // https://github.com/claydotio/exoid/commit/fc26eb830910b6567d50e15063ec7544e2ccfedc
      dataStreams = this.isServerSide
        ? new Rx.BehaviorSubject(Rx.of(undefined))
        : new Rx.ReplaySubject(1)
      valueToCache.dataStreams = dataStreams
      valueToCache.dataStream = dataStreams.pipe(rx.switchAll())
    }

    if (combinedStream && !valueToCache?.combinedStream) {
      combinedStreams = new Rx.ReplaySubject(1)
      valueToCache.options = options
      valueToCache.combinedStreams = combinedStreams
      valueToCache.combinedStream = combinedStreams.pipe(rx.switchAll())
    }

    if (dataStream) {
      valueToCache.dataStreams.next(dataStream)
    }

    if (combinedStream) {
      valueToCache.combinedStreams.next(combinedStream)
    }

    if (!options?.ignoreCache) {
      this._cache[key] = valueToCache
      this._updateDataCacheStream()
    }

    return valueToCache
  }

  _batchRequest (req, param) {
    if (param == null) { param = {} }
    let { isErrorable, streamId } = param
    if (streamId == null) { streamId = uuid.v4() }

    if (!this._consumeTimeout) {
      this._consumeTimeout = setTimeout(this._consumeBatchQueue)
    }

    const res = new Rx.AsyncSubject()
    this._batchQueue.push({ req, res, isErrorable, streamId })
    return res
  }

  _consumeBatchQueue () {
    const queue = this._batchQueue
    this._batchQueue = []
    this._consumeTimeout = null

    var onBatch = responses => {
      return _.forEach(responses, ({ result, error }, streamId) => {
        const queueIndex = _.findIndex(queue, { streamId })
        if (queueIndex === -1) {
          console.log('stream ignored', streamId)
          return
        }
        const { req, res, isErrorable } = queue[queueIndex]
        // console.log '-----------'
        // console.log req.path, req.body, req.query, Date.now() - start
        // console.log '-----------'
        queue.splice(queueIndex, 1)
        if (_.isEmpty(queue)) {
          this.io.off(batchId, onBatch)
        }

        if (isErrorable && (error != null)) {
          // TODO: (hacky) this should use .onError. It has a weird bug where it
          // repeatedly errors though...
          res.next({ error })
          return res.complete()
        } else if ((error == null)) {
          res.next(result)
          return res.complete()
        } else {
          return console.error(error)
        }
      })
    }

    const onError = error => _.map(queue, function ({ res, isErrorable }) {
      if (isErrorable) {
        return res.onError(error)
      } else {
        return console.error(error)
      }
    })

    var batchId = uuid.v4()
    this.io.on(batchId, onBatch, onError)

    return this.ioEmit('exoid', {
      batchId,
      isClient: (typeof window !== 'undefined' && window !== null),
      requests: _.map(queue, ({ req, streamId }) => _.defaults({ streamId }, req))
    })
  }

  _combinedRequestStream (req, options) {
    if (options == null) { options = {} }
    let {
      isErrorable, streamId, clientChangesStream,
      initialSortFn, limit, ignoreCache
    } = options

    if (!this._listeners[streamId]) {
      this._listeners[streamId] = {}
    }

    const initialDataStream = this._initialDataRequest(req, {
      isErrorable, streamId, ignoreCache
    })
    const additionalDataStream = streamId && options.isStreamed
      ? this._replaySubjectFromIo(this.io, streamId)
      : new Rx.ReplaySubject(0)
    if (clientChangesStream == null) { clientChangesStream = Rx.of(null) }
    const changesStream = Rx.merge(
      additionalDataStream, clientChangesStream
    )

    // ideally we'd use concat here instead, but initialDataStream is
    // a switch observable because of cache
    const combinedStream = Rx.merge(
      initialDataStream, changesStream
    )
      .pipe(
        rx.scan((items, update) => {
          return this._combineChanges({
            items,
            initial: update?.changes ? null : update,
            changes: update?.changes
          }, { initialSortFn, limit })
        }
        , null),
        rx.publishReplay(1),
        rx.refCount()
      )

    // if stream gets to 0 subscribers, the next subscriber starts over
    // from scratch and we lose all the progress of the .scan.
    // This is because publishReplay().refCount() (and any subject)
    // will disconnect when it
    // hits 0 and reconnect. The supposed solution is "autoconnect", I think,
    // but it's not in rxjs at the moment: http://stackoverflow.com/a/36118469
    this._listeners[streamId].combinedDisposable = combinedStream.subscribe(() => null)

    return combinedStream
  }

  _combineChanges ({ items, initial, changes }, { initialSortFn, limit }) {
    if (initial) {
      items = _.clone(initial)
      if (_.isArray(items) && initialSortFn) {
        items = initialSortFn(items)
      }
    } else if (changes) {
      if (items == null) { items = [] }
      _.forEach(changes, function (change) {
        let existingIndex = change.oldId &&
                        _.findIndex(items, { id: change.oldId })
        if ((existingIndex == null) || (existingIndex === -1)) {
          existingIndex = _.findIndex(items, { clientId: change.newVal?.clientId })
        }
        if ((existingIndex != null) && (existingIndex !== -1) && change.newVal) {
          return items.splice(existingIndex, 1, change.newVal)
        } else if ((existingIndex != null) && (existingIndex !== -1)) {
          return items.splice(existingIndex, 1)
        } else {
          return items = items.concat([change.newVal])
        }
      })
    }
    if (limit) { return _.takeRight(items, limit) } else { return items }
  }

  _replaySubjectFromIo (io, eventName) {
    let replaySubject
    if (!this._listeners[eventName].replaySubject) {
      replaySubject = new Rx.ReplaySubject(0)
      const ioListener = data => replaySubject.next(data)
      io.on(eventName, ioListener)
      this._listeners[eventName].replaySubject = replaySubject
      this._listeners[eventName].ioListener = ioListener
    }
    return this._listeners[eventName].replaySubject
  }

  _streamFromIo (io, eventName) {
    return new Rx.Observable(observer => {
      io.on(eventName, data => observer.next(data))
      return () => io.off(eventName)
    })
  }

  _initialDataRequest (req, { isErrorable, streamId, ignoreCache }) {
    const key = stringify(req)
    let cachedValue = this._cache[key]
    if (!cachedValue?.dataStream || ignoreCache) {
      // should only be caching the actual async result and nothing more, since
      // that's all we can really get from server -> client rendering with
      // json.stringify
      cachedValue = this._cacheSet(key, {
        dataStream: this._batchRequest(req, { isErrorable, streamId }),
        options: { ignoreCache }
      })
    }

    return cachedValue.dataStream
  }

  setDataCache (req, data) {
    const key = typeof req === 'string' ? req : stringify(req)
    return this._cacheSet(key, { dataStream: Rx.of(data) })
  }

  getCached (path, body) {
    const req = { path, body }
    const key = stringify(req)

    if (this._cache[key] != null) {
      return this._cache[key].dataStream.pipe(rx.take(1)).toPromise()
    } else {
      return Promise.resolve(null)
    }
  }

  stream (path, body, options) {
    if (options == null) { options = {} }
    const req = { path, body }
    const key = stringify(req)

    let cachedValue = this._cache[key]

    if (!cachedValue?.combinedStream || options.ignoreCache) {
      const streamId = uuid.v4()
      options = _.defaults(options, {
        streamId,
        isErrorable: false
      })
      let {
        clientChangesStream
      } = options
      if (clientChangesStream == null) { clientChangesStream = new Rx.ReplaySubject(0) }
      clientChangesStream = clientChangesStream.pipe(rx.map(change => ({
        initial: null,
        changes: [{ newVal: change }],
        isClient: true
      })))
      options.clientChangesStream = clientChangesStream

      cachedValue = this._cacheSet(key, {
        options,
        combinedStream: this._combinedRequestStream(req, options)
      })
    }

    return cachedValue?.combinedStream
    // TODO: (hacky) this should use .onError. It has a weird bug where it
    // repeatedly errors though...
    .pipe(rx.map(function (result) {
      if (result?.error && (typeof window !== 'undefined' && window !== null)) {
        throw new Error(JSON.stringify(result?.error))
      }
      return result
    })
    )
  }

  call (path, body, param) {
    if (param == null) { param = {} }
    const { additionalDataStream } = param
    const req = { path, body }

    const streamId = uuid.v4()

    if (additionalDataStream) {
      additionalDataStream.next(this._streamFromIo(this.io, streamId))
    }

    const stream = this._batchRequest(req, { isErrorable: true, streamId })

    return stream.pipe(rx.take(1)).toPromise().then(function (result) {
      if (result?.error && (typeof window !== 'undefined' && window !== null)) {
        throw new Error(JSON.stringify(result?.error))
      }
      return result
    })
  }

  disposeAll () {
    _.map(this._listeners, (listener, streamId) => {
      this.io.off(streamId, listener?.ioListener)
      return listener.combinedDisposable?.unsubscribe()
    })
    return this._listeners = {}
  }

  // deobunced in constructor
  _invalidateAll (streamsOnly) {
    if (streamsOnly == null) { streamsOnly = false }
    if (!this.allowInvalidation) {
      return
    }

    this.disposeAll()

    if (streamsOnly) {
      this._cache = _.pickBy(this._cache, (cache, key) => cache.options?.isStreamed)
    }

    this._cache = _.pickBy(_.mapValues(this._cache, (cache, key) => {
      const { dataStreams, combinedStreams, options } = cache;

      // without this, after invalidating, the stream is just the clientChanges
      // for a split second (eg chat in starfire just shows the messages you
      // posted for a flash until the rest reload in). this is kind of hacky
      // since it's a prop on the object, the observable gets completed replaced
      // in the model too
      (options != null) && (options.clientChangesStream = new Rx.ReplaySubject(0))

      if (!combinedStreams || (combinedStreams.observers.length === 0)) {
        return false
      }
      const req = JSON.parse(key)
      dataStreams.next(this._batchRequest(req, options))
      combinedStreams.next(this._combinedRequestStream(req, options))
      return cache
    }), val => val)
    return null
  }

  invalidate (path, body) {
    if (!this.allowInvalidation) {
      return
    }

    let req = { path, body }
    const key = stringify(req)

    _.map(this._cache, (cache, cacheKey) => {
      const { dataStreams, combinedStreams, options } = cache
      req = JSON.parse(cacheKey)

      if (((req.path === path) && _.isUndefined(body)) || (cacheKey === key)) {
        const listener = this._listeners[options.streamId]
        listener.combinedDisposable?.unsubscribe()
        delete this._listeners[options.streamId]
        this.io.off(options.streamId)

        dataStreams.next(this._batchRequest(req, options))
        return combinedStreams.next(this._combinedRequestStream(req, options))
      }
    })

    return null
  }
}
