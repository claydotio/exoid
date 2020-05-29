import * as _ from 'lodash-es'
import * as Rx from 'rxjs'
import * as rx from 'rxjs/operators'
# get consistent hash from stringified results
import stringify from 'json-stable-stringify'
import uuid from 'uuid'

export default class Exoid
  constructor: ({@api, cache, @ioEmit, @io, @isServerSide, @allowInvalidation}) ->
    cache ?= {}
    @allowInvalidation ?= true

    @_cache = {}
    @_batchQueue = []
    @_listeners = {}
    @_consumeTimeout = null

    @dataCacheStreams = new Rx.ReplaySubject 1
    @dataCacheStreams.next Rx.of cache
    @dataCacheStream = @dataCacheStreams.pipe rx.switchAll()
    # simulataneous invalidateAlls seem to break streams
    @invalidateAll = _.debounce @_invalidateAll, 0, {trailing: true}

    @io.on 'reconnect', => @invalidateAll true

    _.map cache, (result, key) =>
      @_cacheSet key, {dataStream: Rx.of result}

  disableInvalidation: =>
    @allowInvalidation = false

  enableInvalidation: =>
    @allowInvalidation = true

  _updateDataCacheStream: =>
    dataStreamsArray = _.map(@_cache, ({dataStream}, key) ->
      dataStream.pipe rx.map (value) ->
        [key, value]
    )
    stream = Rx.combineLatest.apply this, dataStreamsArray.concat [
      (vals...) -> vals
    ]
    .pipe rx.map (pairs) ->
      _.transform pairs, (cache, [key, val]) ->
        # ignore if the request hasn't finished yet (esp for server-side render)
        # don't use null since some reqs return null
        if val isnt undefined
          cache[key] = val
      , {}

    @dataCacheStreams.next stream

  getCacheStream: => @dataCacheStream

  _cacheSet: (key, {combinedStream, dataStream, options}) =>
    valueToCache = if options?.ignoreCache then {} else @_cache[key] or {}
    if dataStream and not valueToCache?.dataStream
      # https://github.com/claydotio/exoid/commit/fc26eb830910b6567d50e15063ec7544e2ccfedc
      dataStreams = if @isServerSide \
                    then new Rx.BehaviorSubject(Rx.of undefined) \
                    else new Rx.ReplaySubject 1
      valueToCache.dataStreams = dataStreams
      valueToCache.dataStream = dataStreams.pipe rx.switchAll()

    if combinedStream and not valueToCache?.combinedStream
      combinedStreams = new Rx.ReplaySubject 1
      valueToCache.options = options
      valueToCache.combinedStreams = combinedStreams
      valueToCache.combinedStream = combinedStreams.pipe rx.switchAll()

    if dataStream
      valueToCache.dataStreams.next dataStream

    if combinedStream
      valueToCache.combinedStreams.next combinedStream

    unless options?.ignoreCache
      @_cache[key] = valueToCache
      @_updateDataCacheStream()

    valueToCache

  _batchRequest: (req, {isErrorable, streamId} = {}) =>
    streamId ?= uuid.v4()

    unless @_consumeTimeout
      @_consumeTimeout = setTimeout @_consumeBatchQueue

    res = new Rx.AsyncSubject()
    @_batchQueue.push {req, res, isErrorable, streamId}
    res

  _consumeBatchQueue: =>
    queue = @_batchQueue
    @_batchQueue = []
    @_consumeTimeout = null

    start = Date.now()
    onBatch = (responses) =>
      _.forEach responses, ({result, error}, streamId) =>
        queueIndex = _.findIndex queue, {streamId}
        if queueIndex is -1
          console.log 'stream ignored', streamId
          return
        {req, res, isErrorable} = queue[queueIndex]
        # console.log '-----------'
        # console.log req.path, req.body, req.query, Date.now() - start
        # console.log '-----------'
        queue.splice queueIndex, 1
        if _.isEmpty queue
          @io.off batchId, onBatch

        if isErrorable and error?
          # TODO: (hacky) this should use .onError. It has a weird bug where it
          # repeatedly errors though...
          res.next {error}
          res.complete()
        else if not error?
          res.next result
          res.complete()
        else
          console.error error

    onError = (error) ->
      _.map queue, ({res, isErrorable}) ->
        if isErrorable
          res.onError error
        else
          console.error error

    batchId = uuid.v4()
    @io.on batchId, onBatch, onError

    @ioEmit 'exoid', {
      batchId: batchId
      isClient: window?
      requests: _.map queue, ({req, streamId}) -> _.defaults {streamId}, req
    }

  _combinedRequestStream: (req, options = {}) =>
    {isErrorable, streamId, clientChangesStream,
      initialSortFn, limit, ignoreCache} = options

    unless @_listeners[streamId]
      @_listeners[streamId] = {}

    initialDataStream = @_initialDataRequest req, {
      isErrorable, streamId, ignoreCache
    }
    additionalDataStream = if streamId and options.isStreamed \
                           then @_replaySubjectFromIo @io, streamId \
                           else new Rx.ReplaySubject 0
    clientChangesStream ?= Rx.of null
    changesStream = Rx.merge(
      additionalDataStream, clientChangesStream
    )

    # ideally we'd use concat here instead, but initialDataStream is
    # a switch observable because of cache
    combinedStream = Rx.merge(
      initialDataStream, changesStream
    )
    .pipe(
      rx.scan (items, update) =>
        @_combineChanges {
          items
          initial: if update?.changes then null else update
          changes: update?.changes
        }, {initialSortFn, limit}
      , null
      rx.publishReplay(1)
      rx.refCount()
    )

    # if stream gets to 0 subscribers, the next subscriber starts over
    # from scratch and we lose all the progress of the .scan.
    # This is because publishReplay().refCount() (and any subject)
    # will disconnect when it
    # hits 0 and reconnect. The supposed solution is "autoconnect", I think,
    # but it's not in rxjs at the moment: http://stackoverflow.com/a/36118469
    @_listeners[streamId].combinedDisposable = combinedStream.subscribe ->
      null

    combinedStream

  _combineChanges: ({items, initial, changes}, {initialSortFn, limit}) ->
    if initial
      items = _.clone initial
      if _.isArray(items) and initialSortFn
        items = initialSortFn items
    else if changes
      items ?= []
      _.forEach changes, (change) ->
        existingIndex = change.oldId and
                        _.findIndex(items, {id: change.oldId})
        if not existingIndex? or existingIndex is -1
          existingIndex = _.findIndex(items, {clientId: change.newVal?.clientId})
        if existingIndex? and existingIndex isnt -1 and change.newVal
          items.splice existingIndex, 1, change.newVal
        else if existingIndex? and existingIndex isnt -1
          items.splice existingIndex, 1
        else
          items = items.concat [change.newVal]
    return if limit then _.takeRight items, limit else items

  _replaySubjectFromIo: (io, eventName) =>
    unless @_listeners[eventName].replaySubject
      replaySubject = new Rx.ReplaySubject 0
      ioListener = (data) ->
        replaySubject.next data
      io.on eventName, ioListener
      @_listeners[eventName].replaySubject = replaySubject
      @_listeners[eventName].ioListener = ioListener
    @_listeners[eventName].replaySubject

  _streamFromIo: (io, eventName) =>
    Rx.create (observer) =>
      io.on eventName, (data) ->
        observer.next data
      ->
        io.off eventName

  _initialDataRequest: (req, {isErrorable, streamId, ignoreCache}) =>
    key = stringify req
    cachedValue = @_cache[key]
    if not cachedValue?.dataStream or ignoreCache
      # should only be caching the actual async result and nothing more, since
      # that's all we can really get from server -> client rendering with
      # json.stringify
      cachedValue = @_cacheSet key, {
        dataStream: @_batchRequest(req, {isErrorable, streamId})
        options: {ignoreCache}
      }

    cachedValue.dataStream

  setDataCache: (req, data) ->
    key = if typeof req is 'string' then req else stringify req
    @_cacheSet key, {dataStream: Rx.of data}

  getCached: (path, body) =>
    req = {path, body}
    key = stringify req

    if @_cache[key]?
      @_cache[key].dataStream.pipe(rx.take(1)).toPromise()
    else
      Promise.resolve null

  stream: (path, body, options = {}) =>
    req = {path, body}
    key = stringify req

    cachedValue = @_cache[key]

    if not cachedValue?.combinedStream or options.ignoreCache
      streamId = uuid.v4()
      options = _.defaults options, {
        streamId
        isErrorable: false
      }
      clientChangesStream = options.clientChangesStream
      clientChangesStream ?= new Rx.ReplaySubject 0
      clientChangesStream = clientChangesStream.pipe rx.map (change) ->
        {initial: null, changes: [{newVal: change}], isClient: true}
      options.clientChangesStream = clientChangesStream

      cachedValue = @_cacheSet key, {
        options
        combinedStream: @_combinedRequestStream req, options
      }

    cachedValue?.combinedStream
    # TODO: (hacky) this should use .onError. It has a weird bug where it
    # repeatedly errors though...
    .pipe rx.map (result) ->
      if result?.error and window?
        throw new Error JSON.stringify result?.error
      result

  call: (path, body, {additionalDataStream} = {}) =>
    req = {path, body}

    streamId = uuid.v4()

    if additionalDataStream
      additionalDataStream.next @_streamFromIo @io, streamId

    stream = @_batchRequest req, {isErrorable: true, streamId}

    return stream.pipe(rx.take(1)).toPromise().then (result) ->
      if result?.error and window?
        throw new Error JSON.stringify result?.error
      return result

  disposeAll: =>
    _.map @_listeners, (listener, streamId) =>
      @io.off streamId, listener?.ioListener
      listener.combinedDisposable?.unsubscribe()
    @_listeners = {}

  # deobunced in constructor
  _invalidateAll: (streamsOnly = false) =>
    unless @allowInvalidation
      return

    @disposeAll()

    if streamsOnly
      @_cache = _.pickBy @_cache, (cache, key) ->
        cache.options?.isStreamed

    @_cache = _.pickBy _.mapValues(@_cache, (cache, key) =>
      {dataStreams, combinedStreams, options} = cache

      # without this, after invalidating, the stream is just the clientChanges
      # for a split second (eg chat in starfire just shows the messages you
      # posted for a flash until the rest reload in). this is kind of hacky
      # since it's a prop on the object, the observable gets completed replaced
      # in the model too
      options?.clientChangesStream = new Rx.ReplaySubject 0

      if not combinedStreams or combinedStreams.observers.length is 0
        return false
      req = JSON.parse key
      dataStreams.next @_batchRequest req, options
      combinedStreams.next @_combinedRequestStream req, options
      cache
    ), (val) -> val
    return null

  invalidate: (path, body) =>
    unless @allowInvalidation
      return

    req = {path, body}
    key = stringify req

    _.map @_cache, (cache, cacheKey) =>
      {dataStreams, combinedStreams, options} = cache
      req = JSON.parse cacheKey

      if req.path is path and _.isUndefined(body) or cacheKey is key
        listener = @_listeners[options.streamId]
        listener.combinedDisposable?.unsubscribe()
        delete @_listeners[options.streamId]
        @io.off options.streamId

        dataStreams.next @_batchRequest req, options
        combinedStreams.next @_combinedRequestStream req, options

    return null
