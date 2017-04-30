_map = require 'lodash/map'
_isArray = require 'lodash/isArray'
_filter = require 'lodash/filter'
_isEmpty = require 'lodash/isEmpty'
_isUndefined = require 'lodash/isUndefined'
_find = require 'lodash/find'
_transform = require 'lodash/transform'
_zip = require 'lodash/zip'
_defaults = require 'lodash/defaults'
_pickBy = require 'lodash/pickBy'
_mapValues = require 'lodash/mapValues'
_clone = require 'lodash/clone'
_forEach = require 'lodash/forEach'
_findIndex = require 'lodash/findIndex'
_takeRight = require 'lodash/takeRight'
_keys = require 'lodash/keys'
Rx = require 'rx-lite'
log = require 'loga'
stringify = require 'json-stable-stringify'
uuid = require 'node-uuid'

module.exports = class Exoid
  constructor: ({@api, cache, @ioEmit, @io, @isServerSide}) ->
    cache ?= {}

    @_cache = {}
    @_batchQueue = []
    @_listeners = {}
    @_consumeTimeout = null

    @dataCacheStreams = new Rx.ReplaySubject 1
    @dataCacheStreams.onNext Rx.Observable.just cache
    @dataCacheStream = @dataCacheStreams.switch()

    @io.on 'reconnect', => @invalidateAll true

    _map cache, (result, key) =>
      @_cacheSet key, {dataStream: Rx.Observable.just result}

  _updateDataCacheStream: =>
    dataStreamsArray = _map(@_cache, ({dataStream}, key) ->
      dataStream.map (value) ->
        [key, value]
    )
    stream = Rx.Observable.combineLatest.apply this, dataStreamsArray.concat [
      (vals...) -> vals
    ]
    .map (pairs) ->
      _transform pairs, (cache, [key, val]) ->
        # ignore if the request hasn't finished yet (esp for server-side render)
        if val isnt null
          cache[key] = val
      , {}

    @dataCacheStreams.onNext stream

  getCacheStream: => @dataCacheStream

  _cacheSet: (key, {combinedStream, dataStream, options}) =>
    if dataStream and not @_cache[key]?.dataStream
      # https://github.com/claydotio/exoid/commit/fc26eb830910b6567d50e15063ec7544e2ccfedc
      dataStreams = if @isServerSide \
                    then new Rx.BehaviorSubject(Rx.Observable.just null)
                    else new Rx.ReplaySubject 1
      @_cache[key] ?= {}
      @_cache[key].dataStreams = dataStreams
      @_cache[key].dataStream = dataStreams.switch()
      @_updateDataCacheStream()

    if combinedStream and not @_cache[key]?.combinedStream
      combinedStreams = new Rx.ReplaySubject 1
      @_cache[key] ?= {}
      @_cache[key].options = options
      @_cache[key].combinedStreams = combinedStreams
      @_cache[key].combinedStream = combinedStreams.switch()

    if dataStream
      @_cache[key].dataStreams.onNext dataStream

    if combinedStream
      @_cache[key].combinedStreams.onNext combinedStream

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
      _forEach responses, ({result, error}, streamId) =>
        queueIndex = _findIndex queue, {streamId}
        if queueIndex is -1
          console.log 'stream ignored', streamId
          return
        {req, res, isErrorable} = queue[queueIndex]
        # console.log '-----------'
        # console.log req.path, req.body, req.query, Date.now() - start
        # console.log '-----------'
        queue.splice queueIndex, 1
        if _isEmpty queue
          @io.off batchId, onBatch

        if isErrorable and error?
          properError = new Error "#{JSON.stringify error}"
          res.onError _defaults properError, error
        else if not error?
          res.onNext result
          res.onCompleted()
        else
          log.error error

    onError = (error) ->
      _map queue, ({res, isErrorable}) ->
        if isErrorable
          res.onError error
        else
          log.error error

    batchId = uuid.v4()
    @io.on batchId, onBatch, onError

    @ioEmit 'exoid', {
      batchId: batchId
      isClient: window?
      requests: _map queue, ({req, streamId}) -> _defaults {streamId}, req
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
                           then @_replaySubjectFromIo @io, streamId
                           else new Rx.ReplaySubject 0
    clientChangesStream ?= Rx.Observable.just null
    changesStream = Rx.Observable.merge(
      additionalDataStream, clientChangesStream
    )

    # ideally we'd use concat here instead, but initialDataStream is
    # a switch observable because of cache
    combinedStream = Rx.Observable.merge(
      initialDataStream, changesStream
    )
    .scan (items, update) =>
      @_combineChanges {
        items
        initial: if update?.changes then null else update
        changes: update?.changes
      }, {initialSortFn, limit}
    , null
    .shareReplay 1

    # TODO: does this have bad side-effects?
    # if stream gets to 0 subscribers, the next subscriber starts over
    # from scratch and we lose all the progress of the .scan.
    # This is because shareReplay (and any subject) will disconnect when it
    # hits 0 and reconnect. The supposed solution is "autoconnect", I think,
    # but it's not in rxjs at the moment: http://stackoverflow.com/a/36118469
    @_listeners[streamId].combinedDisposable = combinedStream.subscribe ->
      null

    combinedStream

  _combineChanges: ({items, initial, changes}, {initialSortFn, limit}) ->
    if initial
      items = _clone initial
      if _isArray(items) and initialSortFn
        items = initialSortFn items
    else if changes
      items ?= []
      _forEach changes, (change) ->
        existingIndex = change.oldId and
                        _findIndex(items, {id: change.oldId}) or
                        _findIndex(items, {clientId: change.newVal?.clientId})
        if existingIndex? and existingIndex isnt -1 and change.newVal
          items.splice existingIndex, 1, change.newVal
        else if existingIndex? and existingIndex isnt -1
          items.splice existingIndex, 1
        else
          items = items.concat [change.newVal]
    return if limit then _takeRight items, limit else items

  _replaySubjectFromIo: (io, eventName) =>
    unless @_listeners[eventName].replaySubject
      replaySubject = new Rx.ReplaySubject 0
      ioListener = (data) ->
        replaySubject.onNext data
      io.on eventName, ioListener
      @_listeners[eventName].replaySubject = replaySubject
      @_listeners[eventName].ioListener = ioListener
    @_listeners[eventName].replaySubject

  _initialDataRequest: (req, {isErrorable, streamId, ignoreCache}) =>
    key = stringify req
    if not @_cache[key]?.dataStream or ignoreCache
      # should only be caching the actual async result and nothing more, since
      # that's all we can really get from server -> client rendering with
      # json.stringify
      @_cacheSet key, {dataStream: @_batchRequest(req, {isErrorable, streamId})}

    @_cache[key].dataStream

  getCached: (path, body) =>
    req = {path, body}
    key = stringify req

    if @_cache[key]?
      @_cache[key].dataStream.take(1).toPromise()
    else
      Promise.resolve null

  stream: (path, body, options = {}) =>
    req = {path, body}
    key = stringify req

    unless @_cache[key]?.combinedStream
      streamId = uuid.v4()
      options = _defaults options, {
        streamId
        isErrorable: false
      }
      clientChangesStream = options.clientChangesStream
      clientChangesStream ?= new Rx.ReplaySubject 0
      clientChangesStream = clientChangesStream.map (change) ->
        {initial: null, changes: [{newVal: change}], isClient: true}
      options.clientChangesStream = clientChangesStream

      @_cacheSet key, {
        options
        combinedStream: @_combinedRequestStream req, options
      }

    @_cache[key]?.combinedStream

  call: (path, body) =>
    req = {path, body}

    stream = @_batchRequest req, {isErrorable: true}
    return stream.take(1).toPromise().then (result) ->
      return result

  invalidateAll: (streamsOnly = false) =>
    _map @_listeners, (listener, streamId) =>
      @io.off streamId, listener?.ioListener
      listener.combinedDisposable?.dispose()
    @_listeners = {}

    if streamsOnly
      @_cache = _pickBy @_cache, (cache, key) ->
        cache.options?.isStreamed

    @_cache = _pickBy _mapValues(@_cache, (cache, key) =>
      {dataStreams, combinedStreams, options} = cache
      if not combinedStreams or not combinedStreams.hasObservers()
        return false
      req = JSON.parse key
      dataStreams.onNext @_batchRequest req, options
      combinedStreams.onNext @_combinedRequestStream req, options
      cache
    ), (val) -> val
    return null

  invalidate: (path, body) =>
    req = {path, body}
    key = stringify req

    _map @_cache, (cache, cacheKey) =>
      {dataStreams, combinedStreams, options} = cache
      req = JSON.parse cacheKey
      if req.path is path and _isUndefined(body) or cacheKey is key
        dataStreams.onNext @_batchRequest req, options
        combinedStreams.onNext @_combinedRequestStream req, options
    return null
