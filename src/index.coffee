_map = require 'lodash/map'
_isArray = require 'lodash/isArray'
_filter = require 'lodash/filter'
_isEmpty = require 'lodash/isEmpty'
_some = require 'lodash/some'
_isUndefined = require 'lodash/isUndefined'
_find = require 'lodash/find'
_transform = require 'lodash/transform'
_zip = require 'lodash/zip'
_defaults = require 'lodash/defaults'
_isString = require 'lodash/isString'
Rx = require 'rx-lite'
log = require 'loga'
stringify = require 'json-stable-stringify'
uuid = require 'node-uuid'

uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

module.exports = class Exoid
  constructor: ({@api, cache, @ioEmit, @io}) ->
    cache ?= {}

    @_cache = {}
    @_batchQueue = []

    @cacheStreams = new Rx.ReplaySubject(1)
    @cacheStreams.onNext Rx.Observable.just @_cache
    @cacheStream = @cacheStreams.switch()

    @io.on 'disconnect', @invalidateAll

    _map cache, @_cacheRefs

    _map cache, (result, key) =>
      req = JSON.parse key

      isResource = uuidRegex.test req.path
      if isResource
        return null

      @_cacheSet key, @_streamResult req, result

  _cacheRefs: (result) =>
    # top level refs only
    resources = if _isArray(result) then result else [result]

    _map resources, (resource) =>
      if resource?.id?
        unless uuidRegex.test resource.id
          throw new Error 'ids must be uuid'
        embedded = resource.embedded or []
        key = stringify {path: resource.id, embedded: embedded}
        @_cacheSet key, Rx.Observable.just resource

  _streamResult: (req, result) =>
    resources = if _isArray(result) then result else [result]
    refs = _filter _map resources, (resource) =>
      if resource?.id?
        unless uuidRegex.test resource.id
          throw new Error 'ids must be uuid'
        embedded = resource.embedded or []
        resourceKey = stringify {path: resource.id, embedded: embedded}
        @_cache[resourceKey].stream
      else
        null

    stream = (if _isEmpty(refs) then Rx.Observable.just []
    else Rx.Observable.combineLatest(refs)
    ).flatMapLatest (refs) =>
      # if a sub-resource is invalidated (deleted), re-request
      if _some refs, _isUndefined
        return @_deferredRequestStream req

      Rx.Observable.just \
      if _isArray result
        _map result, (resource) ->
          ref = _find refs, {id: resource?.id}
          if ref? then ref else resource
      else
        ref = _find refs, {id: result?.id}
        if ref? then ref else result

    return stream

  _deferredRequestStream: (req, isErrorable, streamId) =>
    cachedStream = null
    Rx.Observable.defer =>
      if cachedStream?
        return cachedStream

      return cachedStream = @_batchCacheRequest req, isErrorable, streamId

  _batchCacheRequest: (req, isErrorable, streamId) =>
    if _isEmpty @_batchQueue
      setTimeout @_consumeBatchQueue

    resStreams = new Rx.ReplaySubject(1)
    req.streamId = streamId
    @_batchQueue.push {req, resStreams, isErrorable}

    resStreams.switch()

  _updateCacheStream: =>
    stream = Rx.Observable.combineLatest _map @_cache, ({stream}, key) ->
      stream.map (value) -> [key, value]
    .map (pairs) ->
      _transform pairs, (cache, [key, val]) ->
        cache[key] = val
      , {}

    @cacheStreams.onNext stream

  getCacheStream: => @cacheStream

  _cacheSet: (key, stream) =>
    unless @_cache[key]?
      requestStreams = new Rx.ReplaySubject(1)
      @_cache[key] = {stream: requestStreams.switch(), requestStreams}
      @_updateCacheStream()

    @_cache[key].requestStreams.onNext stream

  _consumeBatchQueue: =>
    queue = @_batchQueue
    @_batchQueue = []

    batchId = uuid.v4()
    @io.on batchId, ({results, cache, errors}) =>
      # update explicit caches from response
      _map cache, ({path, body, result}) =>
        @_cacheSet stringify({path, body}), Rx.Observable.just result

      # update implicit caches from results
      _map results, @_cacheRefs

      # update explicit request cache result, using ref-stream
      # top level replacement only
      _map _zip(queue, results, errors),
      ([{req, resStreams, isErrorable}, result, error]) =>
        if isErrorable and error?
          properError = new Error "#{JSON.stringify error}"
          resStreams.onError _defaults properError, error
        else if not error?
          resStreams.onNext @_streamResult req, result
        else
          log.error error
    , (error) ->
      _map queue, ({resStreams, isErrorable}) ->
        if isErrorable
          resStreams.onError error
        else
          log.error error

      # TODO: dispose

    @ioEmit 'exoid', {
      batchId: batchId
      isClient: window?
      requests: _map queue, 'req'
    }
    # .catch log.error

  getCached: (path, body) =>
    req = {path, body}
    key = stringify req

    if @_cache[key]?
      @_cache[key].stream.take(1).toPromise()
    else
      Promise.resolve null

  stream: (path, body, {ignoreCache} = {}) =>
    req = {path, body}
    key = stringify req
    streamId = uuid.v4()
    resourceKey = stringify {path: body, embedded: []}

    if @_cache[key]? and not ignoreCache
      return @_cache[key].stream

    if _isString(body) and uuidRegex.test(body) and @_cache[resourceKey]?
      resultPromise = @_cache[resourceKey].stream.take(1).toPromise()
      stream = Rx.Observable.defer ->
        resultPromise
      .flatMapLatest (result) =>
        @_streamResult req, result
      @_cacheSet key, stream
      return @_cache[key].stream

    batchStream = @_deferredRequestStream req, false, streamId
    requestStream = Rx.Observable.fromEvent @io, streamId
    stream = Rx.Observable.merge(batchStream, requestStream)

    if ignoreCache
      return stream

    @_cacheSet key, stream
    return @_cache[key].stream

  call: (path, body) =>
    req = {path, body}
    key = stringify req

    stream = @_deferredRequestStream req, true
    return stream.take(1).toPromise().then (result) ->
      # @_cacheSet key, Rx.Observable.just result
      return result

  update: (result) =>
    @_cacheRefs result
    return null

  invalidateAll: =>
    _map @_cache, ({requestStreams}, key) =>
      req = JSON.parse key
      if _isString(req.path) and uuidRegex.test(req.path)
        return
      requestStreams.onNext @_deferredRequestStream req
    return null

  invalidate: (path, body) =>
    req = {path, body}
    key = stringify req
    resourceKey = stringify {path}

    if _isString(path) and uuidRegex.test(path) and @_cache[resourceKey]?
      @_cache[resourceKey].requestStreams.onNext Rx.Observable.just(undefined)
      return null

    _map @_cache, ({requestStreams}, cacheKey) =>
      req = JSON.parse cacheKey
      if _isString(req.path) and uuidRegex.test(req.path)
        return

      if req.path is path and _isUndefined body
        requestStreams.onNext @_deferredRequestStream req
      else if cacheKey is key
        requestStreams.onNext @_deferredRequestStream req
    return null
