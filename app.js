'use strict';

const path = require('path');
const assert = require('assert');
const _ = require('lodash');
const Queue = require('bull');

module.exports = app => {
  const config = app.config.queue;
  // 默认记录clients的唯一key做为name, 保证name的唯一性以便queue可以处理指定的的任务
  if (config.clients) Object.keys(config.clients).forEach(id => { config.clients[id].name = id; });
  const queues = {};

  app.Queue = Queue;
  app.addSingleton('queue', createQueue);

  loadTask(config.loadOptions || {});

  function createQueue(config) {
    const { name = 'task', logger, ...options } = config;
    const { redis } = options;
    assert(
      redis && redis.host && redis.port,
      '[egg-task] host and port of redis are required on config.queue'
    );

    const client = new Queue(name, options);
    queues[client.name] = client;
    client.logger = logger || app.logger;
    attachEvent(client, app);
    return client;
  }

  function loadTask(options) {
    const { loader } = app;

    loader.timing.start('Load Task');
    // 载入到 app.taskClasses
    options = Object.assign({
      call: true,
      caseStyle: 'lower',
      fieldClass: 'taskClasses',
      directory: path.join(app.options.baseDir, 'app/task'),
    }, options);
    loader.loadToContext(options.directory, 'task', options);
    loader.timing.end('Load Task');
  }

  let ctx;
  function getContext() {
    if (!ctx) ctx = app.createAnonymousContext();
    return ctx;
  }

  function getTask(job, methodName) {
    const { data } = job;
    const pathName = _.get(data, 'meta.pathName');
    if (!pathName) throw new Error('Missing task pathName meta data.');
    const task = _.get(getContext(), pathName);
    if (!task) throw new Error(`The task ${pathName} is not exists.`);
    // 调用方法
    if (methodName) {
      return typeof task[methodName] === 'function' ? task[methodName].bind(task) : false;
    }
    return task;
  }

  function attachEvent(queue) {
    const { logger } = queue;
    queue.process(queue.settings.concurrency || 10, function(job) {
      const task = getTask(job);
      // TODO 增加 job handler 支持, 细化到job可以控制queue的生产控制(job process concurrency, job event handler)
      return task.processTask(job);
    });
    queue
      .on('error', function(error) {
        // An error occured.
        logger.error('[egg-task]', '[event]', 'error', error);
      })

      .on('active', function(job, jobPromise) {
        logger.info('[egg-task]', '[event]', 'job active', job);

        // A job has started. You can use `jobPromise.cancel()`` to abort it.
        const method = getTask(job, 'onActive');
        if (method) method(job, jobPromise);
      })

      .on('stalled', function(job) {
        logger.warn('[egg-task]', '[event]', 'job stalled', job);

        // A job has been marked as stalled. This is useful for debugging job
        // workers that crash or pause the event loop.
        const method = getTask(job, 'onStalled');
        if (method) method(job);
      })

      .on('progress', function(job, progress){
        // A job's progress was updated!
        const method = getTask(job, '[event]', 'onProgress');
        if (method) method(job, progress);
      })

      .on('completed', function(job, result){
        logger.info('[egg-task]', '[event]', 'job completed', job, result);

        // A job successfully completed with a `result`.
        const method = getTask(job, 'onCompleted');
        if (method) method(job, result);
      })

      .on('failed', function(job, err) {
        logger.warn('[egg-task]', '[event]', 'job failed', job, err);

        // A job failed with reason `err`!
        const method = getTask(job, 'onFailed');
        if (method) method(job, err);
      })

      // .on('paused', function() {
        // The queue has been paused.
      // })

      .on('resumed', function(job) {
        logger.warn('[egg-task]', '[event]', 'job resumed', job);

        // The queue has been resumed.
        const method = getTask(job, 'onResumed');
        if (method) method(job);
      })

      .on('cleaned', function(jobs, type) {
        logger.info('[egg-task]', '[event]', 'job cleaned', jobs, type);
        // Old jobs have been cleaned from the queue. `jobs` is an array of cleaned
        // jobs, and `type` is the type of jobs cleaned.
        jobs.forEach(job => {
          const method = getTask(job, 'onCleaned');
          if (method) method(job, type);
        })
      })

      // .on('drained', function() {
        // Emitted every time the queue has processed all the waiting jobs (even if there can be some delayed jobs not yet processed)
      // })

      .on('removed', function(job) {
        logger.info('[egg-task]', '[event]', 'job removed', job);
        // A job successfully removed.
        const method = getTask(job, 'onRemoved');
        if (method) method(job);
      });
  }
};

