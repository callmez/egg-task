'use strict';

const path = require('path');
const _ = require('lodash');
const assert = require('assert');
const Queue = require('bull');

const DEFAULT_QUEUE_NAME = 'task';
// TODO 增加日志完善指标监控
module.exports = app => {
  const queues = {};

  // 使用clients的key作为唯一name标记, client则默认为task
  const { clients, client, load, } = app.config.queue;
  _.each(clients || { [DEFAULT_QUEUE_NAME]:  client }, (data, key) => { data._name = key; })

  app.Queue = Queue;
  app.addSingleton('queue', createQueue);
  loadTask(load || {});

  async function createQueue({ _name, ...options }) {
    const { redis } = options;
    assert(
      redis && redis.host && redis.port,
      '[egg-task] host and port of redis are required on config.task'
    );
    const queue = queues[_name] = new Queue(_name, options);
    attachProcessor(queue);
    attachListeners(queue);

    app.beforeClose(() => { queue.close(); }); // 优雅的关闭
    // debug 环境下 不能每次都能触发beforeClose, 所以在加个信号判断退出
    // @see https://github.com/eggjs/egg/issues/1267
    if (process.env.EGG_DEBUG) process.once('SIGINT', () => { queue.close(); }); // debug (ctrl-c or stop)
    return queue;
  }

  function loadTask(options) {
    const { loader } = app;

    loader.timing.start('Load Task');
    options = Object.assign({
      call: true,
      caseStyle: 'lower',
      fieldClass: 'taskClasses',
      directory: path.join(app.options.baseDir, 'app/task'),
      initializer: (task, opt) => {
        if (task.concurrency) { // 自设定并发数 则触发单独处理函数
          // 设定并发数需要制定queueName,不指定则使用默认queue
          const { queueName = DEFAULT_QUEUE_NAME } = task;
          const queue = queues[queueName];
          if (!queue) throw new Error(`The queue ${queueName} is not exists.`);
          attachProcessor(queue, opt.pathName);
        }
        return task;
      }
    }, options);
    loader.loadToContext(options.directory, 'task', options);
    loader.timing.end('Load Task');
  }

  let ctx;
  function getContext() {
    if (!ctx) ctx = app.createAnonymousContext();
    return ctx;
  }

  function getTask({ name }, methodName) {
    const task = _.get(getContext(), name);
    if (!task) throw new Error(`The task ${name} is not exists.`);
    // 调用方法
    if (methodName) {
      return typeof task[methodName] === 'function' ? task[methodName].bind(task) : false;
    }
    return task;
  }

  function attachProcessor(queue, name = '*', concurrency = queue.settings.concurrency || 20) {
    queue.process(name, concurrency, job => {
      return getTask(job)._processTask(job);
    });
  }

  function attachListeners(queue) {
    queue
      .on('error', function(error) {
        // An error occured.

      })

      .on('active', function(job, jobPromise) {
        // A job has started. You can use `jobPromise.cancel()`` to abort it.

        const method = getTask(job, 'onActive');
        if (method) method(job, jobPromise);
      })

      .on('stalled', function(job) {
        // A job has been marked as stalled. This is useful for debugging job
        // workers that crash or pause the event loop.

        const method = getTask(job, 'onStalled');
        if (method) method(job);
      })

      .on('progress', function(job, progress){
        // A job's progress was updated!

        const method = getTask(job, 'onProgress');
        if (method) method(job, progress);
      })

      .on('completed', function(job, result){
        // A job successfully completed with a `result`.

        const method = getTask(job, 'onCompleted');
        if (method) method(job, result);
      })

      .on('failed', function(job, err) {
        // A job failed with reason `err`!

        const method = getTask(job, 'onFailed');
        if (method) method(job, err);
      })

      // .on('paused', function() {
      //   // The queue has been paused.
      //
      // })

      .on('resumed', function(job) {
        // The queue has been resumed.

        const method = getTask(job, 'onResumed');
        if (method) method(job);
      })

      .on('cleaned', function(jobs, type) {
        // Old jobs have been cleaned from the queue. `jobs` is an array of cleaned
        // jobs, and `type` is the type of jobs cleaned.
        for (const job of jobs) {
          const method = getTask(job, 'onCleaned');
          if (method) method(job, type);
        }
      })

      // .on('drained', function() {
      //   //Emitted every time the queue has processed all the waiting jobs (even if there can be some delayed jobs not yet processed)
      //
      // })

      .on('removed', function(job) {
        // A job successfully removed.

        const method = getTask(job, 'onRemoved');
        if (method) method(job);
      });
  }
};

