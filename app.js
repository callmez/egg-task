'use strict';

const path = require('path');
const assert = require('assert');
const _ = require('lodash');
const Queue = require('bull');

module.exports = app => {
  const config = app.config.task;

  if (config.clients) throw new Error('egg-task only support single client');

  const { queue } = config.client || {};
  assert(queue, '[egg-bull] queue is required on config');
  const { redis } = queue;
  assert(
    redis && redis.host && redis.port,
    '[egg-task] host and port of redis are required on config.queue'
  );
  const queueClient = new Queue(config.name || 'task', config.queue);

  loadToTask();
  app.beforeStart(process);


  function loadToTask(opt) {
    const { loader } = app;

    loader.timing.start('Load Task');
    // 载入到 app.taskClasses
    opt = Object.assign({
      call: true,
      caseStyle: 'lower',
      fieldClass: 'taskClasses',
      directory: path.join(app.options.baseDir, 'app/task'),
      initializer(model, opt) {
        model.prototype.queue = queueClient; // 传递queue到task类中
        return model;
      },
    }, opt);
    const taskPaths = opt.directory;
    loader.loadToContext(taskPaths, 'task', opt);
    loader.timing.end('Load Task');
  }

  function process() {
    queueClient.process(config.concurrency, function(job) {
      const pathName = _.get(job.data, 'meta.pathName');
      if (!pathName) throw new Error('Missing task pathName meta data.');
      const ctx = app.createAnonymousContext();
      const task = _.get(ctx, pathName);
      if (!task) throw new Error(`The task ${pathName} is not exists.`);
      return task.processTask(job);
    });
    queueClient
      .on('failed', (job, error) => {
        app.logger.error('[egg-task]', 'process failed', error, job);
      })
      .on('error', error => {
        app.logger.error('[egg-task]', 'process error', error);
      });
  }
};
