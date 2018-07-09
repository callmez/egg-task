'use strict';

const _ = require('lodash');
const { BaseContextClass } = require('egg');
const Singleton = require('egg/lib/core/singleton');

const QUEUE = Symbol('Task#queue');

class Task extends BaseContextClass {

  constructor(ctx) {
    super(ctx);
    this.task = ctx.task;
  }

  /**
   * 获取指定queue处理任务
   * @return {*}
   */
  get queue() {
    if (!this[QUEUE]) {
      const { app: { queue } } = this;
      let client;
      if (queue instanceof Singleton) { // 多queue环境
        // 通过指定queueName可以为当前task指定queue, 必须为存在的queue.name;
        const name = this.queueName || queue.clients.keys().next().value; // 无指定则返回第一个queue
        client = queue.get(name);
        if (!client) throw new Error(`the name "${name}" of queue is not exists.`);
      } else {
        client = queue;
      }
      this[QUEUE] = client;
    }
    return this[QUEUE];
  }

  /**
   * 通过复写该方法获得该task默认选项
   * @return {{}} - return task options
   */
  get options() {
    return { };
  }

  async _readd(oldJob) {
    const { data, opts: { timestamp, ...opts} } = oldJob; // 重新添加需要去除timestamp标记
    const job = await this._addTask(data, opts);
    return job;
  }

  async add(data, options = {}) {
    // 注意:
    // 1. bull中的延迟执行会使lifo失效., 所以delay在和lifo组合使用时需注意
    // @see https://github.com/OptimalBits/bull/issues/945
    return this._addTask(data, {
      attempts: 10, // 十次错误尝试
      removeOnComplete: true, // 执行成功后删除
      ...this.options,
      ...options,
    });
  }

  async process(data) {
    return null;
  }

  async _addTask(data, options = {}) {
    const { queue, fullPath, pathName } = this;
    // TODO add backoff support
    const job = await queue.add(pathName, { meta: { fullPath, pathName }, ...data }, options);
    return job;
  }

  async _processTask(job) {
    const { data } = job;
    const result = await this.process(data, job);
    return result;
  }
};

module.exports = Task;
