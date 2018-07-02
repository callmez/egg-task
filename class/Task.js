'use strict';

const _ = require('lodash');
const { BaseContextClass } = require('egg');
const Singleton = require('egg/lib/core/singleton');

const QUEUE = Symbol('Task#queue')

class Task extends BaseContextClass {

  constructor(ctx) {
    super(ctx);
    this.task = ctx.task;
  }

  /**
   * 通过复写该方法获得该task默认选项
   * @return {{}} - return task options
   */
  get options() {
    return {

    };
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
   * 记录日志信息
   * @param type
   * @param message
   * @param category
   * @param data
   * @return {Promise<void>}
   */
  log({ type = 'info', message, category = 'app', ...data}) {
    this.queue.logger[type]('[egg-task]', `[${category}]`, `[${this.pathName}]`, message, data);
  }
  
  /**
   * 添加任务. 默认执行该操作,可复写更改任务流程
   * @param data
   * @param options
   * @return {Promise<*>}
   */
  async add(data, options = {}) {
    return this.addTask(data, {
      attempts: 3, // 默认执行3次错误重试
      removeOnComplete: true, // 默认执行成功后删除记录, 缩减redis数据大小
      ...this.options,
      ...options,
    });
  }

  /**
   * 执行任务. 复写该方法执行任务流程
   * @return {Promise<*>}
   */
  async process(job) {
    return this.app.logger.error('process method must be override');
  }

  /**
   * 重新添加任务
   * @param job
   * @return {Promise<*>}
   */
  async readd(oldJob) {
    const { data, opts } = oldJob;
    delete opts.timestamp; // 删除时间 重新计算执行事件
    const job = await this.addTask(data, opts);
    this.log({ message: 'readd task', category: 'system', job: job.toJSON() });
    return job;
  }

  /**
   * 调用子任务
   * @param string name
   * @param {Array} args
   * @return {Promise<*>}
   */
  async addSubtask(name, ...args) {
    const task = _.get(this.task, name);
    if (!task instanceof Task) {
      throw new Error('The subtask class must instance of Task');
    }
    const job = await task.add.apply(task, args);
    this.log({ message: 'add subtask', category: 'system', job: job.toJSON() });
    return job;
  }

  /**
   * 该方法为基础调用. 请勿复写
   * @param data
   * @param options
   * @return {Promise<*>}
   */
  async addTask(data, options) {
    const { fullPath, pathName } = this;
    const _data = {
      meta: {
        fullPath,
        pathName,
      },
      ...data,
    };
    const job = await this.queue.add(_data, options);
    this.log({ message: 'add task', category: 'system', job: job.toJSON() });
    return job;
  }

  /**
   * 该方法为基础调用. 请勿复写
   * @param job
   * @return {Promise<*>}
   */
  async processTask(job) {
    this.log({ message: 'process task', category: 'system', job: job.toJSON() });
    const result = await this.process(job);
    return result;
  }
};

module.exports = Task;
