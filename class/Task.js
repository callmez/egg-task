'use strict';

const { BaseContextClass } = require('egg');
const _ = require('lodash');

module.exports = class Task extends BaseContextClass {

  constructor(ctx) {
    super(ctx);
    this.task = ctx.task;
  }

  /**
   * 通过复写该方法获得该task默认选项
   * @return {{}} - return task options
   */
  get options() {
    return {};
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
    this.app.logger[type]('[task]', `[${this.pathName}]`, `[${category}]`, message, data);
  }
  
  /**
   * 添加任务. 默认执行该操作,可复写更改任务流程
   * @param data
   * @param options
   * @return {Promise<*>}
   */
  async add(data, options = {}) {
    return this.addTask(data, {
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
    return this.process(job);
  }
};
