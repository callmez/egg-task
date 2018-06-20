'use strict';

const { BaseContextClass } = require('egg');

module.exports = class Task extends BaseContextClass {

  constructor(ctx) {
    super(ctx);
    this.task = ctx.task;
  }

  /**
   * 通过复写该方法获得该task默认选项
   * @return {{}}
   */
  get options() {
    return {};
  }

  /**
   * 添加任务. 默认执行该操作,可复写更改任务流程
   * @param data
   * @param options
   * @return {*}
   */
  add(data, options = {}) {
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
   * 该方法为基础调用. 请勿复写
   * @param data
   * @param options
   * @return {*}
   */
  addTask(data, options) {
    const { fullPath, pathName } = this;
    return this.queue.add({
      meta: {
        fullPath,
        pathName,
      },
      ...data,
    }, options);
  }

  /**
   * 该方法为基础调用. 请勿复写
   * @param job
   * @return {Promise<*>}
   */
  async processTask(job) {
    return this.process(job);
  }
};
