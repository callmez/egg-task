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
   * @param {string} key 锁定键值
   * @param {number} ttl 超时锁定时间 默认1天
   * @return {Promise<Promise<*>|*>}
   */
  async lock(key, ttl = 24 * 3600 * 1000) {
    return this.redlock.lock(key, ttl);
  }

  async unlock(){
    return this.redlock.unlock();
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
    // !!注意!! 设定了开启了lock之后, 需要指定jobID才能锁定指定的job,
    let lock = _.get(job, 'opts.lock', false);
    if (!!lock) {
      const jobID = _.get(job, 'opts.repeat.jobId', job.id);
      const ttl = /\d+/.test(lock) ? lock : 24 * 3600 * 1000; // 默认锁超时1天
      try {
        lock = await this.redlock.lock(`lock:${jobID}`, ttl); // TODO 更完善的重试job时锁控制
      } catch (e) {
        this.log({ type: 'debug', message: `The task job ${jobID} has been locked. will skip this job call.`});
        return false;
      }
    }

    let result;
    try { 
      this.log({ message: 'process task', category: 'system', job: job.toJSON() });
      result = await this.process(job);
    } finally {
      // 无论失败和成功都释放, 以便queue重启任务的时候可以继续操作.(但是需注意逻辑的严谨性)
      if (lock) await lock.unlock();
    }

    return result;
  }
};
