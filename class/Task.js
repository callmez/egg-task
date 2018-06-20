'use strict';

const { BaseContextClass } = require('egg');

module.exports = class Task extends BaseContextClass {

  constructor(ctx) {
    super(ctx);
    this.task = ctx.task;
  }

  add(opt = {}) {
    const { fullPath, pathName } = this;
    this.queue.add({
      meta: {
        fullPath,
        pathName,
      },
      ...opt,
    });
  }

  process(task = {}) {

  }
};
