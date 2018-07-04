# egg-task

## Usage
```java
// [Project]/config/plugin.js
exports.task = {
  enable: true,
  package: 'egg-task',
};

// [Project]/config/config.default.js
config.queue = {
    client: {
      redis: {
        host: 'localhost',
        port: 6379,
        db: 0,
      },
    },
  };

// [Project]/app/task/test.js  // example
'use strict';

const { Task } = require('egg-task');

module.exports = class Test extends Task {

  /**
   * @inheritDoc
   */
  get options() {
    return {
      // bull.js queue.add options
    };
  }

  onCompleted(job, result) { // bull.js queue event
  }

  async process(job) { // bull.js queue.process
      await this.addSubtask('xxx', { number: i }); // subtask
  }
};

// [Project]/app/controller/home.js  // example
this.ctx.task.test.add();
```
