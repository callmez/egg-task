// const delay = timeout => new Promise(resolve => setTimeout(resolve, timeout));
// const queue = new Queue('test');
//
// let _prefix = 0;
// const cluser = parseInt(Math.random() * 100);
// async function add_test() {
//   const prefix = `${cluser}_${++_prefix}`;
//   console.log(`add test_${prefix}`);
//   await queue.add({ name: 'test', prefix }, { jobId: 'test', lifo: true, removeOnComplete: true });
// }
//
// async function process_test(job) {
//   const { data: { prefix } } = job
//   console.log(`process test_${prefix}`);
//   for (let number = 0; number < 50; number++) {
//     await queue.add({ name: 'subtest', prefix }, { jobId: `subtest_${prefix}_${number}` });
//   }
// }
//
// async function process_subtest(job) {
//   const { opts: { jobId } } = job
//   console.log(`process ${jobId}`);
//   await delay(10000);
// }
//
// queue.process(10, async (job) => {
//   const {data: { name } } = job;
//   if (name === 'test') {
//     await process_test(job);
//   } else if (name === 'subtest') {
//     await process_subtest(job);
//   }
// });
// queue.on('completed', async function(job, result){
//   const { data: { name } } = job;
//   if (name === 'test') await add_test();
// })
//
// setTimeout(() => {
//   add_test();
// }, 3000);