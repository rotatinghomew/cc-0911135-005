// 演练子进程入口：由 tools/drill.mjs 通过 DRILL_ARGS 环境变量传参启动。
// 独立进程模拟真实的崩溃/重启边界，不与父进程共享内存状态。
import { childMain } from './drill.mjs';

const encoded = process.env.DRILL_ARGS;
if (!encoded) throw new Error('缺少 DRILL_ARGS 环境变量');
await childMain(encoded);
