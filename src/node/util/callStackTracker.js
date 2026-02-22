import async_hooks from 'async_hooks';

class CallStackTracker {
  static #asyncStackMap = new Map();
  static #hook = async_hooks.createHook({
    init(asyncId, type, triggerAsyncId) {
      CallStackTracker.#asyncStackMap.set(asyncId, triggerAsyncId);
    },
    destroy(asyncId) {
      CallStackTracker.#asyncStackMap.delete(asyncId);
    }
  });

  static start() {
    CallStackTracker.#hook.enable();
  }

  static stop() {
    CallStackTracker.#hook.disable();
    CallStackTracker.#asyncStackMap.clear();
  }

  static whereTheHellAmIandHowDidIGetHere() {
    const originalPrepareStackTrace = Error.prepareStackTrace;
    Error.prepareStackTrace = (err, structuredStackTrace) => structuredStackTrace;
    const err = new Error();
    Error.captureStackTrace(err, CallStackTracker.whereTheHellAmIandHowDidIGetHere);
    const stack = err.stack;
    Error.prepareStackTrace = originalPrepareStackTrace;
    return stack; // Array of CallSite objects
  }

  static getAsyncCallerChain() {
    let asyncId = async_hooks.executionAsyncId();
    const chain = [];
    while (asyncId) {
      chain.push(asyncId);
      asyncId = CallStackTracker.#asyncStackMap.get(asyncId);
    }
    return chain;
  }

  static getFullStackTrace() {
    const syncStack = CallStackTracker.whereTheHellAmIandHowDidIGetHere();
    const asyncChain = CallStackTracker.getAsyncCallerChain();
    return { syncStack, asyncChain };
  }
}

// Start tracking async contexts as soon as you load this
CallStackTracker.start();

export default CallStackTracker;
