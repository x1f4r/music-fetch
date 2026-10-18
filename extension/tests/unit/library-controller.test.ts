import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { tm } from '@/src/i18n';
import { DemoEngine } from '@/src/ui/demo/runtime';
import { demoState } from '@/src/ui/demo/states';
import { LibraryController, TOAST_MS } from '@/src/ui/library.svelte';

async function library() {
  const engine = new DemoEngine(demoState('app-library')!);
  const controller = new LibraryController(engine);
  await controller.start();
  return { engine, controller };
}

describe('the library controller', () => {
  beforeEach(() => fakeBrowser.reset());

  it('selects with clicks, ranges and select all', async () => {
    const { controller } = await library();
    const order = controller.order;
    expect(controller.click(order[3]!)).toBe('open');
    controller.click(order[4]!, { mod: true });
    controller.click(order[6]!, { shift: true });
    expect(controller.selected.map((e) => e.id)).toEqual(order.slice(4, 7).map((e) => e.id));
    controller.selectAll();
    expect(controller.selection.size).toBe(order.length);
    controller.clearSelection();
    expect(controller.selecting).toBe(false);
  });

  it('deletes with Undo, and only removes from the engine when the toast ends', async () => {
    vi.useFakeTimers();
    try {
      const { engine, controller } = await library();
      const blinding = controller.entries.find((e) => e.id === 'demo-blinding')!;
      const spy = vi.spyOn(engine, 'deleteSessions');
      await controller.requestDelete([blinding]);
      expect(controller.visible.some((e) => e.id === 'demo-blinding')).toBe(false);
      expect(tm(controller.toast!.message)).toBe('Deleted 1');
      await controller.undo();
      expect(controller.visible.some((e) => e.id === 'demo-blinding')).toBe(true);
      await controller.requestDelete([blinding]);
      await vi.advanceTimersByTimeAsync(TOAST_MS + 10);
      expect(spy).toHaveBeenCalledWith(['demo-blinding']);
    } finally {
      vi.useRealTimers();
    }
  });

  it('asks before deleting a pinned mix', async () => {
    const { engine, controller } = await library();
    const mix = controller.entries.find((e) => e.id === 'demo-mix')!;
    const spy = vi.spyOn(engine, 'deleteSessions');
    await controller.requestDelete([mix]);
    expect(controller.confirmDelete?.map((e) => e.id)).toEqual(['demo-mix']);
    expect(spy).not.toHaveBeenCalled();
    await controller.confirmDeletion();
    expect(spy).toHaveBeenCalledWith(['demo-mix']);
  });

  it('pins unless all are pinned, with Undo', async () => {
    const { engine, controller } = await library();
    const targets = controller.entries.filter((e) => e.id === 'demo-houdini' || e.id === 'demo-blinding');
    await controller.togglePinned(targets);
    expect((await engine.session('demo-blinding'))!.entry.pinned).toBe(true);
    expect(tm(controller.toast!.message)).toBe('Pinned 2');
    await controller.undo();
    expect((await engine.session('demo-blinding'))!.entry.pinned).toBe(false);
    expect((await engine.session('demo-houdini'))!.entry.pinned).toBe(true);
  });
});
