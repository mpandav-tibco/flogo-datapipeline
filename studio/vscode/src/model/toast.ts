import { $, el } from './webviewState';

export function toast(kind: 'ok' | 'err' | 'warn' | 'info', msg: string, sticky = false) {
    const box = $('#toasts')!;
    const t = el('div', 'toast');
    const icon = { ok: '<span style="color:var(--green)">✓</span>', err: '<span style="color:var(--red)">✕</span>', warn: '<span style="color:var(--yellow)">⚠</span>', info: '<span style="color:var(--blueHi)">ℹ</span>' }[kind];
    t.innerHTML = `<div class="tt"><span class="i">${icon}</span><div>${msg}</div><span class="close">✕</span></div>`;
    (t.querySelector('.close') as HTMLElement).onclick = () => t.remove();
    box.appendChild(t);
    if (!sticky) setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity .3s'; setTimeout(() => t.remove(), 300); }, 3600);
}

export function escapeHtml(s: string) {
    return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
